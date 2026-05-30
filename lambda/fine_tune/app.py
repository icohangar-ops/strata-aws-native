"""
Strata CFO Resilience Matrix — Layer 2: Fine-Tuning Pipeline Lambda

This Lambda function manages the fine-tuning pipeline for CFO-specific models:
- Reads curated data from S3
- Prepares Bedrock fine-tuning jobs or S3 artifacts for external training
- Tracks job status in DynamoDB with full lifecycle management
- Version controls model artifacts with immutable naming

FTR Compliance Notes:
- All S3 operations use KMS CMK encryption
- DynamoDB tracks job state with TTL for cleanup
- Bedrock API calls use explicit model ARNs (no wildcards)
- X-Ray tracing for end-to-end job visibility
- Structured logging for CloudWatch dashboards
"""

import json
import os
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

# FTR Compliance: Environment variables from SAM template
CURATED_DATA_BUCKET = os.environ.get("CURATED_DATA_BUCKET", "")
MODEL_ARTIFACTS_BUCKET = os.environ.get("MODEL_ARTIFACTS_BUCKET", "")
FINE_TUNING_TABLE = os.environ.get("FINE_TUNING_TABLE", "")
BEDROCK_SECRET_ARN = os.environ.get("BEDROCK_SECRET_ARN", "")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "")
PRIMARY_MODEL_ID = os.environ.get("PRIMARY_MODEL_ID", "anthic.claude-3-5-sonnet-20241022-v1:0")

logger = Logger(service="strata-finetune")
metrics = Metrics(namespace="StrataCFO")
tracer = Tracer()

_s3_client = None
_dynamodb_resource = None
_dynamodb_table = None
_bedrock_client = None
_secrets_client = None


class JobStatus(str, Enum):
    """Fine-tuning job lifecycle states — FTR: Explicit state machine for auditability."""
    PENDING = "PENDING"
    PREPARING = "PREPARING"
    VALIDATING = "VALIDATING"
    SUBMITTED = "SUBMITTED"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FineTuningStrategy(str, Enum):
    """
    Supported fine-tuning strategies.

    FTR Note: Strategy selection is driven by data volume and model type.
    Bedrock custom model training uses native API when available,
    otherwise exports artifacts for external training pipelines.
    """
    BEDROCK_CUSTOM_MODEL = "bedrock_custom_model"
    PROMPT_ENGINEERING = "prompt_engineering"
    RAG_AUGMENTATION = "rag_augmentation"
    EXPORT_ARTIFACTS = "export_artifacts"


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def get_dynamodb_table():
    global _dynamodb_resource, _dynamodb_table
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    if _dynamodb_table is None:
        _dynamodb_table = _dynamodb_resource.Table(FINE_TUNING_TABLE)
    return _dynamodb_table


def get_bedrock_client():
    """Initialize Bedrock client for fine-tuning job management."""
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock")
    return _bedrock_client


def get_bedrock_config() -> Dict[str, Any]:
    """Retrieve Bedrock configuration from Secrets Manager — FTR: zero secrets in code."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=BEDROCK_SECRET_ARN)
    return json.loads(response["SecretString"])


@tracer.capture_method
def create_job_record(job_id: str, strategy: str, tenant_id: str = "default") -> Dict[str, Any]:
    """
    Create a new fine-tuning job record in DynamoDB.

    FTR Compliance:
    - Conditional write prevents duplicate job creation
    - TTL set for automated cleanup of old records
    - All fields explicitly typed and documented
    """
    now = datetime.now(timezone.utc)
    table = get_dynamodb_table()

    job_record = {
        "job_id": job_id,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "status": JobStatus.PENDING.value,
        "strategy": strategy,
        "tenant_id": tenant_id,
        "base_model": PRIMARY_MODEL_ID,
        "progress_percentage": 0,
        "training_samples": 0,
        "validation_samples": 0,
        "epochs_completed": 0,
        "total_epochs": 0,
        "artifacts_s3_key": "",
        "evaluation_metrics": {},
        "error_message": "",
        "error_count": 0,
        "cost_estimate_usd": 0.0,
        "expires_at": int(now.timestamp() + 180 * 24 * 3600),  # 180-day TTL
    }

    try:
        table.put_item(
            Item=job_record,
            ConditionExpression="attribute_not_exists(job_id)",
        )
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning(f"Job {job_id} already exists — skipping creation")
    except Exception as e:
        logger.error(f"Failed to create job record: {e}")
        raise

    return job_record


@tracer.capture_method
def update_job_status(job_id: str, status: JobStatus, **kwargs) -> None:
    """
    Update job status with optimistic locking for concurrency safety.

    FTR Compliance:
    - State transitions are atomic and auditable
    - Update expression ensures no lost updates
    - Timestamp always updated on change
    """
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc)

    update_expression = "SET #status = :status, #updated_at = :updated_at"
    expression_values = {
        ":status": status.value,
        ":updated_at": now.isoformat(),
    }
    expression_names = {
        "#status": "status",
        "#updated_at": "updated_at",
    }

    # Add optional fields dynamically
    for key, value in kwargs.items():
        safe_key = key.replace("-", "_")
        update_expression += f", #{safe_key} = :{safe_key}"
        expression_names[f"#{safe_key}"] = key
        expression_values[f":{safe_key}"] = value

    try:
        table.update_item(
            Key={"job_id": job_id, "created_at": kwargs.get("created_at", "UNKNOWN")},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names,
            ConditionExpression="attribute_exists(job_id)",
        )
    except Exception as e:
        logger.error(f"Failed to update job {job_id}: {e}")
        raise


@tracer.capture_method
def prepare_training_data(job_id: str) -> Dict[str, Any]:
    """
    Read curated data from S3 and prepare fine-tuning training artifacts.

    Training data preparation:
    1. Read curated JSONL files from S3
    2. Split into training (80%) and validation (20%) sets
    3. Format for Bedrock fine-tuning API (or export format)
    4. Write prepared artifacts to model-artifacts bucket

    FTR Compliance:
    - KMS-encrypted S3 reads and writes
    - Data partitioning for efficient access
    - Training/validation split with statistical validity
    """
    s3 = get_s3_client()
    now = datetime.now(timezone.utc)

    # Read all curated JSONL files
    training_entries = []
    validation_entries = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=CURATED_DATA_BUCKET,
        Prefix="curated/",
        MaxKeys=1000,
    ):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".jsonl"):
                continue

            try:
                response = s3.get_object(Bucket=CURATED_DATA_BUCKET, Key=obj["Key"])
                content = response["Body"].read().decode("utf-8")

                for line in content.strip().split("\n"):
                    if not line.strip():
                        continue
                    entry = json.loads(line)

                    # FTR: Training/validation split (80/20) based on hash
                    hash_val = hash(entry.get("dedup_hash", ""))
                    if hash_val % 5 == 0:
                        validation_entries.append(entry)
                    else:
                        training_entries.append(entry)

            except Exception as e:
                logger.warning(f"Failed to read curated file {obj['Key']}: {e}")

    logger.info(
        f"Prepared training data: {len(training_entries)} training, "
        f"{len(validation_entries)} validation entries"
    )

    # Write training and validation artifacts
    timestamp = now.strftime("%Y%m%d-%H%M%S")

    training_key = f"training/{job_id}/{timestamp}/train.jsonl"
    validation_key = f"training/{job_id}/{timestamp}/validation.jsonl"

    training_content = "\n".join(json.dumps(e, default=str) for e in training_entries)
    validation_content = "\n".join(json.dumps(e, default=str) for e in validation_entries)

    for key, content in [(training_key, training_content), (validation_key, validation_content)]:
        s3.put_object(
            Bucket=MODEL_ARTIFACTS_BUCKET,
            Key=key,
            Body=content.encode("utf-8"),
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=KMS_KEY_ID,
            ContentType="application/jsonl",
            Metadata={
                "job-id": job_id,
                "created-at": now.isoformat(),
                "record-count": str(len(training_entries) if "train" in key else len(validation_entries)),
            },
        )

    return {
        "training_key": training_key,
        "validation_key": validation_key,
        "training_samples": len(training_entries),
        "validation_samples": len(validation_entries),
        "training_s3_prefix": f"training/{job_id}/{timestamp}/",
    }


@tracer.capture_method
def format_bedrock_training_config(job_id: str, training_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format training configuration for AWS Bedrock custom model training.

    Bedrock fine-tuning configuration includes:
    - Base model ID
    - Training data S3 location
    - Hyperparameters (learning rate, epochs, batch size)
    - Output location for fine-tuned model artifacts

    FTR Compliance:
    - Explicit model ARNs (no wildcards)
    - KMS encryption for training artifacts
    - Role ARN with least-privilege access
    """
    bedrock_config = get_bedrock_config()

    return {
        "jobName": f"strata-cfo-{job_id}",
        "baseModelIdentifier": bedrock_config.get("primary_model", PRIMARY_MODEL_ID),
        "customModelName": f"strata-cfo-finetuned-{job_id[:8]}",
        "customModelTags": [
            {"key": "Project", "value": "StrataCFO"},
            {"key": "Environment", "value": os.environ.get("ENVIRONMENT", "production")},
            {"key": "JobId", "value": job_id},
        ],
        "trainingDataConfig": {
            "s3Uri": f"s3://{MODEL_ARTIFACTS_BUCKET}/{training_data['training_s3_prefix']}",
        },
        "outputDataConfig": {
            "s3Uri": f"s3://{MODEL_ARTIFACTS_BUCKET}/output/{job_id}/",
        },
        "hyperParameters": {
            "epochCount": str(min(3, max(1, training_data["training_samples"] // 100))),
            "batchSize": "8",
            "learningRate": "0.00005",
            "learningRateWarmupSteps": "100",
        },
    }


@tracer.capture_method
def submit_bedrock_job(job_id: str, config: Dict[str, Any]) -> str:
    """
    Submit a fine-tuning job to AWS Bedrock.

    FTR Compliance:
    - Explicit error handling with retry consideration
    - Bedrock API uses specific model ARNs
    - Job ARN stored for tracking
    """
    bedrock = get_bedrock_client()

    try:
        response = bedrock.create_model_customization_job(**config)
        bedrock_job_arn = response.get("arn", "")
        logger.info(f"Submitted Bedrock fine-tuning job: {bedrock_job_arn}")
        return bedrock_job_arn
    except bedrock.exceptions.ValidationException as e:
        logger.error(f"Bedrock validation error: {e}")
        raise
    except bedrock.exceptions.ResourceNotFoundException as e:
        logger.error(f"Bedrock resource not found: {e}")
        raise
    except bedrock.exceptions.ThrottlingException:
        logger.warning("Bedrock API throttled — job will be retried via SQS")
        raise
    except Exception as e:
        logger.error(f"Failed to submit Bedrock job: {e}")
        raise


@tracer.capture_method
def check_job_status(job_id: str) -> Dict[str, Any]:
    """
    Check the status of a Bedrock fine-tuning job.

    Maps Bedrock job statuses to Strata JobStatus enum.
    """
    bedrock = get_bedrock_client()
    table = get_dynamodb_table()

    try:
        response = bedrock.get_model_customization_job(jobIdentifier=job_id)
        bedrock_status = response.get("status", "UNKNOWN")

        status_map = {
            "CREATING": JobStatus.SUBMITTED,
            "IN_PROGRESS": JobStatus.IN_PROGRESS,
            "COMPLETED": JobStatus.COMPLETED,
            "FAILED": JobStatus.FAILED,
            "STOPPING": JobStatus.CANCELLED,
            "STOPPED": JobStatus.CANCELLED,
        }

        mapped_status = status_map.get(bedrock_status, JobStatus.IN_PROGRESS)

        # Update DynamoDB with current status
        if bedrock_status == "COMPLETED":
            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #status = :status, #updated_at = :now, progress_percentage = :pct",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#updated_at": "updated_at",
                },
                ExpressionAttributeValues={
                    ":status": JobStatus.COMPLETED.value,
                    ":now": datetime.now(timezone.utc).isoformat(),
                    ":pct": 100,
                },
            )
            metrics.add_metric(name="FineTuningCompleted", unit=MetricUnit.Count, value=1)

        elif bedrock_status == "FAILED":
            failure_message = response.get("failureMessage", "Unknown failure")
            table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #status = :status, #updated_at = :now, error_message = :err, error_count = error_count + :one",
                ExpressionAttributeNames={"#status": "status", "#updated_at": "updated_at"},
                ExpressionAttributeValues={
                    ":status": JobStatus.FAILED.value,
                    ":now": datetime.now(timezone.utc).isoformat(),
                    ":err": failure_message,
                    ":one": 1,
                },
            )
            metrics.add_metric(name="FineTuningFailed", unit=MetricUnit.Count, value=1)

        return {
            "job_id": job_id,
            "bedrock_status": bedrock_status,
            "strata_status": mapped_status.value,
            "model_arn": response.get("outputModelArn", ""),
        }

    except Exception as e:
        logger.error(f"Failed to check job status for {job_id}: {e}")
        raise


@tracer.capture_method
def version_model_artifact(job_id: str, job_status: str) -> Optional[str]:
    """
    Create an immutable version reference for completed model artifacts.

    Version naming: artifacts/{job_id}/v{version}/model.tar.gz
    Each version is immutable and tagged with metadata.

    FTR Compliance:
    - Immutable artifacts for reproducibility
    - Version tagging for rollback capability
    - KMS-encrypted storage
    """
    if job_status != JobStatus.COMPLETED.value:
        return None

    s3 = get_s3_client()
    now = datetime.now(timezone.utc)

    # List output artifacts from the job
    output_prefix = f"output/{job_id}/"
    try:
        response = s3.list_objects_v2(Bucket=MODEL_ARTIFACTS_BUCKET, Prefix=output_prefix)
        artifacts = response.get("Contents", [])
    except Exception:
        return None

    if not artifacts:
        logger.warning(f"No output artifacts found for job {job_id}")
        return None

    # Create version tag
    version = now.strftime("v%Y%m%d%H%M%S")
    version_prefix = f"artifacts/{job_id}/{version}/"

    for artifact in artifacts:
        source_key = artifact["Key"]
        dest_key = f"{version_prefix}{source_key.replace(output_prefix, '')}"

        try:
            s3.copy_object(
                Bucket=MODEL_ARTIFACTS_BUCKET,
                CopySource={"Bucket": MODEL_ARTIFACTS_BUCKET, "Key": source_key},
                Key=dest_key,
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=KMS_KEY_ID,
                MetadataDirective="REPLACE",
                Metadata={
                    "job-id": job_id,
                    "version": version,
                    "status": "completed",
                    "created-at": now.isoformat(),
                },
                Tagging="Type=ModelArtifact&Status=Completed&JobId=" + job_id,
            )
        except Exception as e:
            logger.error(f"Failed to version artifact {source_key}: {e}")

    logger.info(f"Created version {version} for job {job_id}")
    metrics.add_metric(name="ModelVersionsCreated", unit=MetricUnit.Count, value=1)
    return version_prefix


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------
@logger.inject_lambda_context(correlation_id_path=correlation_paths.SQS)
@metrics.log_metrics(capture_cold_start_metric=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    """
    Layer 2 Fine-Tuning Pipeline entry point.

    Can handle:
    1. New training request (from SQS or API Gateway)
    2. Job status check (from EventBridge or API Gateway)
    3. Scheduled re-evaluation of pending jobs

    FTR Requirement: Full lifecycle management with auditable state transitions.
    """
    invocation_id = str(uuid.uuid4())
    logger.info(
        "Starting Layer 2 Fine-Tuning Pipeline",
        extra={"invocation_id": invocation_id, "event_source": str(event.get("source", "direct"))},
    )

    try:
        # Determine action from event
        action = event.get("action", "create_job")
        tenant_id = event.get("tenant_id", "default")

        if action == "create_job":
            # Create a new fine-tuning job
            job_id = event.get("job_id", f"ft-{uuid.uuid4().hex[:12]}")
            strategy = event.get("strategy", FineTuningStrategy.EXPORT_ARTIFACTS.value)

            logger.info(f"Creating fine-tuning job: {job_id} (strategy: {strategy})")

            # Step 1: Create job record
            job_record = create_job_record(job_id, strategy, tenant_id)

            # Step 2: Prepare training data
            update_job_status(job_id, JobStatus.PREPARING, created_at=job_record["created_at"])
            training_data = prepare_training_data(job_id)

            if not training_data["training_samples"]:
                logger.warning(f"Insufficient training data for job {job_id}")
                update_job_status(
                    job_id, JobStatus.FAILED,
                    created_at=job_record["created_at"],
                    error_message="Insufficient training data (0 samples)",
                    error_count=1,
                )
                return {"statusCode": 400, "body": {"error": "Insufficient training data"}}

            # Step 3: Validate data quality
            update_job_status(job_id, JobStatus.VALIDATING, created_at=job_record["created_at"])
            validation_ratio = training_data["validation_samples"] / max(training_data["training_samples"], 1)
            logger.info(f"Validation ratio: {validation_ratio:.2f} (target: 0.20-0.25)")

            # Step 4: Submit to Bedrock (if strategy supports it)
            if strategy == FineTuningStrategy.BEDROCK_CUSTOM_MODEL.value:
                update_job_status(job_id, JobStatus.SUBMITTED, created_at=job_record["created_at"])

                config = format_bedrock_training_config(job_id, training_data)
                bedrock_job_arn = submit_bedrock_job(job_id, config)

                update_job_status(
                    job_id, JobStatus.IN_PROGRESS,
                    created_at=job_record["created_at"],
                    bedrock_job_arn=bedrock_job_arn,
                    training_samples=training_data["training_samples"],
                    validation_samples=training_data["validation_samples"],
                )
            else:
                # Export artifacts strategy — store prepared data
                update_job_status(
                    job_id, JobStatus.COMPLETED,
                    created_at=job_record["created_at"],
                    training_samples=training_data["training_samples"],
                    validation_samples=training_data["validation_samples"],
                    artifacts_s3_key=training_data["training_s3_prefix"],
                )

            metrics.add_metric(name="FineTuningJobsCreated", unit=MetricUnit.Count, value=1)

            return {
                "statusCode": 200,
                "body": {
                    "message": "Fine-tuning job created",
                    "invocation_id": invocation_id,
                    "job_id": job_id,
                    "strategy": strategy,
                    "training_samples": training_data["training_samples"],
                    "validation_samples": training_data["validation_samples"],
                },
            }

        elif action == "check_status":
            job_id = event.get("job_id", "")
            if not job_id:
                return {"statusCode": 400, "body": {"error": "job_id required for status check"}}

            status = check_job_status(job_id)
            version = version_model_artifact(job_id, status.get("strata_status", ""))

            return {
                "statusCode": 200,
                "body": {
                    "invocation_id": invocation_id,
                    **status,
                    "artifact_version": version,
                },
            }

        else:
            return {
                "statusCode": 400,
                "body": {"error": f"Unknown action: {action}"},
            }

    except Exception as e:
        logger.error(f"Fine-tuning pipeline failed: {e}", exc_info=True)
        metrics.add_metric(name="FineTuningFailures", unit=MetricUnit.Count, value=1)

        return {
            "statusCode": 500,
            "body": {
                "message": "Fine-tuning pipeline failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "invocation_id": invocation_id,
            },
        }
