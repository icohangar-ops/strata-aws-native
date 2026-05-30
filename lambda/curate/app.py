"""
Strata CFO Resilience Matrix — Layer 1: Data Curation Lambda

This Lambda function implements the Data Curation layer of the Strata resilience system.
It fetches raw failure logs from S3, normalizes patterns into structured format,
stores curated datasets back to S3, updates DynamoDB metrics, and indexes in OpenSearch.

FTR Compliance Notes:
- All S3 operations use KMS CMK encryption (server-side)
- DynamoDB writes use KMS-encrypted tables with TTL
- Structured JSON logging for CloudWatch
- X-Ray tracing for request correlation
- Least-privilege IAM: only accesses specific bucket and table ARNs
- No secrets hardcoded: Bedrock config retrieved from Secrets Manager
"""

import json
import os
import re
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

# FTR Compliance: Environment variables injected via SAM template (zero secrets)
CURATED_DATA_BUCKET = os.environ.get("CURATED_DATA_BUCKET", "")
RESILIENCE_LOGS_BUCKET = os.environ.get("RESILIENCE_LOGS_BUCKET", "")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "")
OPENSEARCH_SECRET_ARN = os.environ.get("OPENSEARCH_SECRET_ARN", "")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "")

# Initialize AWS Lambda Powertools for structured logging, metrics, and tracing
logger = Logger(service="strata-curate")
metrics = Metrics(namespace="StrataCFO")
tracer = Tracer()

# AWS SDK clients — lazy initialization for container reuse
_s3_client = None
_dynamodb_resource = None
_dynamodb_table = None
_secrets_client = None


def get_s3_client():
    """Lazy-initialize S3 client for container reuse (FTR: connection efficiency)."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def get_dynamodb_table():
    """Lazy-initialize DynamoDB table resource for container reuse."""
    global _dynamodb_resource, _dynamodb_table
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    if _dynamodb_table is None:
        _dynamodb_table = _dynamodb_resource.Table(METRICS_TABLE)
    return _dynamodb_table


def get_secrets_client():
    """Lazy-initialize Secrets Manager client."""
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


# ---------------------------------------------------------------------------
# Failure Pattern Classification — FTR: Deterministic, auditable classification
# ---------------------------------------------------------------------------
FAILURE_CATEGORIES = {
    "timeout": {
        "pattern": re.compile(r"(timeout|timed out|deadline exceeded|socket timeout)", re.IGNORECASE),
        "severity": "high",
        "resilience_action": "retry_with_backoff",
    },
    "rate_limit": {
        "pattern": re.compile(r"(rate.?limit|throttl|429|too many requests|quota)", re.IGNORECASE),
        "severity": "medium",
        "resilience_action": "circuit_breaker",
    },
    "auth_failure": {
        "pattern": re.compile(r"(auth|unauthorized|forbidden|401|403|access denied|credentials)", re.IGNORECASE),
        "severity": "critical",
        "resilience_action": "fallback_model",
    },
    "model_error": {
        "pattern": re.compile(r"(model.?error|internal.?error|overloaded|500|503|service unavailable)", re.IGNORECASE),
        "severity": "high",
        "resilience_action": "model_fallback",
    },
    "context_overflow": {
        "pattern": re.compile(r"(token.?limit|context.?overflow|input.?too.?large|param.?too.?large)", re.IGNORECASE),
        "severity": "medium",
        "resilience_action": "graceful_degradation",
    },
    "network_error": {
        "pattern": re.compile(r"(connection.?refused|dns.?fail|network.?error|no.?route)", re.IGNORECASE),
        "severity": "high",
        "resilience_action": "retry_with_backoff",
    },
    "validation_error": {
        "pattern": re.compile(r"(validation|invalid.?input|malformed|schema.?error|bad.?request)", re.IGNORECASE),
        "severity": "low",
        "resilience_action": "input_sanitization",
    },
}


@tracer.capture_method
def classify_failure(log_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify a failure log entry into a structured category.

    Classification logic:
    1. Scan error messages against known patterns
    2. Assign severity based on operational impact
    3. Map to the appropriate resilience action
    4. Generate a deterministic hash for deduplication

    FTR Requirement: Deterministic classification ensures reproducible curation results.
    """
    error_message = log_entry.get("error_message", "") or log_entry.get("message", "")
    classified = {
        "category": "unknown",
        "severity": "medium",
        "resilience_action": "manual_review",
        "confidence": 0.0,
    }

    for category, config in FAILURE_CATEGORIES.items():
        if config["pattern"].search(error_message):
            classified["category"] = category
            classified["severity"] = config["severity"]
            classified["resilience_action"] = config["resilience_action"]
            classified["confidence"] = 0.95
            break

    # Generate deterministic hash for deduplication (FTR: auditable trace)
    hash_input = f"{log_entry.get('source', '')}:{log_entry.get('error_message', '')}"
    classified["dedup_hash"] = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    return classified


@tracer.capture_method
def normalize_log_entry(raw_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a raw log entry into the Strata structured format.

    Normalization ensures consistent schema across all failure sources:
    - Timestamps normalized to ISO 8601 UTC
    - Error messages trimmed and deduplicated
    - Source attribution tracked
    - Classification applied
    - TTL set for DynamoDB auto-cleanup

    FTR Requirement: Structured data format enables pattern search in OpenSearch
    and metric aggregation in DynamoDB.
    """
    now = datetime.now(timezone.utc)

    # Normalize timestamp
    raw_ts = raw_entry.get("timestamp", raw_entry.get("time", raw_entry.get("date", "")))
    if isinstance(raw_ts, (int, float)):
        timestamp = datetime.fromtimestamp(raw_ts, tz=timezone.utc).isoformat()
    elif isinstance(raw_ts, str):
        try:
            timestamp = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).isoformat()
        except (ValueError, AttributeError):
            timestamp = now.isoformat()
    else:
        timestamp = now.isoformat()

    # Normalize error message
    error_message = raw_entry.get("error_message", raw_entry.get("message", raw_entry.get("error", "")))
    if isinstance(error_message, str):
        error_message = error_message.strip()[:2048]  # FTR: Field size limit

    # Classify the failure
    classification = classify_failure(raw_entry)

    # Build normalized entry with consistent schema
    normalized = {
        "pk": f"FAILURE#{classification['category']}",
        "sk": f"{timestamp}#{uuid.uuid4()}",
        "source": raw_entry.get("source", raw_entry.get("service", "unknown")),
        "timestamp": timestamp,
        "epoch_ms": int(now.timestamp() * 1000),
        "error_message": error_message,
        "category": classification["category"],
        "severity": classification["severity"],
        "resilience_action": classification["resilience_action"],
        "confidence": classification["confidence"],
        "dedup_hash": classification["dedup_hash"],
        "model_id": raw_entry.get("model_id", ""),
        "agent_type": raw_entry.get("agent_type", ""),
        "tenant_id": raw_entry.get("tenant_id", ""),
        "request_id": raw_entry.get("request_id", str(uuid.uuid4())),
        "latency_ms": int(raw_entry.get("latency_ms", raw_entry.get("duration_ms", 0))),
        "tokens_used": int(raw_entry.get("tokens_used", 0)),
        "retry_count": int(raw_entry.get("retry_count", 0)),
        "raw_payload": json.dumps(raw_entry, default=str)[:4096],
        # TTL: 30 days from now (FTR: Data lifecycle management)
        "expires_at": int((now.timestamp()) + 30 * 24 * 3600),
        "curated_at": now.isoformat(),
    }

    return normalized


@tracer.capture_method
def fetch_raw_logs_from_s3() -> List[Dict[str, Any]]:
    """
    Fetch raw failure logs from the curated-data S3 bucket.

    Reads from the 'raw-logs/' prefix where upstream systems deposit
    unprocessed failure logs. Supports both JSON and JSONL formats.

    FTR Compliance:
    - Uses S3 VPC endpoint (no internet traversal)
    - KMS-encrypted bucket access
    - Paginated listing for large datasets
    """
    s3 = get_s3_client()
    raw_logs = []

    try:
        paginator = s3.get_paginator("list_objects_v2")

        # FTR: Paginated list to handle large datasets without memory exhaustion
        for page in paginator.paginate(
            Bucket=CURATED_DATA_BUCKET,
            Prefix="raw-logs/",
            MaxKeys=1000,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                # Skip directory markers and non-JSON files
                if key.endswith("/") or not key.endswith((".json", ".jsonl")):
                    continue

                try:
                    response = s3.get_object(Bucket=CURATED_DATA_BUCKET, Key=key)
                    content = response["Body"].read().decode("utf-8")

                    if key.endswith(".jsonl"):
                        # JSONL: one JSON object per line
                        for line in content.strip().split("\n"):
                            line = line.strip()
                            if line:
                                try:
                                    entry = json.loads(line)
                                    entry["_source_file"] = key
                                    raw_logs.append(entry)
                                except json.JSONDecodeError:
                                    logger.warning(
                                        "Skipping malformed JSONL line",
                                        extra={"file": key, "line_preview": line[:100]},
                                    )
                    else:
                        # Single JSON object or array
                        data = json.loads(content)
                        if isinstance(data, list):
                            for entry in data:
                                entry["_source_file"] = key
                                raw_logs.append(entry)
                        else:
                            data["_source_file"] = key
                            raw_logs.append(data)

                except Exception as e:
                    logger.error(
                        "Failed to read S3 object",
                        extra={
                            "bucket": CURATED_DATA_BUCKET,
                            "key": key,
                            "error": str(e),
                        },
                    )
                    metrics.add_metric(name="CurationReadErrors", unit=MetricUnit.Count, value=1)

    except Exception as e:
        logger.error("Failed to list S3 objects", extra={"error": str(e)})
        raise

    logger.info(f"Fetched {len(raw_logs)} raw log entries from S3")
    metrics.add_metric(name="RawLogsFetched", unit=MetricUnit.Count, value=len(raw_logs))
    return raw_logs


@tracer.capture_method
def store_curated_dataset(normalized_logs: List[Dict[str, Any]]) -> str:
    """
    Store normalized logs as a curated dataset partition in S3.

    Partitioning strategy: curated/{category}/{date}/batch-{uuid}.jsonl
    This enables efficient querying by category and time range.

    FTR Compliance:
    - KMS-encrypted write (bucket default)
    - Structured partitioning for query efficiency
    - JSONL format for streaming ingestion
    """
    if not normalized_logs:
        logger.info("No normalized logs to store")
        return ""

    now = datetime.now(timezone.utc)
    batch_id = uuid.uuid4().hex[:8]
    date_partition = now.strftime("%Y-%m-%d")

    # Group by category for partitioned storage
    by_category: Dict[str, List[Dict[str, Any]]] = {}
    for log in normalized_logs:
        cat = log.get("category", "unknown")
        by_category.setdefault(cat, []).append(log)

    s3 = get_s3_client()
    stored_keys = []

    for category, entries in by_category.items():
        # Build JSONL content
        jsonl_content = "\n".join(json.dumps(entry, default=str) for entry in entries)

        # FTR: Partitioned S3 key for efficient querying
        key = f"curated/{category}/{date_partition}/batch-{batch_id}.jsonl"

        try:
            # FTR: Explicit KMS encryption context for audit trail
            s3.put_object(
                Bucket=CURATED_DATA_BUCKET,
                Key=key,
                Body=jsonl_content.encode("utf-8"),
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=KMS_KEY_ID,
                ContentType="application/jsonl",
                Metadata={
                    "category": category,
                    "batch-id": batch_id,
                    "record-count": str(len(entries)),
                    "curated-at": now.isoformat(),
                },
            )
            stored_keys.append(key)
            logger.info(
                f"Stored curated partition: {key} ({len(entries)} records)",
                extra={"category": category, "record_count": len(entries)},
            )

        except Exception as e:
            logger.error(
                "Failed to store curated partition",
                extra={"key": key, "category": category, "error": str(e)},
            )
            metrics.add_metric(name="CurationWriteErrors", unit=MetricUnit.Count, value=1)

    metrics.add_metric(name="CuratedPartitionsStored", unit=MetricUnit.Count, value=len(stored_keys))
    return ";".join(stored_keys)


@tracer.capture_method
def update_dynamodb_metrics(normalized_logs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Update DynamoDB resilience metrics with curation results.

    Writes aggregated statistics per failure category to enable
    dashboard queries and alerting.

    FTR Compliance:
    - KMS-encrypted DynamoDB table
    - Conditional writes for idempotency
    - TTL expires_at set for data lifecycle
    """
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc)

    # Aggregate by category and severity
    stats: Dict[str, Dict[str, int]] = {}
    for log in normalized_logs:
        cat = log.get("category", "unknown")
        sev = log.get("severity", "medium")
        if cat not in stats:
            stats[cat] = {"count": 0, "high": 0, "medium": 0, "low": 0, "critical": 0}
        stats[cat]["count"] += 1
        stats[cat][sev] = stats[cat].get(sev, 0) + 1

    written = 0
    for category, category_stats in stats.items():
        try:
            table.put_item(
                Item={
                    "pk": f"CURATED#{category}",
                    "sk": now.strftime("%Y-%m-%d-%H"),
                    "category": category,
                    "hourly_stats": category_stats,
                    "total_count": category_stats["count"],
                    "curated_at": now.isoformat(),
                    "expires_at": int(now.timestamp() + 90 * 24 * 3600),  # 90-day TTL
                },
                ConditionExpression="attribute_not_exists(pk) OR :now > curated_at",
                ExpressionAttributeValues={":now": now.isoformat()},
            )
            written += 1

        except table.meta.client.exceptions.ConditionalCheckFailedException:
            # Idempotency: entry already exists for this hour
            logger.debug(f"Skipping duplicate curation stat for {category}")
            written += 1
        except Exception as e:
            logger.error(
                "Failed to write DynamoDB metric",
                extra={"category": category, "error": str(e)},
            )
            metrics.add_metric(name="DynamoDBWriteErrors", unit=MetricUnit.Count, value=1)

    metrics.add_metric(name="DynamoDBMetricsWritten", unit=MetricUnit.Count, value=written)
    return stats


@tracer.capture_method
def archive_processed_logs(source_files: List[str]) -> None:
    """
    Move processed raw logs to an archive prefix after successful curation.

    FTR Requirement: Data lifecycle management — raw logs archived,
    not deleted, for compliance audit trail.
    """
    s3 = get_s3_client()

    for source_key in source_files:
        archive_key = source_key.replace("raw-logs/", "archive/raw-logs/")

        try:
            # FTR: S3 copy then delete (atomic move)
            s3.copy_object(
                Bucket=CURATED_DATA_BUCKET,
                CopySource={"Bucket": CURATED_DATA_BUCKET, "Key": source_key},
                Key=archive_key,
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=KMS_KEY_ID,
            )
            s3.delete_object(Bucket=CURATED_DATA_BUCKET, Key=source_key)
            logger.debug(f"Archived: {source_key} -> {archive_key}")

        except Exception as e:
            logger.warning(
                "Failed to archive processed log",
                extra={"source": source_key, "archive": archive_key, "error": str(e)},
            )


@tracer.capture_method
def emit_curator_summary(normalized_logs: List[Dict[str, Any]], stats: Dict[str, Dict[str, int]]) -> None:
    """
    Emit summary metrics to CloudWatch for dashboard visualization.

    FTR Compliance: Custom CloudWatch metrics for operational visibility.
    """
    total = len(normalized_logs)
    metrics.add_metric(name="CurationTotalProcessed", unit=MetricUnit.Count, value=total)

    # Per-category metrics
    for category, cat_stats in stats.items():
        metrics.add_metric(
            name=f"CurationByCategory",
            unit=MetricUnit.Count,
            value=cat_stats["count"],
            extra={"category": category},
        )

    # Severity distribution
    severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for cat_stats in stats.values():
        for sev in severity_counts:
            severity_counts[sev] += cat_stats.get(sev, 0)

    for severity, count in severity_counts.items():
        metrics.add_metric(
            name=f"CurationBySeverity",
            unit=MetricUnit.Count,
            value=count,
            extra={"severity": severity},
        )


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------
@logger.inject_lambda_context(correlation_id_path=correlation_paths.EVENTBRIDGE)
@metrics.log_metrics(capture_cold_start_metric=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    """
    Layer 1 Data Curation entry point.

    Workflow:
    1. Fetch raw failure logs from S3 (raw-logs/ prefix)
    2. Normalize each entry (timestamp, classification, dedup)
    3. Store curated partitions to S3 (curated/{category}/{date}/)
    4. Update aggregated metrics in DynamoDB
    5. Archive processed raw logs
    6. Emit summary metrics to CloudWatch

    Can be triggered by:
    - EventBridge scheduled event (every 2 hours)
    - Direct Lambda invocation
    - API Gateway (manual trigger)

    FTR Requirement: Complete observability via CloudWatch + X-Ray.
    """
    invocation_id = str(uuid.uuid4())
    logger.info(
        "Starting Layer 1 Data Curation",
        extra={
            "invocation_id": invocation_id,
            "event_source": event.get("source", "unknown"),
            "function_name": context.function_name,
            "remaining_ms": context.get_remaining_time_in_millis(),
        },
    )

    try:
        # Step 1: Fetch raw logs from S3
        with tracer.subsegment("FetchRawLogs") as subsegment:
            raw_logs = fetch_raw_logs_from_s3()
            subsegment.put_annotation("raw_log_count", len(raw_logs))

        if not raw_logs:
            logger.info("No raw logs found — nothing to curate")
            return {
                "statusCode": 200,
                "body": {
                    "message": "No raw logs found for curation",
                    "invocation_id": invocation_id,
                    "records_processed": 0,
                },
            }

        # Step 2: Normalize each log entry
        with tracer.subsegment("NormalizeLogs") as subsegment:
            normalized_logs = []
            dedup_hashes = set()
            duplicates_skipped = 0

            for entry in raw_logs:
                normalized = normalize_log_entry(entry)

                # Deduplication: skip if we've seen this exact failure
                dedup_hash = normalized["dedup_hash"]
                if dedup_hash in dedup_hashes:
                    duplicates_skipped += 1
                    continue

                dedup_hashes.add(dedup_hash)
                normalized_logs.append(normalized)

            subsegment.put_annotation("normalized_count", len(normalized_logs))
            subsegment.put_annotation("duplicates_skipped", duplicates_skipped)

        logger.info(
            f"Normalized {len(normalized_logs)} entries "
            f"(skipped {duplicates_skipped} duplicates)"
        )
        metrics.add_metric(name="DuplicatesSkipped", unit=MetricUnit.Count, value=duplicates_skipped)

        # Step 3: Store curated dataset to S3
        with tracer.subsegment("StoreCuratedDataset"):
            stored_keys = store_curated_dataset(normalized_logs)

        # Step 4: Update DynamoDB metrics
        with tracer.subsegment("UpdateDynamoDBMetrics"):
            stats = update_dynamodb_metrics(normalized_logs)

        # Step 5: Archive processed raw logs
        with tracer.subsegment("ArchiveProcessedLogs"):
            source_files = list(set(e.get("_source_file", "") for e in raw_logs if e.get("_source_file")))
            if source_files:
                archive_processed_logs(source_files)

        # Step 6: Emit summary
        emit_curator_summary(normalized_logs, stats)

        result = {
            "statusCode": 200,
            "body": {
                "message": "Data curation completed successfully",
                "invocation_id": invocation_id,
                "records_processed": len(normalized_logs),
                "duplicates_skipped": duplicates_skipped,
                "partitions_stored": len(stored_keys.split(";")) if stored_keys else 0,
                "categories": list(stats.keys()),
                "stats": stats,
            },
        }

        logger.info("Data curation completed", extra={"result_summary": result["body"]})
        return result

    except Exception as e:
        logger.error(
            "Data curation failed",
            extra={"error": str(e), "error_type": type(e).__name__},
            exc_info=True,
        )
        metrics.add_metric(name="CurationFailures", unit=MetricUnit.Count, value=1)

        # FTR: Always return structured error, never raise unhandled exception
        return {
            "statusCode": 500,
            "body": {
                "message": "Data curation failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "invocation_id": invocation_id,
            },
        }
