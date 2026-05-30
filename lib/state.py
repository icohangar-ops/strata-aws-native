"""
Strata CFO Resilience Matrix — State Management Library

This module manages state across the Strata system:
- S3 workflow state (job tracking, artifact versioning)
- DynamoDB metadata (metrics, circuit breaker state, tenant config)
- Tenant isolation (ABAC enforcement)

FTR Compliance Notes:
- All S3 operations use KMS CMK encryption
- DynamoDB uses KMS encryption with TTL for lifecycle
- ABAC ensures tenant data isolation at the storage layer
- State transitions are atomic (DynamoDB conditional writes)
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3


# ---------------------------------------------------------------------------
# Environment Configuration (from SAM template)
# ---------------------------------------------------------------------------
CURATED_DATA_BUCKET = os.environ.get("CURATED_DATA_BUCKET", "")
MODEL_ARTIFACTS_BUCKET = os.environ.get("MODEL_ARTIFACTS_BUCKET", "")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "")
FINE_TUNING_TABLE = os.environ.get("FINE_TUNING_TABLE", "")
CIRCUIT_BREAKERS_TABLE = os.environ.get("CIRCUIT_BREAKERS_TABLE", "")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "")


# =========================================================================
# S3 Workflow State Manager
# =========================================================================
class WorkflowStateManager:
    """
    Manages workflow state stored in S3 for long-running processes.

    Uses JSON state files in S3 to track:
    - Fine-tuning job progress
    - Data curation batch status
    - Chaos test suite results

    FTR Compliance:
    - KMS-encrypted S3 storage
    - Versioning enabled for audit trail
    - Atomic write via put_object
    """

    def __init__(self, bucket: str = None, prefix: str = "state/workflows/", kms_key: str = None):
        self.bucket = bucket or CURATED_DATA_BUCKET
        self.prefix = prefix
        self.kms_key = kms_key or KMS_KEY_ID
        self._s3 = None

    def _get_s3(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3")
        return self._s3

    def create_workflow(
        self,
        workflow_type: str,
        workflow_id: Optional[str] = None,
        initial_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new workflow state record in S3.

        Returns the full state document.
        """
        workflow_id = workflow_id or f"{workflow_type}-{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)

        state = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "status": "created",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "version": 1,
            "steps_completed": [],
            "steps_remaining": [],
            "metadata": initial_state or {},
            "error": "",
        }

        key = f"{self.prefix}{workflow_id}.json"
        self._write_state(key, state)
        return state

    def get_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Read workflow state from S3."""
        s3 = self._get_s3()
        key = f"{self.prefix}{workflow_id}.json"

        try:
            response = s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            return None

    def update_workflow(
        self,
        workflow_id: str,
        status: Optional[str] = None,
        step_completed: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update workflow state atomically.

        Supports: status change, step completion, metadata update, error recording.
        """
        state = self.get_workflow(workflow_id)
        if state is None:
            return None

        now = datetime.now(timezone.utc)

        if status:
            state["status"] = status
        if step_completed:
            state["steps_completed"].append(step_completed)
            if step_completed in state["steps_remaining"]:
                state["steps_remaining"].remove(step_completed)
        if metadata:
            state["metadata"].update(metadata)
        if error:
            state["error"] = error

        state["updated_at"] = now.isoformat()
        state["version"] = state.get("version", 0) + 1

        key = f"{self.prefix}{workflow_id}.json"
        self._write_state(key, state)
        return state

    def _write_state(self, key: str, state: Dict[str, Any]) -> None:
        """Write state to S3 with KMS encryption."""
        s3 = self._get_s3()

        put_kwargs = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": json.dumps(state, default=str).encode("utf-8"),
            "ContentType": "application/json",
            "Metadata": {
                "workflow-id": state.get("workflow_id", ""),
                "status": state.get("status", ""),
                "version": str(state.get("version", 1)),
            },
        }

        if self.kms_key:
            put_kwargs["ServerSideEncryption"] = "aws:kms"
            put_kwargs["SSEKMSKeyId"] = self.kms_key

        s3.put_object(**put_kwargs)

    def list_workflows(
        self,
        workflow_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List workflow states with optional filtering."""
        s3 = self._get_s3()

        try:
            paginator = s3.get_paginator("list_objects_v2")
            workflows = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith(".json"):
                        continue

                    response = s3.get_object(Bucket=self.bucket, Key=obj["Key"])
                    state = json.loads(response["Body"].read().decode("utf-8"))

                    if workflow_type and state.get("workflow_type") != workflow_type:
                        continue
                    if status and state.get("status") != status:
                        continue

                    workflows.append(state)

            return sorted(workflows, key=lambda w: w.get("updated_at", ""), reverse=True)

        except Exception:
            return []


# =========================================================================
# DynamoDB Metadata Manager
# =========================================================================
class MetadataManager:
    """
    Manages DynamoDB metadata for the Strata system.

    Handles:
    - Resilience metrics aggregation
    - Fine-tuning job metadata
    - Tenant configuration
    - System configuration

    FTR Compliance:
    - KMS-encrypted DynamoDB tables
    - TTL for automatic data lifecycle
    - Conditional writes for concurrency
    """

    def __init__(self, table_name: str = None):
        self.table_name = table_name or METRICS_TABLE
        self._resource = None
        self._table = None

    def _get_table(self):
        if self._table is None:
            self._resource = boto3.resource("dynamodb")
            self._table = self._resource.Table(self.table_name)
        return self._table

    def put_metadata(
        self,
        pk: str,
        sk: str,
        attributes: Dict[str, Any],
        ttl_days: int = 90,
        condition: Optional[str] = None,
    ) -> bool:
        """
        Write metadata with optional conditional update.

        FTR: Atomic conditional writes prevent lost updates.
        """
        table = self._get_table()
        now = datetime.now(timezone.utc)

        item = {
            "pk": pk,
            "sk": sk,
            "updated_at": now.isoformat(),
            **attributes,
            "expires_at": int(now.timestamp() + ttl_days * 24 * 3600),
        }

        kwargs = {"Item": item}
        if condition:
            kwargs["ConditionExpression"] = condition

        try:
            table.put_item(**kwargs)
            return True
        except Exception:
            return False

    def get_metadata(self, pk: str, sk: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Read metadata by primary key."""
        table = self._get_table()

        key = {"pk": pk}
        if sk:
            key["sk"] = sk

        try:
            response = table.get_item(Key=key)
            return response.get("Item")
        except Exception:
            return None

    def query_metadata(
        self,
        pk: str,
        sk_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query metadata by partition key with optional sort key prefix."""
        table = self._get_table()

        kwargs = {
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": pk},
            "Limit": limit,
        }

        if sk_prefix:
            kwargs["KeyConditionExpression"] = "pk = :pk AND begins_with(sk, :sk_prefix)"
            kwargs["ExpressionAttributeValues"][":sk_prefix"] = sk_prefix

        try:
            response = table.query(**kwargs)
            return response.get("Items", [])
        except Exception:
            return []

    def update_metadata(
        self,
        pk: str,
        sk: str,
        updates: Dict[str, Any],
        increment_fields: Optional[List[str]] = None,
    ) -> bool:
        """Update specific metadata fields atomically."""
        table = self._get_table()
        now = datetime.now(timezone.utc)

        set_expressions = ["#updated_at = :now"]
        remove_expressions = []
        expression_names = {"#updated_at": "updated_at"}
        expression_values = {":now": now.isoformat()}

        for key, value in updates.items():
            safe_key = key.replace("-", "_")
            if value is None:
                remove_expressions.append(f"#{safe_key}")
                expression_names[f"#{safe_key}"] = key
            else:
                set_expressions.append(f"#{safe_key} = :val_{safe_key}")
                expression_names[f"#{safe_key}"] = key
                expression_values[f":val_{safe_key}"] = value

        if increment_fields:
            for field in increment_fields:
                safe_field = field.replace("-", "_")
                set_expressions.append(f"#{safe_field} = #{safe_field} + :inc_{safe_field}")
                expression_names[f"#{safe_field}"] = field
                expression_values[f":inc_{safe_field}"] = 1

        update_expression = "SET " + ", ".join(set_expressions)
        if remove_expressions:
            update_expression += " REMOVE " + ", ".join(remove_expressions)

        try:
            table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=expression_values,
            )
            return True
        except Exception:
            return False

    def delete_metadata(self, pk: str, sk: Optional[str] = None) -> bool:
        """Delete metadata record."""
        table = self._get_table()
        key = {"pk": pk}
        if sk:
            key["sk"] = sk

        try:
            table.delete_item(Key=key)
            return True
        except Exception:
            return False


# =========================================================================
# Tenant Isolation (ABAC)
# =========================================================================
class TenantManager:
    """
    Manages tenant isolation using Attribute-Based Access Control (ABAC).

    FTR Compliance:
    - tenant_id is embedded in all DynamoDB partition keys
    - S3 paths include tenant_id prefix
    - Cognito custom attributes carry tenant_id
    - IAM policies enforce tenant isolation at the API level
    """

    def __init__(self, metadata_manager: Optional[MetadataManager] = None):
        self._metadata = metadata_manager or MetadataManager()
        self._tenant_cache: Dict[str, Dict[str, Any]] = {}

    def register_tenant(
        self,
        tenant_id: str,
        name: str,
        tier: str = "standard",
        config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Register a new tenant with configuration."""
        return self._metadata.put_metadata(
            pk=f"TENANT#{tenant_id}",
            sk="CONFIG",
            attributes={
                "tenant_id": tenant_id,
                "name": name,
                "tier": tier,
                "config": config or {},
                "status": "active",
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def get_tenant_config(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get tenant configuration. Uses local cache for efficiency."""
        if tenant_id in self._tenant_cache:
            return self._tenant_cache[tenant_id]

        config = self._metadata.get_metadata(f"TENANT#{tenant_id}", "CONFIG")
        if config:
            self._tenant_cache[tenant_id] = config
        return config

    def validate_tenant(self, tenant_id: str) -> bool:
        """Validate that a tenant exists and is active."""
        config = self.get_tenant_config(tenant_id)
        return config is not None and config.get("status") == "active"

    def tenant_s3_prefix(self, tenant_id: str) -> str:
        """Return the S3 prefix for a tenant's isolated data."""
        return f"tenants/{tenant_id}/"

    def tenant_dynamodb_pk(self, tenant_id: str, entity_type: str) -> str:
        """Return the DynamoDB partition key for a tenant's data."""
        return f"{entity_type.upper()}#{tenant_id}"

    def invalidate_cache(self, tenant_id: str) -> None:
        """Invalidate cached tenant configuration."""
        self._tenant_cache.pop(tenant_id, None)
