"""
Strata CFO Resilience Matrix — Cross-Cutting Concerns Lambda Layer

This module provides shared utilities used across all Lambda functions:
- Structured logging (JSON format for CloudWatch Insights)
- Metrics emission (CloudWatch custom metrics)
- X-Ray tracing helpers (subsegments, annotations)
- Error classification and handling patterns

Packaged as a Lambda Layer for code reuse across all 6 function layers.

FTR Compliance Notes:
- All logging is structured JSON for CloudWatch Insights querying
- Custom metrics use the StrataCFO namespace for dashboard consistency
- X-Ray subsegments provide per-layer trace visibility
- Error classification enables automated remediation
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

import boto3

# =========================================================================
# Error Classification — FTR: Deterministic, auditable error taxonomy
# =========================================================================
class ErrorSeverity(Enum):
    """Error severity levels for classification and alerting."""
    LOW = "low"            # Informational, no action needed
    MEDIUM = "medium"      # Potential issue, monitoring recommended
    HIGH = "high"          # Service degradation, investigation needed
    CRITICAL = "critical"  # Service outage, immediate response required


class ErrorCategory(Enum):
    """Error categories for automated remediation routing."""
    TRANSIENT = "transient"        # Retryable network/service errors
    RATE_LIMIT = "rate_limit"      # Throttling, backoff and retry
    AUTH = "auth"                  # Authentication/authorization failures
    VALIDATION = "validation"      # Input validation errors
    RESOURCE = "resource"          # Resource exhaustion
    TIMEOUT = "timeout"            # Timeout exceeded
    DEPENDENCY = "dependency"      # Downstream service failure
    CONFIGURATION = "configuration" # Misconfiguration
    UNKNOWN = "unknown"            # Unclassified error


class ClassifiedError:
    """
    Structured error with classification for automated handling.

    FTR Compliance:
    - Every error is classified for automated remediation
    - Error metadata includes full context for debugging
    - Structured format enables CloudWatch Insights querying
    """

    _SEVERITY_MAP = {
        "ThrottlingException": (ErrorSeverity.MEDIUM, ErrorCategory.RATE_LIMIT),
        "ProvisionedThroughputExceededException": (ErrorSeverity.MEDIUM, ErrorCategory.RATE_LIMIT),
        "ServiceUnavailable": (ErrorSeverity.HIGH, ErrorCategory.TRANSIENT),
        "InternalServerException": (ErrorSeverity.HIGH, ErrorCategory.DEPENDENCY),
        "ConnectionError": (ErrorSeverity.HIGH, ErrorCategory.TRANSIENT),
        "TimeoutError": (ErrorSeverity.HIGH, ErrorCategory.TIMEOUT),
        "ReadTimeoutError": (ErrorSeverity.HIGH, ErrorCategory.TIMEOUT),
        "ValidationException": (ErrorSeverity.LOW, ErrorCategory.VALIDATION),
        "AccessDeniedException": (ErrorSeverity.CRITICAL, ErrorCategory.AUTH),
        "UnrecognizedClientException": (ErrorSeverity.CRITICAL, ErrorCategory.AUTH),
        "ResourceNotFoundException": (ErrorSeverity.MEDIUM, ErrorCategory.RESOURCE),
        "ResourceExhaustedException": (ErrorSeverity.HIGH, ErrorCategory.RESOURCE),
        "SerializationException": (ErrorSeverity.LOW, ErrorCategory.CONFIGURATION),
    }

    def __init__(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        self.error = error
        self.error_type = type(error).__name__
        self.error_message = str(error)[:2000]  # FTR: Field size limit
        self.context = context or {}
        self.classified_at = datetime.now(timezone.utc).isoformat()
        self.error_id = str(uuid.uuid4())

        # Classify
        self.severity, self.category = self._SEVERITY_MAP.get(
            self.error_type,
            (ErrorSeverity.MEDIUM, ErrorCategory.UNKNOWN),
        )

    @property
    def is_retryable(self) -> bool:
        """Whether the error should trigger a retry."""
        return self.category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.RATE_LIMIT,
            ErrorCategory.TIMEOUT,
        )

    @property
    def should_alert(self) -> bool:
        """Whether the error should trigger an alert."""
        return self.severity in (ErrorSeverity.HIGH, ErrorSeverity.CRITICAL)

    @property
    def remediation_action(self) -> str:
        """Suggested automated remediation action."""
        action_map = {
            ErrorCategory.TRANSIENT: "retry_with_backoff",
            ErrorCategory.RATE_LIMIT: "circuit_breaker_backoff",
            ErrorCategory.AUTH: "refresh_credentials",
            ErrorCategory.VALIDATION: "sanitize_input",
            ErrorCategory.RESOURCE: "scale_up",
            ErrorCategory.TIMEOUT: "increase_timeout",
            ErrorCategory.DEPENDENCY: "activate_fallback",
            ErrorCategory.CONFIGURATION: "review_config",
            ErrorCategory.UNKNOWN: "manual_review",
        }
        return action_map.get(self.category, "manual_review")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for structured logging."""
        return {
            "error_id": self.error_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "severity": self.severity.value,
            "category": self.category.value,
            "is_retryable": self.is_retryable,
            "should_alert": self.should_alert,
            "remediation_action": self.remediation_action,
            "context": self.context,
            "classified_at": self.classified_at,
        }

    def to_cloudwatch_json(self) -> str:
        """Format for CloudWatch structured logging."""
        return json.dumps({
            "level": "ERROR",
            "error": self.to_dict(),
            "timestamp": self.classified_at,
        })


# =========================================================================
# Structured JSON Logger
# =========================================================================
class StructuredLogger:
    """
    JSON-structured logger for CloudWatch Insights compatibility.

    FTR Compliance:
    - All log entries are valid JSON for CloudWatch Insights
    - Consistent field naming for query efficiency
    - Correlation IDs for distributed tracing
    """

    def __init__(self, service_name: str, log_level: str = "INFO"):
        self.service_name = service_name
        self.log_level = log_level.upper()
        self._log_level_map = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
        self._threshold = self._log_level_map.get(self.log_level, 1)

    def _should_log(self, level: str) -> bool:
        return self._log_level_map.get(level, 0) >= self._threshold

    def _emit(self, level: str, message: str, **kwargs) -> None:
        if not self._should_log(level):
            return

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "service": self.service_name,
            "message": message,
            "environment": os.environ.get("ENVIRONMENT", "unknown"),
            "aws_request_id": os.environ.get("AWS_REQUEST_ID", ""),
            **kwargs,
        }
        print(json.dumps(entry, default=str))

    def debug(self, message: str, **kwargs) -> None:
        self._emit("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._emit("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._emit("WARNING", message, **kwargs)

    def error(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        if error:
            classified = ClassifiedError(error, context=kwargs)
            kwargs["error"] = classified.to_dict()
        self._emit("ERROR", message, **kwargs)

    def critical(self, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        if error:
            classified = ClassifiedError(error, context=kwargs)
            kwargs["error"] = classified.to_dict()
        self._emit("CRITICAL", message, **kwargs)


# =========================================================================
# CloudWatch Metrics Emitter
# =========================================================================
class MetricsEmitter:
    """
    CloudWatch custom metrics emitter.

    FTR Compliance:
    - All metrics use the StrataCFO namespace
    - Dimensions are consistent for dashboard aggregation
    - Metric units are properly typed for graph scaling
    """

    def __init__(self, namespace: str = "StrataCFO", region: str = None):
        self.namespace = namespace
        self._region = region or os.environ.get("AWS_REGION", "us-east-1")
        self._client = None
        self._buffer: List[Dict[str, Any]] = []
        self._max_buffer_size = 20

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("cloudwatch", region_name=self._region)
        return self._client

    def put_metric(
        self,
        name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Add a metric to the buffer. Flushes when buffer is full.

        FTR: Buffering reduces API calls and costs.
        """
        metric_data = {
            "MetricName": name,
            "Value": value,
            "Unit": unit,
        }

        if dimensions:
            metric_data["Dimensions"] = [
                {"Name": k, "Value": v} for k, v in dimensions.items()
            ]

        self._buffer.append(metric_data)

        if len(self._buffer) >= self._max_buffer_size:
            self.flush()

    def put_latency(self, name: str, value_ms: float, dimensions: Optional[Dict[str, str]] = None) -> None:
        """Convenience method for latency metrics in milliseconds."""
        self.put_metric(name, value_ms, unit="Milliseconds", dimensions=dimensions)

    def flush(self) -> None:
        """Flush buffered metrics to CloudWatch."""
        if not self._buffer:
            return

        try:
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=self._buffer,
            )
            self._buffer = []
        except Exception as e:
            # FTR: Never let metrics emission failures crash the function
            self._buffer = []


# =========================================================================
# X-Ray Tracing Helpers
# =========================================================================
class TraceHelper:
    """
    Helper class for X-Ray tracing with subsegments and annotations.

    FTR Compliance: X-Ray provides end-to-end request tracing across
    all 6 Lambda functions.
    """

    @staticmethod
    def add_annotation(key: str, value: Any) -> None:
        """Add annotation to current X-Ray subsegment."""
        try:
            from aws_xray_sdk import global_sdk_context
            segment = global_sdk_context.get_local().get_segment()
            if segment:
                segment.put_annotation(key, value)
        except ImportError:
            pass
        except Exception:
            pass

    @staticmethod
    def add_metadata(key: str, value: Any) -> None:
        """Add metadata to current X-Ray subsegment."""
        try:
            from aws_xray_sdk import global_sdk_context
            segment = global_sdk_context.get_local().get_segment()
            if segment:
                segment.put_metadata(key, value, "strata")
        except ImportError:
            pass
        except Exception:
            pass

    @staticmethod
    def create_subsegment(name: str) -> Optional[Any]:
        """Create an X-Ray subsegment for tracing."""
        try:
            from aws_xray_sdk import global_sdk_context
            segment = global_sdk_context.get_local().get_segment()
            if segment:
                return segment.begin_subsegment(name)
        except (ImportError, Exception):
            pass
        return None

    @staticmethod
    def end_subsegment(subsegment: Any) -> None:
        """Close an X-Ray subsegment."""
        if subsegment:
            try:
                subsegment.end()
            except Exception:
                pass


# =========================================================================
# Cognito Pre-Token Handler
# =========================================================================
def cognito_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Cognito Pre-Token Generation Lambda handler.

    Injects tenant_id into the JWT token claims for ABAC authorization.

    FTR Compliance:
    - tenant_id is an immutable user attribute (set at creation)
    - Role is injected for ABAC policy evaluation
    - Token generation is idempotent
    """
    user_attributes = event.get("request", {}).get("userAttributes", {})
    tenant_id = user_attributes.get("tenant_id", "default")
    role = user_attributes.get("role", "viewer")

    # Build claims to inject into the JWT
    claims_and_scope_override_details = {
        "groupOverrideDetails": {
            "groupsToOverride": [f"tenant:{tenant_id}"],
            "iamRolesToOverride": [],
            "preferredRole": None,
        },
        "claimsToAddOrUpdate": {
            "tenant_id": tenant_id,
            "role": role,
            "custom:tenant_id": tenant_id,
            "custom:role": role,
        },
        "claimsToSuppress": [],
    }

    return {
        "version": 1,
        "tokens": {
            "idToken": claims_and_scope_override_details,
            "accessToken": claims_and_scope_override_details,
        },
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point — dispatches to cognito_handler."""
    return cognito_handler(event, context)
