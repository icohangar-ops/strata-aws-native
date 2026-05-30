"""
Strata CFO Resilience Matrix — Observability Library

This module provides CloudWatch + X-Ray observability helpers:
- Structured JSON logging for CloudWatch Insights
- Custom metrics emission to CloudWatch
- X-Ray subsegments, annotations, and metadata

FTR Compliance Notes:
- All log entries are valid JSON for CloudWatch Insights queries
- Custom metrics use the StrataCFO namespace for consistent dashboards
- X-Ray provides distributed tracing across all 6 Lambda layers
- Metrics dimensions enable drill-down by agent type, model, tenant
"""

import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

import boto3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
SERVICE_NAME = os.environ.get("POWERTOOLS_SERVICE_NAME", "strata")
METRICS_NAMESPACE = os.environ.get("POWERTOOLS_METRICS_NAMESPACE", "StrataCFO")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "unknown")
AWS_REQUEST_ID = os.environ.get("AWS_REQUEST_ID", "")


# =========================================================================
# Structured JSON Logger
# =========================================================================
class StructuredLogger:
    """
    JSON-structured logger for CloudWatch Insights compatibility.

    All log entries are valid JSON objects with consistent fields:
    - timestamp (ISO 8601)
    - level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - service (Lambda function name)
    - environment (deployment environment)
    - message (human-readable description)
    - correlation_id (request tracking)
    - [optional] error, metrics, dimensions

    FTR Compliance:
    - Valid JSON for CloudWatch Insights querying
    - Consistent field naming for efficient filter patterns
    - Structured error objects for automated alerting
    """

    _LEVEL_MAP = {
        "DEBUG": 10,
        "INFO": 20,
        "WARNING": 30,
        "ERROR": 40,
        "CRITICAL": 50,
    }

    def __init__(self, service: str = None, log_level: str = None):
        self.service = service or SERVICE_NAME
        self.log_level = (log_level or LOG_LEVEL).upper()
        self._threshold = self._LEVEL_MAP.get(self.log_level, 20)
        self._correlation_id: Optional[str] = None

    def set_correlation_id(self, correlation_id: str) -> None:
        """Set the correlation ID for all subsequent log entries."""
        self._correlation_id = correlation_id

    def _should_log(self, level: str) -> bool:
        return self._LEVEL_MAP.get(level, 0) >= self._threshold

    def _emit(self, level: str, message: str, **extra_fields) -> None:
        """Emit a structured JSON log entry."""
        if not self._should_log(level):
            return

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "service": self.service,
            "environment": ENVIRONMENT,
            "message": message,
        }

        if self._correlation_id:
            entry["correlation_id"] = self._correlation_id
        if AWS_REQUEST_ID:
            entry["aws_request_id"] = AWS_REQUEST_ID
        if extra_fields:
            entry.update(extra_fields)

        print(json.dumps(entry, default=str))

    def debug(self, message: str, **kwargs) -> None:
        self._emit("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._emit("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._emit("WARNING", message, **kwargs)

    def error(self, message: str, error: Any = None, **kwargs) -> None:
        """Log an error with optional structured error object."""
        if error:
            if isinstance(error, Exception):
                error_obj = {
                    "type": type(error).__name__,
                    "message": str(error)[:1000],
                }
                kwargs["error"] = error_obj
            elif isinstance(error, dict):
                kwargs["error"] = error
        self._emit("ERROR", message, **kwargs)

    def critical(self, message: str, error: Any = None, **kwargs) -> None:
        self.error(message, error=error, **kwargs)
        self._emit("CRITICAL", message, **kwargs)


# =========================================================================
# CloudWatch Custom Metrics
# =========================================================================
class CloudWatchMetrics:
    """
    CloudWatch custom metrics emitter with buffering.

    Buffers metrics and flushes in batches to minimize PutMetricData API calls.
    All metrics use the StrataCFO namespace and standard dimensions.

    FTR Compliance:
    - Consistent namespace for dashboard aggregation
    - Standard dimensions for drill-down (environment, agent_type, model)
    - Buffered emission reduces API costs
    - Never fails the main function (best-effort metrics)
    """

    MAX_BUFFER_SIZE = 20  # CloudWatch PutMetricData limit per call
    MAX_METRIC_VALUES = 150  # Total metric data points per PutMetricData call

    def __init__(
        self,
        namespace: str = None,
        region: str = None,
        default_dimensions: Optional[Dict[str, str]] = None,
    ):
        self.namespace = namespace or METRICS_NAMESPACE
        self._region = region or os.environ.get("AWS_REGION", "us-east-1")
        self._client = None
        self._buffer: List[Dict[str, Any]] = []
        self._default_dimensions = {
            "Environment": ENVIRONMENT,
            **(default_dimensions or {}),
        }

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("cloudwatch", region_name=self._region)
        return self._client

    def put(
        self,
        name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Add a metric to the buffer.

        Args:
            name: Metric name (e.g., "GatewayLatency")
            value: Numeric value
            unit: CloudWatch unit (Count, Milliseconds, Percent, None)
            dimensions: Additional dimensions (merged with defaults)
            timestamp: Optional timestamp (default: now)
        """
        metric_data = {
            "MetricName": name,
            "Value": value,
            "Unit": unit,
        }

        if timestamp:
            metric_data["Timestamp"] = timestamp.isoformat()

        # Merge dimensions: defaults + overrides
        all_dimensions = {**self._default_dimensions}
        if dimensions:
            all_dimensions.update(dimensions)

        metric_data["Dimensions"] = [
            {"Name": k, "Value": v} for k, v in all_dimensions.items()
        ]

        self._buffer.append(metric_data)

        if len(self._buffer) >= self.MAX_BUFFER_SIZE:
            self.flush()

    def put_count(self, name: str, value: int = 1, **kwargs) -> None:
        """Convenience: emit a Count metric."""
        self.put(name, float(value), unit="Count", **kwargs)

    def put_latency(self, name: str, value_ms: float, **kwargs) -> None:
        """Convenience: emit a Milliseconds metric."""
        self.put(name, value_ms, unit="Milliseconds", **kwargs)

    def put_percent(self, name: str, value: float, **kwargs) -> None:
        """Convenience: emit a Percent metric."""
        self.put(name, value, unit="Percent", **kwargs)

    def put_gauge(self, name: str, value: float, **kwargs) -> None:
        """Convenience: emit a None (gauge) metric."""
        self.put(name, value, unit="None", **kwargs)

    def flush(self) -> None:
        """Flush buffered metrics to CloudWatch."""
        if not self._buffer:
            return

        try:
            # FTR: Batch into chunks of MAX_METRIC_VALUES
            for i in range(0, len(self._buffer), self.MAX_METRIC_VALUES):
                chunk = self._buffer[i:i + self.MAX_METRIC_VALUES]
                self.client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=chunk,
                )
            self._buffer = []
        except Exception as e:
            # FTR: Never crash on metrics emission failure
            self._buffer = []
            pass

    def __del__(self):
        """Flush on destruction."""
        self.flush()


# =========================================================================
# X-Ray Tracing Helpers
# =========================================================================
class XRayHelper:
    """
    Helper utilities for X-Ray distributed tracing.

    Provides:
    - Subsegment creation and management
    - Annotations (indexed, filterable)
    - Metadata (not indexed, detailed context)
    - Exception recording for error tracking

    FTR Compliance:
    - X-Ray provides end-to-end tracing across all 6 Lambda layers
    - Subsegments show per-layer timing in the X-Ray console
    - Annotations enable filtering traces in service map
    """

    @staticmethod
    def put_annotation(key: str, value: Any) -> None:
        """Add an indexed annotation to the current segment/subsegment."""
        try:
            from aws_xray_sdk.core import xray_recorder
            segment = xray_recorder.current_segment()
            if segment:
                segment.put_annotation(key, value)
        except ImportError:
            pass
        except Exception:
            pass

    @staticmethod
    def put_metadata(key: str, value: Any, namespace: str = "strata") -> None:
        """Add unindexed metadata to the current segment/subsegment."""
        try:
            from aws_xray_sdk.core import xray_recorder
            segment = xray_recorder.current_segment()
            if segment:
                segment.put_metadata(key, value, namespace)
        except ImportError:
            pass
        except Exception:
            pass

    @staticmethod
    def record_error(error: Exception, cause: Optional[Exception] = None) -> None:
        """Record an exception in the X-Ray trace."""
        try:
            from aws_xray_sdk.core import xray_recorder
            segment = xray_recorder.current_segment()
            if segment:
                segment.add_exception_flag()
                segment.put_annotation("error", True)
                segment.put_annotation("error_type", type(error).__name__)
                if cause:
                    segment.put_annotation("cause_type", type(cause).__name__)
        except ImportError:
            pass
        except Exception:
            pass

    @staticmethod
    @contextmanager
    def subsegment(name: str, namespace: str = "strata") -> Generator[None, None, None]:
        """Context manager for creating an X-Ray subsegment."""
        subseg = None
        try:
            from aws_xray_sdk.core import xray_recorder
            subseg = xray_recorder.begin_subsegment(name)
            if subseg:
                subseg.set_namespace(namespace)
            yield
        except ImportError:
            yield
        except Exception:
            pass
        finally:
            if subseg:
                try:
                    from aws_xray_sdk.core import xray_recorder
                    xray_recorder.end_subsegment()
                except Exception:
                    pass


# =========================================================================
# Convenience: Global instances
# =========================================================================
# These are initialized once and reused across the Lambda invocation
logger = StructuredLogger()
metrics = CloudWatchMetrics()
xray = XRayHelper()


# =========================================================================
# Invocation Timer
# =========================================================================
class InvocationTimer:
    """
    Context manager for timing Lambda invocations and emitting metrics.

    Usage:
        with InvocationTimer("gateway", agent_type="cash_flow"):
            # ... do work ...

    FTR: Ensures all Lambda functions report consistent timing metrics.
    """

    def __init__(self, operation: str, **dimensions):
        self.operation = operation
        self.dimensions = dimensions
        self.start_time = time.monotonic()
        self.end_time: Optional[float] = None

    def __enter__(self):
        self.start_time = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.monotonic()
        elapsed_ms = (self.end_time - self.start_time) * 1000

        metrics.put_latency(
            f"{self.operation}Latency",
            elapsed_ms,
            dimensions=self.dimensions,
        )
        metrics.put_count(
            f"{self.operation}Invocations",
            dimensions=self.dimensions,
        )

        if exc_type is not None:
            metrics.put_count(
                f"{self.operation}Errors",
                dimensions=self.dimensions,
            )

        return False
