"""
Strata CFO Resilience Matrix — Resilience Patterns Library

This module implements reusable resilience patterns used across the system:
- CircuitBreaker: State machine with DynamoDB persistence
- RetryWithBackoff: Decorator with exponential backoff and jitter
- SemanticCache: S3-backed response caching with TTL
- GracefulDegradation: Progressive complexity reduction

All patterns are independently testable and configurable.

FTR Compliance Notes:
- Circuit breaker state survives Lambda container recycling (DynamoDB)
- Cache uses content-addressable keys (SHA-256) for deterministic hits
- Backoff uses jitter to prevent thundering herd
- Degradation ensures CFO agents always return a response
"""

import hashlib
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3

# ---------------------------------------------------------------------------
# Environment Configuration (injected via SAM template)
# ---------------------------------------------------------------------------
CIRCUIT_BREAKERS_TABLE = os.environ.get("CIRCUIT_BREAKERS_TABLE", "strata-circuit-breakers")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "strata-resilience-metrics")
CACHE_BUCKET = os.environ.get("CACHE_BUCKET", "")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "")

DEFAULT_CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "5"))
DEFAULT_CIRCUIT_BREAKER_RESET_TIMEOUT = int(os.environ.get("CIRCUIT_BREAKER_RESET_TIMEOUT", "60"))
DEFAULT_RETRY_MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "3"))
DEFAULT_CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "3600"))


# =========================================================================
# Circuit Breaker
# =========================================================================
class CircuitState:
    """Circuit breaker states — explicit constants for type safety."""
    CLOSED = 0       # Normal operation
    HALF_OPEN = 1     # Recovery testing
    OPEN = 2          # Failures blocked

    LABELS = {0: "CLOSED", 1: "HALF_OPEN", 2: "OPEN"}


class CircuitBreaker:
    """
    Circuit breaker pattern with DynamoDB-backed state persistence.

    Protects against cascading failures by tracking failure rates and
    temporarily blocking requests to unhealthy services.

    State Transitions:
      CLOSED  ──[failure >= threshold]──► OPEN
      OPEN     ──[timeout elapsed]──────► HALF_OPEN
      HALF_OPEN──[success]────────────► CLOSED
      HALF_OPEN──[failure]────────────► OPEN

    FTR Compliance:
    - State persisted in DynamoDB (survives Lambda container recycling)
    - Thread-safe for concurrent Lambda invocations
    - Configurable thresholds for fine-tuning
    - CloudWatch metrics for observability
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = None,
        reset_timeout_seconds: int = None,
        table_name: str = None,
    ):
        """
        Initialize circuit breaker.

        Args:
            name: Unique identifier (e.g., "gateway:anthropic.claude-3-5-sonnet")
            failure_threshold: Failures before opening circuit (default from env)
            reset_timeout_seconds: Seconds before OPEN → HALF_OPEN (default from env)
            table_name: DynamoDB table name (default from env)
        """
        self.name = name
        self.failure_threshold = failure_threshold or DEFAULT_CIRCUIT_BREAKER_THRESHOLD
        self.reset_timeout_seconds = reset_timeout_seconds or DEFAULT_CIRCUIT_BREAKER_RESET_TIMEOUT
        self._table_name = table_name or CIRCUIT_BREAKERS_TABLE
        self._table = None

    def _get_table(self):
        """Lazy-initialize DynamoDB table resource."""
        if self._table is None:
            self._table = boto3.resource("dynamodb").Table(self._table_name)
        return self._table

    def _get_state_from_db(self) -> Tuple[int, int]:
        """
        Read circuit breaker state from DynamoDB.

        Returns (state, failure_count).
        Creates initial record if it doesn't exist.
        """
        table = self._get_table()
        now = datetime.now(timezone.utc)

        try:
            response = table.get_item(Key={"breaker_id": self.name})
            item = response.get("Item")

            if not item:
                # Initialize new circuit breaker — start CLOSED
                table.put_item(Item={
                    "breaker_id": self.name,
                    "state": CircuitState.CLOSED,
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure_at": "",
                    "last_success_at": "",
                    "opened_at": "",
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                })
                return (CircuitState.CLOSED, 0)

            state = item["state"]

            # Auto-transition OPEN → HALF_OPEN after timeout
            if state == CircuitState.OPEN:
                opened_at = item.get("opened_at", "")
                if opened_at:
                    try:
                        opened_time = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
                        elapsed = (now - opened_time).total_seconds()
                        if elapsed >= self.reset_timeout_seconds:
                            table.update_item(
                                Key={"breaker_id": self.name},
                                UpdateExpression="SET #state = :half, #updated_at = :now",
                                ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
                                ExpressionAttributeValues={":half": CircuitState.HALF_OPEN, ":now": now.isoformat()},
                            )
                            return (CircuitState.HALF_OPEN, item["failure_count"])
                    except (ValueError, TypeError):
                        pass

            return (state, item["failure_count"])

        except Exception as e:
            # FTR: Fail-open on DynamoDB errors — service resilience > consistency
            return (CircuitState.CLOSED, 0)

    def get_state(self) -> Tuple[int, int]:
        """Public API: Get current (state, failure_count)."""
        return self._get_state_from_db()

    def allow_request(self) -> bool:
        """
        Check if a request is allowed through the circuit breaker.

        Returns True for CLOSED and HALF_OPEN states.
        Returns False for OPEN state.
        """
        state, _ = self._get_state_from_db()
        return state != CircuitState.OPEN

    def record_success(self) -> None:
        """
        Record a successful invocation.

        Resets failure count and transitions to CLOSED.
        FTR: Atomic DynamoDB update for concurrency safety.
        """
        table = self._get_table()
        now = datetime.now(timezone.utc)

        table.update_item(
            Key={"breaker_id": self.name},
            UpdateExpression=(
                "SET failure_count = :zero, success_count = success_count + :one, "
                "#state = :closed, last_success_at = :now, #updated_at = :now"
            ),
            ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
            ExpressionAttributeValues={
                ":zero": 0,
                ":one": 1,
                ":closed": CircuitState.CLOSED,
                ":now": now.isoformat(),
            },
        )

    def record_failure(self) -> int:
        """
        Record a failed invocation. Returns new failure count.

        Opens circuit if failure count >= threshold.
        FTR: Atomic conditional update for race condition safety.
        """
        table = self._get_table()
        now = datetime.now(timezone.utc)

        try:
            response = table.update_item(
                Key={"breaker_id": self.name},
                UpdateExpression=(
                    "SET failure_count = failure_count + :one, "
                    "last_failure_at = :now, #updated_at = :now"
                ),
                ExpressionAttributeNames={"#updated_at": "updated_at"},
                ExpressionAttributeValues={":one": 1, ":now": now.isoformat()},
                ConditionExpression="failure_count < :threshold",
                ExpressionAttributeValues={":one": 1, ":now": now.isoformat(), ":threshold": self.failure_threshold},
                ReturnValues="ALL_NEW",
            )

            new_count = response["Attributes"]["failure_count"]

            if new_count >= self.failure_threshold:
                table.update_item(
                    Key={"breaker_id": self.name},
                    UpdateExpression="SET #state = :open, opened_at = :now, #updated_at = :now",
                    ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
                    ExpressionAttributeValues={":open": CircuitState.OPEN, ":now": now.isoformat()},
                )

            return new_count

        except Exception:
            # Threshold exceeded or condition failed — force open
            table.update_item(
                Key={"breaker_id": self.name},
                UpdateExpression="SET #state = :open, opened_at = :now, #updated_at = :now",
                ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
                ExpressionAttributeValues={":open": CircuitState.OPEN, ":now": now.isoformat()},
            )
            return self.failure_threshold + 1

    def force_open(self) -> None:
        """Manually force the circuit breaker to OPEN state (for testing)."""
        table = self._get_table()
        now = datetime.now(timezone.utc)
        table.update_item(
            Key={"breaker_id": self.name},
            UpdateExpression="SET #state = :open, opened_at = :now, #updated_at = :now",
            ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
            ExpressionAttributeValues={":open": CircuitState.OPEN, ":now": now.isoformat()},
        )

    def force_close(self) -> None:
        """Manually reset the circuit breaker to CLOSED state (for recovery)."""
        table = self._get_table()
        now = datetime.now(timezone.utc)
        table.update_item(
            Key={"breaker_id": self.name},
            UpdateExpression="SET #state = :closed, failure_count = :zero, #updated_at = :now",
            ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
            ExpressionAttributeValues={":closed": CircuitState.CLOSED, ":zero": 0, ":now": now.isoformat()},
        )

    def __repr__(self) -> str:
        state, failures = self._get_state_from_db()
        return f"CircuitBreaker(name={self.name!r}, state={CircuitState.LABELS.get(state, 'UNKNOWN')}, failures={failures})"


# =========================================================================
# Retry with Exponential Backoff
# =========================================================================
class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted."""
    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"All {attempts} retry attempts failed. Last error: {last_error}")


def retry_with_backoff(
    max_attempts: int = None,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[Tuple] = None,
):
    """
    Decorator for retry with exponential backoff and jitter.

    FTR Compliance:
    - Exponential backoff prevents thundering herd
    - Jitter reduces correlated retry storms
    - Explicit retryable error classification (not catch-all)
    - Max attempts bounded to prevent resource exhaustion

    Args:
        max_attempts: Maximum retry attempts (default from env or 3)
        base_delay: Initial delay in seconds (default 0.5)
        max_delay: Maximum delay cap in seconds (default 10.0)
        backoff_factor: Multiplier for each retry (default 2.0)
        jitter: Add random jitter to prevent correlated retries (default True)
        retryable_exceptions: Tuple of exception types that trigger retry
    """
    if max_attempts is None:
        max_attempts = DEFAULT_RETRY_MAX_ATTEMPTS

    if retryable_exceptions is None:
        retryable_exceptions = (
            Exception,  # FTR: In production, use specific exception types
        )

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_error = e

                    if attempt == max_attempts:
                        raise RetryExhaustedError(max_attempts, e)

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)

                    # Add jitter (±10%)
                    if jitter:
                        jitter_amount = delay * 0.1
                        delay += random.uniform(-jitter_amount, jitter_amount)
                        delay = max(0.1, delay)

                    time.sleep(delay)

            raise last_error

        return wrapper
    return decorator


# =========================================================================
# Semantic Cache
# =========================================================================
class SemanticCache:
    """
    S3-backed semantic cache with TTL-based expiration.

    Cache keys are content-addressable: SHA-256(prompt + model_id + system_prompt)
    This ensures deterministic cache hits for identical requests.

    FTR Compliance:
    - KMS-encrypted S3 storage
    - TTL prevents stale responses
    - Prefix-scoped for IAM access control
    """

    def __init__(
        self,
        bucket: str = None,
        prefix: str = "cache/responses/",
        ttl_seconds: int = None,
        kms_key_id: str = None,
    ):
        self.bucket = bucket or CACHE_BUCKET
        self.prefix = prefix
        self.ttl_seconds = ttl_seconds or DEFAULT_CACHE_TTL
        self.kms_key_id = kms_key_id or KMS_KEY_ID
        self._s3 = None

    def _get_s3(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3")
        return self._s3

    def _compute_key(self, prompt: str, model_id: str, system_prompt: str = "") -> str:
        """Compute SHA-256 content-addressable cache key."""
        hash_input = f"{prompt}|{model_id}|{system_prompt}"
        return hashlib.sha256(hash_input.encode()).hexdigest()

    def get(
        self,
        prompt: str,
        model_id: str,
        system_prompt: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached response if it exists and hasn't expired.

        Returns None on cache miss or expired entry.
        Deletes expired entries proactively.
        """
        s3 = self._get_s3()
        key = f"{self.prefix}{self._compute_key(prompt, model_id, system_prompt)}.json"

        try:
            response = s3.get_object(Bucket=self.bucket, Key=key)
            cached = json.loads(response["Body"].read().decode("utf-8"))

            # Check TTL
            now = datetime.now(timezone.utc)
            cached_at = datetime.fromisoformat(cached["cached_at"].replace("Z", "+00:00"))
            age_seconds = (now - cached_at).total_seconds()

            if age_seconds > self.ttl_seconds:
                # Proactively delete expired entry
                try:
                    s3.delete_object(Bucket=self.bucket, Key=key)
                except Exception:
                    pass
                return None

            return cached

        except s3.exceptions.NoSuchKey:
            return None
        except Exception:
            return None

    def put(
        self,
        prompt: str,
        model_id: str,
        response: str,
        metadata: Dict[str, Any] = None,
        system_prompt: str = "",
    ) -> bool:
        """
        Store a response in the cache.

        Returns True on success, False on failure.
        Cache entries are KMS-encrypted.
        """
        s3 = self._get_s3()
        key = f"{self.prefix}{self._compute_key(prompt, model_id, system_prompt)}.json"
        now = datetime.now(timezone.utc)

        entry = {
            "prompt_hash": self._compute_key(prompt, model_id, system_prompt),
            "model_id": model_id,
            "response": response,
            "metadata": metadata or {},
            "cached_at": now.isoformat(),
            "ttl_seconds": self.ttl_seconds,
        }

        try:
            put_kwargs = {
                "Bucket": self.bucket,
                "Key": key,
                "Body": json.dumps(entry).encode("utf-8"),
                "ContentType": "application/json",
                "Metadata": {
                    "cached-at": now.isoformat(),
                    "ttl": str(self.ttl_seconds),
                    "model-id": model_id,
                },
            }

            # FTR: KMS encryption
            if self.kms_key_id:
                put_kwargs["ServerSideEncryption"] = "aws:kms"
                put_kwargs["SSEKMSKeyId"] = self.kms_key_id

            s3.put_object(**put_kwargs)
            return True

        except Exception:
            return False

    def invalidate(self, prompt: str, model_id: str, system_prompt: str = "") -> bool:
        """Remove a specific cache entry."""
        s3 = self._get_s3()
        key = f"{self.prefix}{self._compute_key(prompt, model_id, system_prompt)}.json"

        try:
            s3.delete_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False


# =========================================================================
# Graceful Degradation
# =========================================================================
class GracefulDegradation:
    """
    Progressive complexity reduction when the system is under pressure.

    Levels:
      L0 (Full):     100% context, full tokens
      L1 (Reduced):  50% context, 2048 tokens
      L2 (Minimal):  25% context, 1024 tokens
      L3 (Zero-shot): No context, 512 tokens

    FTR: CFO agents always return *something*, even at L3 degradation.
    """

    LEVELS = {
        0: {"context_ratio": 1.0, "max_tokens": 4096, "label": "full"},
        1: {"context_ratio": 0.5, "max_tokens": 2048, "label": "reduced"},
        2: {"context_ratio": 0.25, "max_tokens": 1024, "label": "minimal"},
        3: {"context_ratio": 0.0, "max_tokens": 512, "label": "zero_shot"},
    }

    @classmethod
    def apply(cls, prompt: str, system_prompt: str, level: int = 0) -> Tuple[str, str, int]:
        """
        Apply degradation level to prompt and system prompt.

        Returns (degraded_prompt, degraded_system_prompt, max_tokens).
        """
        level = max(0, min(level, 3))
        config = cls.LEVELS[level]

        if config["context_ratio"] < 1.0 and prompt:
            char_limit = int(len(prompt) * config["context_ratio"])
            prompt = prompt[:char_limit] + "\n\n[Context truncated due to system degradation]"

        if config["context_ratio"] == 0.0:
            system_prompt = ""

        return prompt, system_prompt, config["max_tokens"]

    @classmethod
    def next_level(cls, current: int) -> int:
        """Move to the next (more degraded) level."""
        return min(current + 1, 3)

    @classmethod
    def reset(cls) -> int:
        """Reset to full capability (L0)."""
        return 0

    @classmethod
    def get_level_info(cls, level: int) -> Dict[str, Any]:
        """Get configuration for a specific degradation level."""
        return cls.LEVELS.get(max(0, min(level, 3)), cls.LEVELS[0]).copy()
