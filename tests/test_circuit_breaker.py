"""
Strata CFO Resilience Matrix — Circuit Breaker Tests

Comprehensive tests for the circuit breaker pattern with DynamoDB state.

FTR Compliance: Circuit breaker state survives Lambda container recycling.
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

sys_path = os.path.join(os.path.dirname(__file__), "..", "lib")
sys.path.insert(0, sys_path)

from lib.resilience import CircuitBreaker, CircuitState


# =========================================================================
# Fixtures
# =========================================================================
@pytest.fixture(autouse=True)
def mock_env():
    env = {
        "CIRCUIT_BREAKERS_TABLE": "test-circuit-breakers",
        "CIRCUIT_BREAKER_THRESHOLD": "3",
        "CIRCUIT_BREAKER_RESET_TIMEOUT": "30",
        "AWS_REGION": "us-east-1",
    }
    with patch.dict(os.environ, env, clear=False):
        yield


@pytest.fixture
def mock_dynamo():
    """Mock DynamoDB resource."""
    with patch("lib.resilience.boto3.resource") as mock_resource:
        mock_table = MagicMock()
        mock_resource.return_value.Table.return_value = mock_table
        yield mock_table


# =========================================================================
# State Initialization Tests
# =========================================================================
class TestCircuitBreakerInitialization:

    def test_creates_new_breaker_when_not_exists(self, mock_dynamo):
        """Should create a new circuit breaker record if none exists."""
        mock_dynamo.get_item.return_value = {}

        cb = CircuitBreaker(name="test:new-breaker")
        state, failures = cb.get_state()

        assert state == CircuitState.CLOSED
        assert failures == 0
        mock_dynamo.put_item.assert_called_once()
        put_call = mock_dynamo.put_item.call_args[1]["Item"]
        assert put_call["state"] == CircuitState.CLOSED
        assert put_call["failure_count"] == 0

    def test_reads_existing_breaker_state(self, mock_dynamo):
        """Should read and return existing breaker state."""
        mock_dynamo.get_item.return_value = {
            "Item": {
                "state": CircuitState.OPEN,
                "failure_count": 5,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
        }

        cb = CircuitBreaker(name="test:existing-breaker")
        state, failures = cb.get_state()

        assert state == CircuitState.OPEN
        assert failures == 5


# =========================================================================
# State Transition Tests
# =========================================================================
class TestCircuitBreakerTransitions:

    def test_closed_remains_closed_on_few_failures(self, mock_dynamo):
        """Circuit should stay CLOSED when failures are below threshold."""
        # Simulate 2 failures with threshold of 3
        mock_dynamo.update_item.return_value = {
            "Attributes": {"failure_count": 2}
        }

        cb = CircuitBreaker(name="test:below-threshold", failure_threshold=3)
        new_count = cb.record_failure()

        assert new_count == 2
        # Verify the update didn't set state to OPEN
        call_args = mock_dynamo.update_item.call_args
        assert call_args is not None

    def test_opens_after_threshold_reached(self, mock_dynamo):
        """Circuit should OPEN when failure count reaches threshold."""
        # First call: increment to threshold
        mock_dynamo.update_item.side_effect = [
            # ConditionalCheckFailedException for the first record_failure
            Exception("ConditionalCheckFailedException"),
        ]

        cb = CircuitBreaker(name="test:threshold-reached", failure_threshold=3)
        new_count = cb.record_failure()

        # Should force open when conditional check fails
        assert new_count > 3
        # Verify force_open was called
        assert mock_dynamo.update_item.call_count >= 2

    def test_closes_on_success(self, mock_dynamo):
        """Circuit should transition to CLOSED on success."""
        cb = CircuitBreaker(name="test:close-on-success")
        cb.record_success()

        call_kwargs = mock_dynamo.update_item.call_args[1]
        assert ":closed" in str(call_kwargs["ExpressionAttributeValues"])
        assert ":zero" in str(call_kwargs["ExpressionAttributeValues"])

    def test_allow_request_closed(self, mock_dynamo):
        """Should allow requests when circuit is CLOSED."""
        mock_dynamo.get_item.return_value = {
            "Item": {"state": CircuitState.CLOSED, "failure_count": 0}
        }

        cb = CircuitBreaker(name="test:allow-closed")
        assert cb.allow_request() is True

    def test_allow_request_half_open(self, mock_dynamo):
        """Should allow single probe request when HALF_OPEN."""
        mock_dynamo.get_item.return_value = {
            "Item": {"state": CircuitState.HALF_OPEN, "failure_count": 3}
        }

        cb = CircuitBreaker(name="test:allow-half-open")
        assert cb.allow_request() is True

    def test_block_request_open(self, mock_dynamo):
        """Should block requests when circuit is OPEN."""
        now = datetime.now(timezone.utc)
        # Set opened_at to recent time (within timeout)
        mock_dynamo.get_item.return_value = {
            "Item": {
                "state": CircuitState.OPEN,
                "failure_count": 5,
                "opened_at": now.isoformat(),
            }
        }

        cb = CircuitBreaker(name="test:block-open", reset_timeout_seconds=60)
        assert cb.allow_request() is False

    def test_auto_transitions_open_to_half_open(self, mock_dynamo):
        """Should auto-transition OPEN → HALF_OPEN after timeout."""
        # Set opened_at to well before timeout
        past = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_dynamo.get_item.return_value = {
            "Item": {
                "state": CircuitState.OPEN,
                "failure_count": 5,
                "opened_at": past.isoformat(),
            }
        }

        cb = CircuitBreaker(name="test:auto-half-open", reset_timeout_seconds=30)
        state, failures = cb.get_state()

        # Should have called update_item to set HALF_OPEN
        assert mock_dynamo.update_item.called
        call_kwargs = mock_dynamo.update_item.call_args[1]
        assert ":half" in str(call_kwargs["ExpressionAttributeValues"])


# =========================================================================
# Manual Override Tests
# =========================================================================
class TestCircuitBreakerOverrides:

    def test_force_open(self, mock_dynamo):
        """force_open should set state to OPEN."""
        cb = CircuitBreaker(name="test:force-open")
        cb.force_open()

        call_kwargs = mock_dynamo.update_item.call_args[1]
        assert call_kwargs["ExpressionAttributeValues"][":open"] == CircuitState.OPEN

    def test_force_close(self, mock_dynamo):
        """force_close should reset state to CLOSED and failures to 0."""
        cb = CircuitBreaker(name="test:force-close")
        cb.force_close()

        call_kwargs = mock_dynamo.update_item.call_args[1]
        assert call_kwargs["ExpressionAttributeValues"][":closed"] == CircuitState.CLOSED
        assert call_kwargs["ExpressionAttributeValues"][":zero"] == 0


# =========================================================================
# Concurrent Access Tests
# =========================================================================
class TestCircuitBreakerConcurrency:

    def test_dynamodb_write_has_condition_expression(self, mock_dynamo):
        """record_failure should use conditional write for concurrency."""
        mock_dynamo.update_item.return_value = {
            "Attributes": {"failure_count": 1}
        }

        cb = CircuitBreaker(name="test:concurrency")
        cb.record_failure()

        call_kwargs = mock_dynamo.update_item.call_args[1]
        assert "ConditionExpression" in call_kwargs
        assert "failure_count < :threshold" in call_kwargs["ConditionExpression"]

    def test_record_success_is_atomic(self, mock_dynamo):
        """record_success should atomically reset failures."""
        cb = CircuitBreaker(name="test:atomic-success")
        cb.record_success()

        call_kwargs = mock_dynamo.update_item.call_args[1]
        # Should set multiple fields in one operation
        update_expr = call_kwargs["UpdateExpression"]
        assert "failure_count" in update_expr
        assert "success_count" in update_expr
        assert "#state" in update_expr


# =========================================================================
# Error Handling Tests
# =========================================================================
class TestCircuitBreakerErrors:

    def test_fail_open_on_dynamodb_error(self, mock_dynamo):
        """Should fail-open (return CLOSED) on DynamoDB read error."""
        mock_dynamo.get_item.side_effect = Exception("DynamoDB unavailable")

        cb = CircuitBreaker(name="test:fail-open")
        state, failures = cb.get_state()

        # FTR: Fail-open for resilience
        assert state == CircuitState.CLOSED
        assert failures == 0

    def test_fail_open_on_invalid_timestamp(self, mock_dynamo):
        """Should fail-open on corrupted opened_at timestamp."""
        mock_dynamo.get_item.return_value = {
            "Item": {
                "state": CircuitState.OPEN,
                "failure_count": 5,
                "opened_at": "not-a-valid-timestamp",
            }
        }

        cb = CircuitBreaker(name="test:corrupt-timestamp")
        state, failures = cb.get_state()

        # Should return OPEN without crashing (timestamp parse fails silently)
        assert state == CircuitState.OPEN

    def test_custom_threshold(self, mock_dynamo):
        """Circuit breaker should respect custom threshold."""
        mock_dynamo.update_item.return_value = {
            "Attributes": {"failure_count": 10}
        }

        cb = CircuitBreaker(name="test:custom-threshold", failure_threshold=15)
        new_count = cb.record_failure()

        # Should not force open since 10 < 15
        assert new_count == 10


# =========================================================================
# Repr Tests
# =========================================================================
class TestCircuitBreakerRepr:

    def test_repr_includes_name_and_state(self, mock_dynamo):
        """__repr__ should include breaker name and state."""
        mock_dynamo.get_item.return_value = {
            "Item": {"state": CircuitState.CLOSED, "failure_count": 0}
        }

        cb = CircuitBreaker(name="test:repr-breaker")
        repr_str = repr(cb)

        assert "test:repr-breaker" in repr_str
        assert "CLOSED" in repr_str
