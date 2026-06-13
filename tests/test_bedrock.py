"""
Strata CFO Resilience Matrix — Bedrock Client Tests

Tests for the shared Bedrock client library with mocked AWS services.

FTR Compliance: All Bedrock invocations use explicit model ARNs.
"""

import json
import os
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

sys_path = os.path.join(os.path.dirname(__file__), "..", "lib")
import sys
sys.path.insert(0, sys_path)

from lib.bedrock import (
    BedrockClient, BedrockRequest, BedrockResponse,
    ModelFamily,
)


# =========================================================================
# Fixtures
# =========================================================================
@pytest.fixture
def mock_boto3():
    """Mock boto3 clients."""
    with patch("lib.bedrock.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        yield mock_client, mock_boto3


@pytest.fixture
def bedrock_client(mock_boto3):
    """Create a BedrockClient with mocked boto3."""
    _, _ = mock_boto3  # Ensure patching
    client = BedrockClient(region_name="us-east-1")
    return client


@pytest.fixture
def claude_request():
    """Standard Claude request for testing."""
    return BedrockRequest(
        model_id="anthic.claude-3-5-sonnet-20241022-v1:0",
        prompt="Analyze Q3 cash flow",
        system_prompt="You are a CFO assistant",
        max_tokens=1024,
        temperature=0.7,
    )


@pytest.fixture
def claude_response_data():
    """Standard Claude response data."""
    return {
        "content": [{"text": "Based on Q3 data, cash flow improved by 15%."}],
        "usage": {"input_tokens": 100, "output_tokens": 50},
        "stop_reason": "end_turn",
    }


# =========================================================================
# Model Family Detection Tests
# =========================================================================
class TestModelFamily:
    """Tests for model family detection from model ID."""

    def test_detect_anthropic_claude(self):
        assert ModelFamily.from_model_id("anthic.claude-3-5-sonnet-20241022-v1:0") == ModelFamily.ANTHROPIC_CLAUDE
        assert ModelFamily.from_model_id("anthropic.claude-3-sonnet-20240229-v1:0") == ModelFamily.ANTHROPIC_CLAUDE

    def test_detect_amazon_titan(self):
        assert ModelFamily.from_model_id("amazon.titan-text-premier-v1:0") == ModelFamily.AMAZON_TITAN
        assert ModelFamily.from_model_id("amazon.titan-text-express-v1:0") == ModelFamily.AMAZON_TITAN

    def test_detect_meta_llama(self):
        assert ModelFamily.from_model_id("meta.llama3-70b-instruct-v1:0") == ModelFamily.META_LLAMA
        assert ModelFamily.from_model_id("meta.llama3-8b-instruct-v1:0") == ModelFamily.META_LLAMA

    def test_detect_unknown(self):
        assert ModelFamily.from_model_id("unknown.model.v1") == ModelFamily.UNKNOWN


# =========================================================================
# BedrockRequest Tests
# =========================================================================
class TestBedrockRequest:
    """Tests for request validation."""

    def test_valid_request(self):
        req = BedrockRequest(model_id="anthic.claude-3-5-sonnet-20241022-v1:0", prompt="test")
        assert req.prompt == "test"
        assert req.request_id is not None
        assert len(req.request_id) > 0

    def test_empty_prompt_raises(self):
        with pytest.raises(ValueError, match="prompt is required"):
            BedrockRequest(model_id="test", prompt="")

    def test_max_tokens_validation(self):
        with pytest.raises(ValueError, match="max_tokens"):
            BedrockRequest(model_id="test", prompt="test", max_tokens=99999)

    def test_temperature_validation(self):
        with pytest.raises(ValueError, match="temperature"):
            BedrockRequest(model_id="test", prompt="test", temperature=3.0)


# =========================================================================
# BedrockClient Invoke Tests
# =========================================================================
class TestBedrockClientInvoke:
    """Tests for Bedrock model invocation."""

    def test_invoke_claude_success(self, bedrock_client, claude_request, claude_response_data, mock_boto3):
        """Successful Claude invocation should return structured response."""
        mock_client = mock_boto3[0]

        # Mock the invoke_model response
        mock_response = MagicMock()
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps(claude_response_data).encode("utf-8")
        mock_response["Body"] = mock_response_body
        mock_client.invoke_model.return_value = mock_response

        response = bedrock_client.invoke(claude_request)

        assert response.success is True
        assert response.response_text == "Based on Q3 data, cash flow improved by 15%."
        assert response.input_tokens == 100
        assert response.output_tokens == 50
        assert response.model_family == ModelFamily.ANTHROPIC_CLAUDE

    def test_invoke_titan_success(self, mock_boto3):
        """Successful Titan invocation."""
        mock_client = mock_boto3[0]
        client = BedrockClient(region_name="us-east-1")

        titan_response = {
            "results": [{"outputText": "Titan response"}],
            "inputTokenCount": 50,
            "outputTokenCount": 25,
        }
        mock_response = MagicMock()
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps(titan_response).encode("utf-8")
        mock_response["Body"] = mock_response_body
        mock_client.invoke_model.return_value = mock_response

        req = BedrockRequest(model_id="amazon.titan-text-premier-v1:0", prompt="test")
        response = client.invoke(req)

        assert response.success is True
        assert response.model_family == ModelFamily.AMAZON_TITAN

    def test_invoke_error_returns_error_response(self, bedrock_client, claude_request, mock_boto3):
        """Failed invocation should return error response, not raise."""
        mock_client = mock_boto3[0]
        mock_client.invoke_model.side_effect = Exception("ServiceUnavailable")

        response = bedrock_client.invoke(claude_request)

        assert response.success is False
        assert response.error is not None
        assert "ServiceUnavailable" in response.error
        assert response.input_tokens == 0
        assert response.output_tokens == 0

    def test_invocation_logging(self, bedrock_client, claude_request, claude_response_data, mock_boto3):
        """Each invocation should be logged."""
        mock_client = mock_boto3[0]
        mock_response = MagicMock()
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps(claude_response_data).encode("utf-8")
        mock_response["Body"] = mock_response_body
        mock_client.invoke_model.return_value = mock_response

        bedrock_client.invoke(claude_request)

        log = bedrock_client.get_invocation_log()
        assert len(log) == 1
        assert log[0]["success"] is True


# =========================================================================
# Cost Estimation Tests
# =========================================================================
class TestCostEstimation:
    """Tests for cost estimation."""

    def test_claude_cost(self):
        client = BedrockClient(region_name="us-east-1")
        cost = client.estimate_cost("anthropic.claude-3-5-sonnet-20241022-v1:0", 1000, 500)
        # input: 1K * $0.003 = $0.003, output: 0.5K * $0.015 = $0.0075
        assert abs(cost - 0.0105) < 0.001

    def test_titan_cost(self):
        client = BedrockClient(region_name="us-east-1")
        cost = client.estimate_cost("amazon.titan-text-premier-v1:0", 1000, 500)
        # input: 1K * $0.0008 = $0.0008, output: 0.5K * $0.0016 = $0.0008
        assert abs(cost - 0.0016) < 0.0001

    def test_total_cost_tracking(self, bedrock_client, claude_request, claude_response_data, mock_boto3):
        """Total cost should accumulate across invocations."""
        mock_client = mock_boto3[0]
        mock_response = MagicMock()
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps(claude_response_data).encode("utf-8")
        mock_response["Body"] = mock_response_body
        mock_client.invoke_model.return_value = mock_response

        bedrock_client.invoke(claude_request)
        bedrock_client.invoke(claude_request)

        total_cost = bedrock_client.get_total_cost()
        assert total_cost > 0

    def test_average_latency(self, bedrock_client, claude_request, claude_response_data, mock_boto3):
        """Average latency should be computed correctly."""
        mock_client = mock_boto3[0]
        mock_response = MagicMock()
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = json.dumps(claude_response_data).encode("utf-8")
        mock_response["Body"] = mock_response_body
        mock_client.invoke_model.return_value = mock_response

        bedrock_client.invoke(claude_request)

        avg_lat = bedrock_client.get_average_latency()
        assert avg_lat >= 0


# =========================================================================
# Token Estimation Tests
# =========================================================================
class TestTokenEstimation:
    """Tests for token estimation."""

    def test_empty_string(self):
        client = BedrockClient(region_name="us-east-1")
        assert client.estimate_tokens("") == 0

    def test_short_text(self):
        client = BedrockClient(region_name="us-east-1")
        tokens = client.estimate_tokens("Hello world")
        assert tokens >= 1

    def test_longer_text(self):
        client = BedrockClient(region_name="us-east-1")
        text = "word " * 400
        tokens = client.estimate_tokens(text)
        assert tokens >= 50  # ~200 chars / 4 per token
