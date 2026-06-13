"""
Strata CFO Resilience Matrix — Shared Bedrock Client Library

This module provides a production-grade wrapper around boto3 Bedrock Runtime
with multi-model support, guardrails integration, token counting, and
invocation logging.

FTR Compliance Notes:
- All model invocations use explicit model ARNs (no wildcards)
- Token counting for cost governance
- Guardrails integration for content safety
- Invocation logging for audit trail
- Multi-model abstraction for seamless fallback
"""

import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import boto3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_MAX_TOKENS = 4096
DEFAULT_TEMPERATURE = 0.7
DEFAULT_TOP_P = 0.9
DEFAULT_TIMEOUT_MS = 30000

# Approximate cost per 1K tokens (USD) — updated quarterly
# FTR: Cost tracking for budget governance
MODEL_COSTS = {
    "anthropic.claude-3-5-sonnet-20241022-v1:0": {"input_per_1k": 0.003, "output_per_1k": 0.015},
    "anthropic.claude-3-sonnet-20240229-v1:0": {"input_per_1k": 0.003, "output_per_1k": 0.015},
    "amazon.titan-text-premier-v1:0": {"input_per_1k": 0.0008, "output_per_1k": 0.0016},
    "amazon.titan-text-express-v1:0": {"input_per_1k": 0.0004, "output_per_1k": 0.0008},
    "meta.llama3-70b-instruct-v1:0": {"input_per_1k": 0.00265, "output_per_1k": 0.0035},
    "meta.llama3-8b-instruct-v1:0": {"input_per_1k": 0.0006, "output_per_1k": 0.0009},
}


class ModelFamily(Enum):
    """Supported Bedrock model families."""
    ANTHROPIC_CLAUDE = "anthropic"
    AMAZON_TITAN = "amazon"
    META_LLAMA = "meta"
    AI21_JURASSIC = "ai21"
    COHERE_COMMAND = "cohere"
    MISTRAL = "mistral"
    UNKNOWN = "unknown"

    @classmethod
    def from_model_id(cls, model_id: str) -> "ModelFamily":
        """Detect model family from model ID string."""
        model_lower = model_id.lower()
        if "claude" in model_lower or "anthropic" in model_lower:
            return cls.ANTHROPIC_CLAUDE
        elif "titan" in model_lower or "amazon" in model_lower:
            return cls.AMAZON_TITAN
        elif "llama" in model_lower or "meta" in model_lower:
            return cls.META_LLAMA
        elif "jamba" in model_lower or "ai21" in model_lower:
            return cls.AI21_JURASSIC
        elif "command" in model_lower or "cohere" in model_lower:
            return cls.COHERE_COMMAND
        elif "mistral" in model_lower or "mixtral" in model_lower:
            return cls.MISTRAL
        return cls.UNKNOWN


@dataclass
class BedrockRequest:
    """Structured Bedrock invocation request."""
    model_id: str
    prompt: str
    system_prompt: str = ""
    max_tokens: int = DEFAULT_MAX_TOKENS
    temperature: float = DEFAULT_TEMPERATURE
    top_p: float = DEFAULT_TOP_P
    stop_sequences: List[str] = field(default_factory=list)
    guardrail_id: Optional[str] = None
    guardrail_version: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self):
        """Validate request parameters — FTR: Input validation."""
        if not self.prompt:
            raise ValueError("prompt is required")
        if self.max_tokens < 1 or self.max_tokens > 8192:
            raise ValueError(f"max_tokens must be between 1 and 8192, got {self.max_tokens}")
        if self.temperature < 0.0 or self.temperature > 2.0:
            raise ValueError(f"temperature must be between 0.0 and 2.0, got {self.temperature}")


@dataclass
class BedrockResponse:
    """Structured Bedrock invocation response with full metadata."""
    request_id: str
    model_id: str
    response_text: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    latency_ms: float
    cost_usd: float
    model_family: ModelFamily
    stop_reason: str = ""
    error: Optional[str] = None
    error_type: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def success(self) -> bool:
        return self.error is None and bool(self.response_text)


class BedrockClient:
    """
    Production-grade Bedrock Runtime client with multi-model support.

    Features:
    - Multi-model abstraction (Claude, Titan, LLaMA)
    - Automatic request formatting per model family
    - Token counting and cost estimation
    - Guardrails integration
    - Invocation logging
    - Timeout handling

    FTR Compliance:
    - Explicit model ARNs (no wildcards)
    - Timeout enforcement prevents hanging
    - Cost tracking for budget governance
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        secrets_manager_arn: Optional[str] = None,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
    ):
        self.region_name = region_name or os.environ.get("AWS_REGION", "us-east-1")
        self.secrets_manager_arn = secrets_manager_arn
        self.timeout_ms = timeout_ms
        self._client = None
        self._config = None
        self._invocation_log: List[Dict[str, Any]] = []

    @property
    def client(self) -> boto3.client:
        """Lazy-initialize Bedrock Runtime client."""
        if self._client is None:
            config = {
                "service_name": "bedrock-runtime",
                "region_name": self.region_name,
            }
            if self.timeout_ms:
                config["config"] = boto3.Config(
                    connect_timeout=self.timeout_ms / 1000,
                    read_timeout=self.timeout_ms / 1000,
                    retries={"max_attempts": 0},  # FTR: We handle retries in resilience stack
                )
            self._client = boto3.client(**config)
        return self._client

    def get_config(self) -> Dict[str, Any]:
        """Retrieve Bedrock configuration from Secrets Manager if configured."""
        if self._config is None and self.secrets_manager_arn:
            client = boto3.client("secretsmanager")
            response = client.get_secret_value(SecretId=self.secrets_manager_arn)
            self._config = json.loads(response["SecretString"])
        return self._config or {}

    def estimate_tokens(self, text: str) -> int:
        """
        Estimate token count for text input.

        Uses a character-based heuristic (approximately 4 chars per token for English).
        FTR Note: This is an approximation. For exact counts, use the model's
        actual token counting (returned in response metadata).
        """
        if not text:
            return 0
        return max(1, len(text) // 4)

    def estimate_cost(self, model_id: str, input_tokens: int, output_tokens: int) -> float:
        """
        Estimate invocation cost in USD.

        FTR: Cost tracking enables budget governance and chargeback.
        """
        costs = MODEL_COSTS.get(model_id, {"input_per_1k": 0.001, "output_per_1k": 0.003})
        return (input_tokens / 1000) * costs["input_per_1k"] + (output_tokens / 1000) * costs["output_per_1k"]

    def _format_request_body(self, request: BedrockRequest) -> Dict[str, Any]:
        """
        Format request body for the specific model family.

        FTR: Each model family has a different request schema.
        This method abstracts the differences.
        """
        family = ModelFamily.from_model_id(request.model_id)

        if family == ModelFamily.ANTHROPIC_CLAUDE:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": request.max_tokens,
                "temperature": request.temperature,
                "top_p": request.top_p,
                "messages": [{"role": "user", "content": request.prompt}],
            }
            if request.system_prompt:
                body["system"] = request.system_prompt
            if request.stop_sequences:
                body["stop_sequences"] = request.stop_sequences

        elif family == ModelFamily.AMAZON_TITAN:
            body = {
                "inputText": request.prompt,
                "textGenerationConfig": {
                    "maxTokenCount": request.max_tokens,
                    "temperature": request.temperature,
                    "topP": request.top_p,
                    "stopSequences": request.stop_sequences,
                },
            }

        elif family == ModelFamily.META_LLAMA:
            body = {
                "prompt": request.prompt,
                "max_gen_len": request.max_tokens,
                "temperature": request.temperature,
                "top_p": request.top_p,
            }

        else:
            # Generic fallback
            body = {
                "prompt": request.prompt,
                "max_tokens": request.max_tokens,
                "temperature": request.temperature,
            }

        return body

    def _parse_response_body(self, model_id: str, response_body: Dict[str, Any]) -> tuple:
        """
        Parse response body and extract text, tokens, and stop reason.

        Returns (text, input_tokens, output_tokens, stop_reason).
        """
        family = ModelFamily.from_model_id(model_id)

        if family == ModelFamily.ANTHROPIC_CLAUDE:
            text = response_body.get("content", [{}])[0].get("text", "")
            usage = response_body.get("usage", {})
            return text, usage.get("input_tokens", 0), usage.get("output_tokens", 0), response_body.get("stop_reason", "end_turn")

        elif family == ModelFamily.AMAZON_TITAN:
            results = response_body.get("results", [])
            text = results[0].get("outputText", "") if results else ""
            return text, response_body.get("inputTokenCount", 0), response_body.get("outputTokenCount", 0), results[0].get("completionReason", "end_turn") if results else "end_turn"

        elif family == ModelFamily.META_LLAMA:
            text = response_body.get("generation", "")
            return text, self.estimate_tokens(""), self.estimate_tokens(text), "end_turn"

        else:
            text = response_body.get("output", response_body.get("generation", ""))
            return text, self.estimate_tokens(""), self.estimate_tokens(text), "end_turn"

    def invoke(self, request: BedrockRequest) -> BedrockResponse:
        """
        Invoke a Bedrock model with full error handling and metadata.

        FTR Compliance:
        - Explicit model ARN
        - Timeout enforcement
        - Cost estimation
        - Invocation logging
        """
        start_time = time.monotonic()

        try:
            body = self._format_request_body(request)

            invoke_kwargs = {
                "modelId": request.model_id,
                "body": json.dumps(body),
            }

            # FTR: Guardrails integration if configured
            if request.guardrail_id and request.guardrail_version:
                invoke_kwargs["guardrailIdentifier"] = request.guardrail_id
                invoke_kwargs["guardrailVersion"] = request.guardrail_version

            raw_response = self.client.invoke_model(**invoke_kwargs)
            response_body = json.loads(raw_response["Body"].read().decode("utf-8"))

            response_text, input_tokens, output_tokens, stop_reason = self._parse_response_body(
                request.model_id, response_body
            )
            latency_ms = (time.monotonic() - start_time) * 1000
            cost_usd = self.estimate_cost(request.model_id, input_tokens, output_tokens)

            response = BedrockResponse(
                request_id=request.request_id,
                model_id=request.model_id,
                response_text=response_text,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
                latency_ms=round(latency_ms, 2),
                cost_usd=round(cost_usd, 6),
                model_family=ModelFamily.from_model_id(request.model_id),
                stop_reason=stop_reason,
            )

            self._log_invocation(request, response)
            return response

        except Exception as e:
            latency_ms = (time.monotonic() - start_time) * 1000
            response = BedrockResponse(
                request_id=request.request_id,
                model_id=request.model_id,
                response_text="",
                input_tokens=0,
                output_tokens=0,
                total_tokens=0,
                latency_ms=round(latency_ms, 2),
                cost_usd=0.0,
                model_family=ModelFamily.from_model_id(request.model_id),
                error=str(e),
                error_type=type(e).__name__,
            )
            self._log_invocation(request, response)
            return response

    def _log_invocation(self, request: BedrockRequest, response: BedrockResponse) -> None:
        """Record invocation in internal log for audit trail."""
        log_entry = {
            "request_id": request.request_id,
            "model_id": request.model_id,
            "prompt_length": len(request.prompt),
            "input_tokens": response.input_tokens,
            "output_tokens": response.output_tokens,
            "latency_ms": response.latency_ms,
            "cost_usd": response.cost_usd,
            "success": response.success,
            "error": response.error,
            "timestamp": response.timestamp,
        }
        self._invocation_log.append(log_entry)

        # Keep log bounded (last 100 invocations)
        if len(self._invocation_log) > 100:
            self._invocation_log = self._invocation_log[-100:]

    def get_invocation_log(self) -> List[Dict[str, Any]]:
        """Retrieve invocation log for analysis."""
        return list(self._invocation_log)

    def get_total_cost(self) -> float:
        """Calculate total cost across all invocations."""
        return sum(entry["cost_usd"] for entry in self._invocation_log)

    def get_average_latency(self) -> float:
        """Calculate average latency across all invocations."""
        if not self._invocation_log:
            return 0.0
        total = sum(entry["latency_ms"] for entry in self._invocation_log)
        return total / len(self._invocation_log)
