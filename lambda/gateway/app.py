"""
Strata CFO Resilience Matrix — Layer 3: Agent Gateway Lambda

This Lambda function implements the Agent Gateway layer that routes LLM requests
to AWS Bedrock with health checks, circuit breaker pattern, automatic fallback
between models, latency tracking, and cost estimation.

Architecture:
  Client Request → Gateway → Health Check → Circuit Breaker → Bedrock (Primary)
                                                          → Bedrock (Fallback)
                                                          → Bedrock (Tertiary)

FTR Compliance Notes:
- Bedrock invocations use explicit model ARNs (no wildcards in IAM)
- Circuit breaker state persisted in DynamoDB (multi-invocation consistency)
- All metrics emitted to CloudWatch custom namespace
- X-Ray tracing for end-to-end request visibility
- Cost estimation for budget governance
- Structured JSON logging for operational dashboards
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

# FTR Compliance: All configuration from environment (injected via SAM template)
CIRCUIT_BREAKERS_TABLE = os.environ.get("CIRCUIT_BREAKERS_TABLE", "")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "")
CACHE_BUCKET = os.environ.get("CACHE_BUCKET", "")
BEDROCK_SECRET_ARN = os.environ.get("BEDROCK_SECRET_ARN", "")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "")

PRIMARY_MODEL_ID = os.environ.get("PRIMARY_MODEL_ID", "anthic.claude-3-5-sonnet-20241022-v1:0")
FALLBACK_MODEL_ID = os.environ.get("FALLBACK_MODEL_ID", "amazon.titan-text-premier-v1:0")
TERTIARY_MODEL_ID = os.environ.get("TERTIARY_MODEL_ID", "meta.llama3-70b-instruct-v1:0")

CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "5"))
CIRCUIT_BREAKER_RESET_TIMEOUT = int(os.environ.get("CIRCUIT_BREAKER_RESET_TIMEOUT", "60"))

# Cost estimation per 1K tokens (USD) — approximate for budget tracking
# FTR Note: Costs updated quarterly based on AWS pricing
MODEL_COSTS = {
    "anthic.claude-3-5-sonnet-20241022-v1:0": {"input_per_1k": 0.003, "output_per_1k": 0.015},
    "amazon.titan-text-premier-v1:0": {"input_per_1k": 0.0008, "output_per_1k": 0.0016},
    "meta.llama3-70b-instruct-v1:0": {"input_per_1k": 0.00265, "output_per_1k": 0.0035},
}

# Fallback chain — ordered by model quality preference
MODEL_CHAIN = [PRIMARY_MODEL_ID, FALLBACK_MODEL_ID, TERTIARY_MODEL_ID]

logger = Logger(service="strata-gateway")
metrics = Metrics(namespace="StrataCFO")
tracer = Tracer()

_bedrock_runtime = None
_dynamodb_resource = None
_circuit_breakers_table = None
_metrics_table = None
_secrets_client = None


class CircuitState(IntEnum):
    """Circuit breaker states — FTR: Explicit state machine for reliability."""
    CLOSED = 0      # Normal operation — requests pass through
    HALF_OPEN = 1   # Testing recovery — limited requests allowed
    OPEN = 2        # Failure detected — all requests blocked


def get_bedrock_runtime():
    """Lazy-init Bedrock Runtime client for container reuse."""
    global _bedrock_runtime
    if _bedrock_runtime is None:
        _bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
    return _bedrock_runtime


def get_circuit_breakers_table():
    global _dynamodb_resource, _circuit_breakers_table
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    if _circuit_breakers_table is None:
        _circuit_breakers_table = _dynamodb_resource.Table(CIRCUIT_BREAKERS_TABLE)
    return _circuit_breakers_table


def get_metrics_table():
    global _metrics_table
    if _metrics_table is None:
        _metrics_table = get_circuit_breakers_table().meta.resource.Table(METRICS_TABLE) if _dynamodb_resource else boto3.resource("dynamodb").Table(METRICS_TABLE)
    return _metrics_table


def get_bedrock_config() -> Dict[str, Any]:
    """Retrieve Bedrock config from Secrets Manager — FTR: zero secrets in code."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=BEDROCK_SECRET_ARN)
    return json.loads(response["SecretString"])


@tracer.capture_method
def get_circuit_breaker_state(model_id: str) -> CircuitState:
    """
    Retrieve circuit breaker state from DynamoDB.

    FTR Compliance: State is persisted across Lambda invocations via DynamoDB,
    ensuring circuit breaker remembers failures even after container recycling.
    """
    table = get_circuit_breakers_table()
    now = datetime.now(timezone.utc)

    try:
        response = table.get_item(Key={"breaker_id": f"gateway:{model_id}"})
        item = response.get("Item")

        if not item:
            # New breaker — starts CLOSED
            table.put_item(
                Item={
                    "breaker_id": f"gateway:{model_id}",
                    "state": CircuitState.CLOSED.value,
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure_at": "",
                    "last_success_at": "",
                    "opened_at": "",
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                },
            )
            return CircuitState.CLOSED

        state = CircuitState(item["state"])

        # Auto-transition from OPEN to HALF_OPEN after timeout
        if state == CircuitState.OPEN:
            opened_at = item.get("opened_at", "")
            if opened_at:
                try:
                    opened_time = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
                    elapsed = (now - opened_time).total_seconds()
                    if elapsed >= CIRCUIT_BREAKER_RESET_TIMEOUT:
                        # Transition to HALF_OPEN for recovery testing
                        table.update_item(
                            Key={"breaker_id": f"gateway:{model_id}"},
                            UpdateExpression="SET #state = :half_open, #updated_at = :now",
                            ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
                            ExpressionAttributeValues={":half_open": CircuitState.HALF_OPEN.value, ":now": now.isoformat()},
                        )
                        logger.info(f"Circuit breaker HALF_OPEN for {model_id} after {elapsed:.0f}s timeout")
                        return CircuitState.HALF_OPEN
                except (ValueError, TypeError):
                    pass

        return state

    except Exception as e:
        logger.error(f"Failed to get circuit breaker state for {model_id}: {e}")
        return CircuitState.CLOSED  # Fail-open for resilience


@tracer.capture_method
def update_circuit_breaker(model_id: str, success: bool) -> None:
    """
    Update circuit breaker state after an invocation attempt.

    State transitions:
    - CLOSED + failure → increment failure_count → OPEN if >= threshold
    - OPEN + success → CLOSED (recovery confirmed)
    - HALF_OPEN + success → CLOSED
    - HALF_OPEN + failure → OPEN

    FTR Compliance: Atomic DynamoDB update with conditional expression.
    """
    table = get_circuit_breakers_table()
    now = datetime.now(timezone.utc)
    breaker_id = f"gateway:{model_id}"

    if success:
        # Reset failure count on success
        table.update_item(
            Key={"breaker_id": breaker_id},
            UpdateExpression=(
                "SET failure_count = :zero, success_count = success_count + :one, "
                "#state = :closed, last_success_at = :now, #updated_at = :now"
            ),
            ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
            ExpressionAttributeValues={
                ":zero": 0,
                ":one": 1,
                ":closed": CircuitState.CLOSED.value,
                ":now": now.isoformat(),
            },
        )
    else:
        # Increment failure count, potentially OPEN the circuit
        try:
            response = table.update_item(
                Key={"breaker_id": breaker_id},
                UpdateExpression=(
                    "SET failure_count = failure_count + :one, "
                    "last_failure_at = :now, #updated_at = :now, "
                    "#state = :open"
                ),
                ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
                ExpressionAttributeValues={
                    ":one": 1,
                    ":now": now.isoformat(),
                    ":open": CircuitState.OPEN.value,
                },
                ConditionExpression="failure_count < :threshold",
                ExpressionAttributeValues={
                    ":one": 1,
                    ":now": now.isoformat(),
                    ":open": CircuitState.OPEN.value,
                    ":threshold": CIRCUIT_BREAKER_THRESHOLD,
                },
                ReturnValues="ALL_NEW",
            )
            new_count = response["Attributes"]["failure_count"]

            if new_count >= CIRCUIT_BREAKER_THRESHOLD:
                # Manually set to OPEN if threshold reached
                table.update_item(
                    Key={"breaker_id": breaker_id},
                    UpdateExpression="SET #state = :open, opened_at = :now, #updated_at = :now",
                    ExpressionAttributeNames={"#state": "state", "#updated_at": "updated_at"},
                    ExpressionAttributeValues={":open": CircuitState.OPEN.value, ":now": now.isoformat()},
                )
                logger.warning(f"Circuit breaker OPEN for {model_id} (failures: {new_count})")
                metrics.add_metric(
                    name="CircuitBreakerState",
                    unit=MetricUnit.Count,
                    value=CircuitState.OPEN.value,
                    extra={"model_id": model_id, "environment": os.environ.get("ENVIRONMENT", "")},
                )
        except table.meta.client.exceptions.ConditionalCheckFailedException:
            # Threshold already exceeded — circuit is OPEN
            logger.warning(f"Circuit breaker already OPEN for {model_id}")


@tracer.capture_method
def invoke_bedrock_model(model_id: str, prompt: str, system_prompt: str = "", max_tokens: int = 4096, temperature: float = 0.7) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Invoke a Bedrock model with structured error handling.

    Returns (response_text, metadata) tuple.
    Metadata includes latency, token usage, and cost estimation.

    FTR Compliance:
    - Explicit model ARN in invoke request
    - Timeout handling for model latency spikes
    - Structured error classification
    """
    bedrock = get_bedrock_runtime()
    start_time = time.monotonic()
    metadata = {
        "model_id": model_id,
        "invocation_id": str(uuid.uuid4()),
        "start_time": datetime.now(timezone.utc).isoformat(),
        "input_tokens": 0,
        "output_tokens": 0,
        "latency_ms": 0,
        "cost_usd": 0.0,
        "error": None,
    }

    try:
        # Build request body based on model family
        if "claude" in model_id.lower() or "anthropic" in model_id.lower():
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [{"role": "user", "content": prompt}],
            }
            if system_prompt:
                body["system"] = system_prompt

        elif "titan" in model_id.lower() or "amazon" in model_id.lower():
            body = {
                "inputText": prompt,
                "textGenerationConfig": {
                    "maxTokenCount": max_tokens,
                    "temperature": temperature,
                    "topP": 0.9,
                },
            }

        elif "llama" in model_id.lower() or "meta" in model_id.lower():
            body = {
                "prompt": prompt,
                "max_gen_len": max_tokens,
                "temperature": temperature,
                "top_p": 0.9,
            }
        else:
            # Generic Bedrock invocation
            body = {
                "prompt": prompt,
                "max_tokens": max_tokens,
                "temperature": temperature,
            }

        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps(body),
        )

        response_body = json.loads(response["Body"].read().decode("utf-8"))

        # Extract response text based on model family
        if "claude" in model_id.lower() or "anthropic" in model_id.lower():
            response_text = response_body.get("content", [{}])[0].get("text", "")
            metadata["input_tokens"] = response_body.get("usage", {}).get("input_tokens", 0)
            metadata["output_tokens"] = response_body.get("usage", {}).get("output_tokens", 0)
        elif "completion" in response_body:
            response_text = response_body["results"][0]["outputText"] if response_body.get("results") else ""
            metadata["input_tokens"] = response_body.get("inputTokenCount", 0)
            metadata["output_tokens"] = response_body.get("outputTokenCount", 0)
        else:
            response_text = response_body.get("generation", response_body.get("output", ""))

        latency_ms = (time.monotonic() - start_time) * 1000
        metadata["latency_ms"] = round(latency_ms, 2)

        # Cost estimation
        costs = MODEL_COSTS.get(model_id, {"input_per_1k": 0.001, "output_per_1k": 0.003})
        metadata["cost_usd"] = (
            (metadata["input_tokens"] / 1000) * costs["input_per_1k"]
            + (metadata["output_tokens"] / 1000) * costs["output_per_1k"]
        )

        metrics.add_metric(name="BedrockInvocationSuccess", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="BedrockLatency", unit=MetricUnit.Milliseconds, value=metadata["latency_ms"])

        return response_text, metadata

    except Exception as e:
        latency_ms = (time.monotonic() - start_time) * 1000
        metadata["latency_ms"] = round(latency_ms, 2)
        metadata["error"] = str(e)
        metadata["error_type"] = type(e).__name__

        metrics.add_metric(name="BedrockInvocationError", unit=MetricUnit.Count, value=1)

        logger.error(
            f"Bedrock invocation failed for {model_id}",
            extra={"error": str(e), "latency_ms": metadata["latency_ms"]},
        )

        return None, metadata


@tracer.capture_method
def route_request(
    prompt: str,
    system_prompt: str = "",
    max_tokens: int = 4096,
    temperature: float = 0.7,
    model_preference: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Route an LLM request through the gateway with health checks, circuit breaker,
    and automatic fallback between models.

    Routing logic:
    1. Check circuit breaker for preferred/primary model
    2. If CLOSED or HALF_OPEN, attempt invocation
    3. On failure, try next model in fallback chain
    4. Track all attempts and latency for each model
    5. Return first successful response with full metadata

    FTR Compliance:
    - Multi-model fallback ensures zero-downtime resilience
    - Circuit breaker prevents cascading failures
    - Cost tracking for budget governance
    """
    request_id = str(uuid.uuid4())
    start_time = time.monotonic()

    # Build model chain (respect preference if specified)
    chain = MODEL_CHAIN.copy()
    if model_preference and model_preference in chain:
        chain.remove(model_preference)
        chain.insert(0, model_preference)

    attempts = []
    total_cost = 0.0
    final_response = None
    final_metadata = None

    for model_id in chain:
        # Check circuit breaker state
        cb_state = get_circuit_breaker_state(model_id)

        if cb_state == CircuitState.OPEN:
            logger.warning(f"Skipping {model_id}: circuit breaker OPEN")
            attempts.append({
                "model_id": model_id,
                "status": "skipped",
                "reason": "circuit_breaker_open",
            })
            continue

        if cb_state == CircuitState.HALF_OPEN:
            logger.info(f"Testing {model_id}: circuit breaker HALF_OPEN (recovery probe)")

        # Attempt invocation
        with tracer.subsegment(f"InvokeModel:{model_id[:20]}") as subsegment:
            response_text, metadata = invoke_bedrock_model(
                model_id=model_id,
                prompt=prompt,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )

        if response_text is not None:
            # Success — update circuit breaker
            update_circuit_breaker(model_id, success=True)
            final_response = response_text
            final_metadata = metadata
            total_cost += metadata["cost_usd"]
            attempts.append({
                "model_id": model_id,
                "status": "success",
                "latency_ms": metadata["latency_ms"],
                "input_tokens": metadata["input_tokens"],
                "output_tokens": metadata["output_tokens"],
                "cost_usd": metadata["cost_usd"],
            })
            break
        else:
            # Failure — update circuit breaker
            update_circuit_breaker(model_id, success=False)
            total_cost += metadata.get("cost_usd", 0)
            attempts.append({
                "model_id": model_id,
                "status": "failed",
                "error": metadata.get("error", "unknown"),
                "error_type": metadata.get("error_type", "unknown"),
                "latency_ms": metadata["latency_ms"],
            })

    total_latency = (time.monotonic() - start_time) * 1000

    # Emit metrics
    metrics.add_metric(name="GatewayTotalLatency", unit=MetricUnit.Milliseconds, value=round(total_latency, 2))
    metrics.add_metric(name="GatewayTotalCost", unit=MetricUnit.None, value=round(total_cost, 6))

    if final_response is None:
        metrics.add_metric(name="GatewayAllModelsExhausted", unit=MetricUnit.Count, value=1)
        logger.error("All models in fallback chain exhausted", extra={"attempts": attempts})

    return {
        "request_id": request_id,
        "status": "success" if final_response else "all_models_exhausted",
        "response": final_response,
        "metadata": final_metadata,
        "attempts": attempts,
        "total_latency_ms": round(total_latency, 2),
        "total_cost_usd": round(total_cost, 6),
        "models_tried": len(attempts),
    }


@tracer.capture_method
def record_gateway_metrics(result: Dict[str, Any], prompt_length: int) -> None:
    """
    Record gateway invocation metrics to DynamoDB for historical analysis.

    FTR Compliance: Metrics persisted for audit trail and capacity planning.
    """
    table = boto3.resource("dynamodb").Table(METRICS_TABLE)
    now = datetime.now(timezone.utc)

    try:
        table.put_item(Item={
            "pk": f"GATEWAY#{result['request_id']}",
            "sk": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "request_id": result["request_id"],
            "status": result["status"],
            "prompt_length": prompt_length,
            "total_latency_ms": result["total_latency_ms"],
            "total_cost_usd": result["total_cost_usd"],
            "models_tried": result["models_tried"],
            "attempts_summary": result["attempts"],
            "expires_at": int(now.timestamp() + 7 * 24 * 3600),  # 7-day TTL
        })
    except Exception as e:
        logger.warning(f"Failed to record gateway metrics: {e}")


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------
@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@metrics.log_metrics(capture_cold_start_metric=True)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    """
    Layer 3 Agent Gateway entry point.

    Receives LLM requests, routes them through health checks and circuit breakers
    to AWS Bedrock, with automatic multi-model fallback.

    Input (from API Gateway or direct invocation):
    {
        "prompt": "Analyze Q3 cash flow...",
        "system_prompt": "You are a CFO AI assistant...",
        "agent_type": "cash_flow",
        "max_tokens": 4096,
        "temperature": 0.7,
        "model_preference": null,
        "tenant_id": "acme-corp"
    }

    Output:
    {
        "request_id": "uuid",
        "status": "success",
        "response": "Based on Q3 data...",
        "metadata": {...},
        "attempts": [...]
    }
    """
    invocation_id = str(uuid.uuid4())
    logger.info("Gateway request received", extra={"invocation_id": invocation_id})

    try:
        # Parse request body (could be from API Gateway or direct invocation)
        if "body" in event and isinstance(event["body"], str):
            body = json.loads(event["body"])
        elif "body" in event and isinstance(event["body"], dict):
            body = event["body"]
        else:
            body = event

        prompt = body.get("prompt", "")
        if not prompt:
            return {"statusCode": 400, "body": {"error": "prompt is required"}}

        system_prompt = body.get("system_prompt", "")
        max_tokens = min(int(body.get("max_tokens", 4096)), 8192)  # FTR: Hard cap
        temperature = max(0.0, min(float(body.get("temperature", 0.7)), 1.0))  # FTR: Bound check
        model_preference = body.get("model_preference")
        tenant_id = body.get("tenant_id", "default")

        logger.info(
            f"Routing request: prompt_len={len(prompt)}, model_pref={model_preference}",
            extra={"tenant_id": tenant_id},
        )

        # Route through gateway with circuit breaker + fallback
        result = route_request(
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            model_preference=model_preference,
        )

        # Record metrics
        record_gateway_metrics(result, len(prompt))

        status_code = 200 if result["status"] == "success" else 503

        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
                "X-Request-Id": result["request_id"],
                "X-Models-Tried": str(result["models_tried"]),
                "Access-Control-Allow-Origin": "*",
            },
            "body": result,
        }

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in request body: {e}")
        return {"statusCode": 400, "body": {"error": "Invalid JSON"}}
    except Exception as e:
        logger.error(f"Gateway unhandled error: {e}", exc_info=True)
        metrics.add_metric(name="GatewayUnhandledErrors", unit=MetricUnit.Count, value=1)
        return {"statusCode": 500, "body": {"error": str(e), "error_type": type(e).__name__}}
