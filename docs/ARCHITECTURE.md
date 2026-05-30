# Strata CFO Resilience Matrix — Architecture Documentation

## System Overview

Strata is a 6-layer AI resilience system designed for CFO operations, built entirely
on AWS-native services. It ensures that LLM-powered financial analysis agents remain
available, responsive, and accurate even during provider outages, latency spikes,
and partial service degradation.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STRATA CFO RESILIENCE MATRIX                         │
│                    (AWS Partner Network — FTR Submission)                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Amazon API Gateway (REST)                         │   │
│  │  ┌────────────┐  /api/curate   /api/agents   /api/chaos/trigger     │   │
│  │  │  Cognito   │──────────────────────────────────────────────────   │   │
│  │  │ Authorizer │  Multi-tenant ABAC (tenant_id in JWT claims)        │   │
│  │  └────────────┘                                                  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌─────────────────────────────────┼──────────────────────────────────┐    │
│  │           Amazon EventBridge    │  (Chaos Scheduling + Triggers)    │    │
│  │  ┌──────────────┐  ┌──────────────────────────┐  ┌───────────────┐  │    │
│  │  │ cron(*/6h)   │  │ Resilience Event Rules  │  │ CB Alarm Rule │  │    │
│  │  │ Chaos Trigger │  │ Circuit Breaker Alarms  │  │ → Recovery    │  │    │
│  │  └──────────────┘  └──────────────────────────┘  └───────────────┘  │    │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                           │         │         │                                │
│  ╔═══════════════╦═════════╬═════════╬═════════╦══════════════════════════╗  │
│  ║ LAYER 6       ║ LAYER 5  ║ LAYER 4  ║ LAYER 3  ║ LAYER 2      ║ LAYER 1 ║  │
│  ║               ║          ║          ║          ║              ║         ║  │
│  ║ CFO AGENTS    ║ CHAOS    ║ RESIL-   ║ AGENT    ║ FINE-TUNING  ║ DATA    ║  │
│  ║ Lambda        ║ ENGINE   ║ IENCE    ║ GATEWAY  ║ PIPELINE     ║ CURATE  ║  │
│  ║               ║ Lambda   ║ STACK    ║ Lambda   ║ Lambda       ║ Lambda  ║  │
│  ║               ║          ║ Lambda   ║          ║              ║         ║  │
│  ║ • Cash Flow   ║ • LLM    ║          ║          ║ • Bedrock    ║ • S3    ║  │
│  ║ • Risk        ║   Outage ║ 6-LAYER  ║ Circuit   ║   Custom     ║   Fetch ║  │
│  ║ • Compliance  ║ • Latency ║ STACK:   ║ Breaker  ║   Training  ║ • Norm  ║  │
│  ║ • Treasury    ║ • Rate   ║          ║ Pattern   ║ • Job Track  ║ • Class ║  │
│  ║               ║   Limit  ║ 1.Retry  ║          ║ • Artifact   ║ • Store ║  │
│  ║ All requests  ║ • Network║ 2.Circuit ║ Model    ║   Versioning ║         ║  │
│  ║ → Resilience ║   Part.  ║   Breaker ║ Fallback ║              ║         ║  │
│  ║   Stack       ║ • CB     ║ 3.Model   ║ Chain:   ║              ║         ║  │
│  ║               ║   Cascade║   Fallback ║ Claude → ║              ║         ║  │
│  ║               ║ • Cache  ║ 4.Semantc ║ Titan →  ║              ║         ║  │
│  ║               ║   Inval. ║   Cache   ║ LLaMA    ║              ║         ║  │
│  ║               ║ • Degrad ║ 5.Graceful║          ║              ║         ║  │
│  ║               ║ • Multi  ║   Degrade ║ Health   ║              ║         ║  │
│  ║               ║   Fallback║ 6.Timeout ║ Checks   ║              ║         ║  │
│  ║               ║          ║          ║          ║              ║         ║  │
│  ╚═══════════════╩══════════╩══════════╩══════════╩══════════════╩═════════╝  │
│        │              │             │              │              │             │
│  ╔══════╧══════════════╧════════════╧══════════════╧══════════════╧════════╗ │
│  ║                    AWS SERVICES (Data Plane)                             ║ │
│  ║                                                                     ║ │
│  ║  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  ┌───────────┐ ║ │
│  ║  │AWS Bedrock   │  │Amazon S3    │  │Amazon        │  │Amazon     │ ║ │
│  ║  │Runtime       │  │             │  │DynamoDB      │  │SQS        │ ║ │
│  ║  │             │  │• curated-   │  │              │  │           │ ║ │
│  ║  │• Claude 3.5 │  │  data       │  │• resilience  │  │• resilience║ ║ │
│  ║  │  Sonnet     │  │• model-     │  │  _metrics    │  │  -events  │ ║ │
│  ║  │• Titan      │  │  artifacts  │  │• circuit     │  │• chaos    │ ║ │
│  ║  │• LLaMA 3    │  │• resilience │  │  _breakers   │  │  -tasks   │ ║ │
│  ║  │             │  │  _logs      │  │• chaos       │  │           │ ║ │
│  ║  └─────────────┘  └─────────────┘  │  _results    │  └───────────┘ ║ │
│  ║                                    │• fine_tuning  │                ║ │
│  ║                                    │  _jobs        │                ║ │
│  ║                                    └──────────────┘                ║ │
│  ║                                                                     ║ │
│  ║  ┌──────────────────┐  ┌───────────────┐  ┌──────────────────┐   ║ │
│  ║  │Amazon OpenSearch  │  │Amazon Cognito  │  │AWS Secrets       │   ║ │
│  ║  │Serverless         │  │                │  │Manager           │   ║ │
│  ║  │                   │  │• User Pools    │  │                  │   ║ │
│  ║  │• Log aggregation  │  │• ABAC tenant   │  │• Bedrock config  │   ║ │
│  ║  │• Pattern search   │  │  claims        │  │• OpenSearch     │   ║ │
│  ║  │• Failure indexing  │  │• JWT tokens     │  │  credentials    │   ║ │
│  ║  └──────────────────┘  └───────────────┘  └──────────────────┘   ║ │
│  ╚═══════════════════════════════════════════════════════════════════╝ │
│                                                                             │
│  ╔══════════════════════════════════════════════════════════════════════╗ │
│  ║                    CROSS-CUTTING CONCERNS                              ║ │
│  ║                                                                     ║ │
│  ║  ┌────────────┐  ┌──────────────┐  ┌────────────┐  ┌────────────┐  ║ │
│  ║  │AWS KMS CMK │  │CloudWatch   │  │AWS X-Ray   │  │Custom VPC  │  ║ │
│  ║  │            │  │             │  │            │  │            │  ║ │
│  ║  │• All data   │  │• Logs       │  │• Tracing   │  │• 2 AZs     │  ║ │
│  ║  │  encrypted │  │• Metrics    │  │• Subseg-   │  │• Private    │  ║ │
│  ║  │• Key       │  │• Alarms     │  │  ments     │  │  subnets   │  ║ │
│  ║  │  rotation  │  │• Dashboard  │  │• Service   │  │• NAT GW    │  ║ │
│  ║  │            │  │• Insights   │  │  map       │  │• VPC       │  ║ │
│  ║  │            │  │            │  │            │  │  endpoints  │  ║ │
│  ║  └────────────┘  └──────────────┘  └────────────┘  └────────────┘  ║ │
│  ║                                                                     ║ │
│  ║  ┌────────────┐  ┌──────────────┐                                  ║ │
│  ║  │CloudTrail  │  │VPC Flow     │                                  ║ │
│  ║  │            │  │Logs         │                                  ║ │
│  ║  │• API audit │  │             │                                  ║ │
│  ║  │• Log val.  │  │• Traffic    │                                  ║ │
│  ║  │• S3 + CW   │  │  auditing  │                                  ║ │
│  ║  └────────────┘  └──────────────┘                                  ║ │
│  ╚══════════════════════════════════════════════════════════════════════╝ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Request Flow (CFO Agent Invocation)

```
Client
  │
  ├─ POST /api/agents (Cognito JWT with tenant_id)
  │    │
  │    ▼
  │  API Gateway (Cognito Authorizer validates tenant_id)
  │    │
  │    ▼
  │  Lambda: agents (Layer 6 — CFO Agent)
  │    │
  │    ├─ Validate tenant access (ABAC)
  │    ├─ Build structured prompt from agent definition
  │    │
  │    ▼
  │  Lambda: resilience_stack (Layer 4 — 6-Layer Resilience)
  │    │
  │    ├─ Layer 4: Check Semantic Cache (S3-backed)
  │    │    ├── HIT → Return cached response (bypass layers 3-6)
  │    │    └── MISS → Continue
  │    │
  │    ├─ Layer 5: Apply Graceful Degradation
  │    │
  │    ├─ Layer 3: Model Fallback Chain
  │    │    ├── Claude 3.5 Sonnet (Primary)
  │    │    │    └── Layer 2: Circuit Breaker check
  │    │    │         └── Layer 1: Retry with backoff
  │    │    ├── Titan (Fallback) — if primary fails
  │    │    └── LLaMA 3 (Tertiary) — if both fail
  │    │
  │    ├─ Layer 4: Store response in Semantic Cache
  │    │
  │    └─ Layer 6: Verify timeout not exceeded
  │
  │    ▼
  │  AWS Bedrock Runtime (Claude/Titan/LLaMA invocation)
  │
  ▼
Response (JSON with analysis + metrics)
```

### Chaos Engine Flow

```
EventBridge (cron: every 6 hours)
  │
  ▼
Lambda: chaos_engine (Layer 5)
  │
  ├─ LLM Provider Outage Test
  ├─ Latency Spike Test
  ├─ Rate Limiting Test
  ├─ Context Overflow Test
  ├─ Network Partition Test
  ├─ Circuit Breaker Cascade Test
  ├─ Cache Invalidation Test
  ├─ Graceful Degradation Test
  └─ Multi-Model Failover Test
  │
  ▼
DynamoDB: chaos_results (Pass/Fail per scenario)
  │
  ▼
CloudWatch Metrics: ChaosPassRate, ChaosTestFailed
```

## AWS Service Dependencies

| Layer | Primary Service | Supporting Services |
|-------|---------------|-------------------|
| Data Curation | S3, Lambda | DynamoDB, OpenSearch, KMS |
| Fine-Tuning | Bedrock, Lambda | S3, DynamoDB, Secrets Manager |
| Agent Gateway | Bedrock Runtime, Lambda | DynamoDB, S3, KMS, CloudWatch |
| Resilience Stack | Lambda | Bedrock Runtime, DynamoDB, S3, KMS |
| Chaos Engine | Lambda, EventBridge | DynamoDB, CloudWatch |
| CFO Agents | Lambda | Bedrock Runtime, DynamoDB, S3, Cognito |

## Security Model

- **Encryption**: KMS CMK with automatic key rotation
- **Identity**: Cognito User Pools with ABAC (tenant_id in JWT claims)
- **Network**: Custom VPC with private subnets, VPC endpoints for S3/DynamoDB
- **Access**: Least-privilege IAM per Lambda function (no wildcard resources)
- **Secrets**: Secrets Manager for all credentials (zero in code)
- **Auditing**: CloudTrail + VPC Flow Logs + CloudWatch Logs

## Cost Optimization

- Lambda provisioned concurrency for gateway (cold-start mitigation)
- DynamoDB on-demand (PAY_PER_REQUEST) for variable workloads
- S3 lifecycle policies (GLACIER archival after 90 days)
- CloudWatch log retention (30-day, not indefinite)
- SQS DLQ with shorter retention than primary queues
