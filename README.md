# Strata CFO Resilience Matrix

[![Apache 2.0 License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![AWS FTR Compliant](https://img.shields.io/badge/FTR-Compliant-brightgreen)](docs/WELL_ARCHITECTED.md)
[![AWS SAM](https://img.shields.io/badge/IaC-AWS%20SAM-orange)](template.yaml)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](lambda/curate/app.py)

A 6-layer AI resilience system for CFO operations, built entirely on AWS-native services.
Designed for AWS Partner Network Foundational Technical Review (FTR) submission.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                  Amazon API Gateway (REST + Cognito ABAC)          │
├────────────────────────────────────────────────────────────────────┤
│                Amazon EventBridge (Scheduling + Triggers)          │
├──────────┬──────────┬──────────┬──────────┬──────────┬────────────┤
│ Layer 6  │ Layer 5  │ Layer 4  │ Layer 3  │ Layer 2  │ Layer 1    │
│ CFO      │ Chaos    │ Resil-   │ Agent    │ Fine-    │ Data       │
│ Agents   │ Engine   │ ience    │ Gateway  │ Tuning   │ Curation   │
│          │          │ Stack    │          │ Pipeline │            │
│          │          │          │          │          │            │
│ CashFlow │ 9 Chaos  │ 6-Layer  │ Circuit  │ Bedrock  │ S3 → Norm  │
│ Risk     │ Scenarios│ Protect  │ Breaker  │ Training │ Classify   │
│ Treasury │ Automated│ Retry    │ Fallback │ Version  │ Store      │
│Compliance│ Testing  │ Cache    │ Claude→  │ Control  │ Metrics    │
│          │          │ Degrade  │ Titan→   │          │            │
│          │          │ Timeout  │ LLaMA    │          │            │
├──────────┴──────────┴──────────┴──────────┴──────────┴────────────┤
│  AWS Bedrock  │ S3 (KMS) │ DynamoDB (KMS) │ OpenSearch │ Cognito  │
│  Claude/Titan │ 3 Bucket │ 4 Tables       │ Serverless │ ABAC JWT  │
│  LLaMA       │          │ TTL + PITR     │            │ MFA       │
├──────────────┴──────────┴─────────────────┴────────────┴──────────┤
│  KMS CMK │ CloudWatch │ X-Ray │ Custom VPC (2 AZs) │ CloudTrail  │
│  Encrypt  │ Logs+Metrics│ Trace│ Private Subnets    │ + VPC Flows │
└──────────┴─────────────┴───────┴────────────────────┴─────────────┘
```

---

## The 6 Resilience Layers

| Layer | Name | Protection |
|-------|------|-----------|
| **1** | **Retry with Backoff** | Exponential backoff with jitter for transient failures |
| **2** | **Circuit Breaker** | DynamoDB-persisted state machine (OPEN/HALF_OPEN/CLOSED) |
| **3** | **Model Fallback** | Claude → Titan → LLaMA automatic chain |
| **4** | **Semantic Cache** | S3-backed, TTL-based content-addressable cache |
| **5** | **Graceful Degradation** | L0→L3 progressive context reduction |
| **6** | **Hard Timeout** | 30-second absolute deadline enforcement |

Every LLM request flows through all 6 layers sequentially.

---

## AWS-Native Stack

| Service | Purpose |
|---------|---------|
| **AWS Bedrock** | LLM inference (Claude 3.5 Sonnet, Titan, LLaMA) |
| **AWS Lambda (Python 3.12)** | 6 serverless functions (one per layer) |
| **Amazon S3** | Curated datasets, model artifacts, resilience logs |
| **Amazon DynamoDB** | Resilience metrics, circuit breaker state, chaos results |
| **Amazon OpenSearch Serverless** | Log aggregation and failure pattern search |
| **AWS Secrets Manager** | Bedrock config, OpenSearch credentials |
| **Amazon Cognito** | Multi-tenant identity with ABAC |
| **Amazon API Gateway** | REST API with Cognito authorizer |
| **Amazon EventBridge** | Chaos scheduling + resilience triggers |
| **Amazon CloudWatch** | Logs, metrics, alarms, dashboards |
| **AWS X-Ray** | End-to-end distributed tracing |
| **AWS KMS** | Customer Master Key (CMK) encryption |
| **Amazon SQS** | Resilience events + chaos task queues |
| **Amazon SNS** | Operational alert notifications |
| **AWS CloudTrail** | API call auditing with log validation |
| **Custom VPC** | 2 AZs, public + private subnets, NAT Gateway |

---

## FTR Compliance Checklist

| # | Requirement | Status |
|---|------------|--------|
| 1 | 100% IaC (SAM template.yaml) | ✅ |
| 2 | Custom VPC with private subnets | ✅ |
| 3 | KMS CMK encryption everywhere | ✅ |
| 4 | Secrets Manager — zero secrets in code | ✅ |
| 5 | Least-privilege IAM (no wildcard resources) | ✅ |
| 6 | CloudWatch logs + metrics + alarms | ✅ |
| 7 | Multi-AZ deployment | ✅ |
| 8 | VPC Flow Logs + CloudTrail | ✅ |
| 9 | Architecture diagram in README | ✅ |
| 10 | Runbook | ✅ |
| 11 | Well-Architected self-assessment | ✅ |

---

## Project Structure

```
strata-aws-native/
├── template.yaml                    # Full SAM template (100% IaC)
├── samconfig.toml                  # SAM deploy configuration
├── requirements.txt                # Python dependencies
├── LICENSE                         # Apache 2.0
├── README.md                       # This file
├── .gitignore
│
├── lambda/                         # Lambda functions (6 layers)
│   ├── curate/app.py              # Layer 1: Data Curation
│   ├── fine_tune/app.py           # Layer 2: Fine-Tuning Pipeline
│   ├── gateway/app.py             # Layer 3: Agent Gateway
│   ├── resilience_stack/app.py    # Layer 4: 6-Layer Resilience Stack
│   ├── chaos/app.py               # Layer 5: Chaos Engine
│   ├── agents/app.py              # Layer 6: CFO Agents (4 agents)
│   └── layers/app.py              # Cross-cutting concerns layer
│
├── lib/                            # Shared Python libraries
│   ├── bedrock.py                 # Bedrock client (multi-model)
│   ├── resilience.py              # Circuit breaker, retry, cache, degradation
│   ├── state.py                   # S3/DynamoDB state management, ABAC
│   └── observability.py           # CloudWatch + X-Ray helpers
│
├── infra/                          # Nested infrastructure stacks
│   ├── vpc-flow-logs.yaml        # VPC Flow Logs configuration
│   └── cloudtrail.yaml            # CloudTrail configuration
│
├── scripts/
│   └── deploy.sh                  # Deployment script
│
├── tests/                          # Resilience pattern tests
│   ├── test_resilience.py         # 6-layer stack tests
│   ├── test_bedrock.py            # Bedrock client tests (mocked)
│   └── test_circuit_breaker.py    # Circuit breaker tests
│
├── .chp/                           # CHP governance
│   ├── STATE_MACHINE.md           # Change management state machine
│   └── R0_CONFIG.yaml             # Release 0 configuration
│
└── docs/                           # Documentation
    ├── ARCHITECTURE.md            # Full architecture with diagram
    ├── RUNBOOK.md                 # Operational runbook
    ├── WELL_ARCHITECTED.md        # Well-Architected self-assessment
    └── SECURITY.md                # Security narrative
```

---

## Quick Start

### Prerequisites

```bash
# AWS CLI v2
pip install awscli

# SAM CLI
pip install aws-sam-cli

# Configure AWS credentials
aws configure

# Clone the repository
git clone <repo-url>
cd strata-aws-native
```

### Deploy

```bash
# Validate template
sam validate --template-file template.yaml

# Build
sam build --template-file template.yaml

# Deploy to production
./scripts/deploy.sh production

# Or deploy to staging
./scripts/deploy.sh staging
```

### Test

```bash
# Run resilience tests
cd tests
python -m pytest test_resilience.py -v

python -m pytest test_circuit_breaker.py -v

python -m pytest test_bedrock.py -v

# Run all tests
python -m pytest -v
```

---

## CFO Agent Types

| Agent | Actions | Description |
|-------|---------|-------------|
| **Cash Flow** | forecast, variance_analysis, liquidity_assessment | Cash flow prediction, working capital optimization |
| **Risk** | identify_risks, quantify_risk, stress_test | Risk identification, VaR analysis, mitigation |
| **Compliance** | check_compliance, gap_analysis, audit_preparation | SOX, GAAP, AML/KYC compliance checking |
| **Treasury** | cash_position, fx_analysis, debt_management | FX exposure, cash positioning, investment analysis |

All agents run through the 6-layer resilience stack before reaching Bedrock.

---

## Chaos Engineering

The Chaos Engine runs **9 failure scenarios** every 6 hours:

1. **LLM Provider Outage** — All models return 503 errors
2. **Latency Spike** — 10-15 second model responses
3. **Rate Limiting** — ThrottlingException (429)
4. **Context Overflow** — Token limit exceeded
5. **Network Partition** — Connection failures
6. **Circuit Breaker Cascade** — All breakers open simultaneously
7. **Cache Invalidation** — TTL expiry and fresh invocation
8. **Graceful Degradation** — Progressive L0→L3 levels
9. **Multi-Model Failover** — Primary fails, tertiary succeeds

Results are persisted in DynamoDB and reported to CloudWatch.

---

## Documentation

- **[Architecture](docs/ARCHITECTURE.md)** — Full system architecture with ASCII diagrams
- **[Runbook](docs/RUNBOOK.md)** — Operational procedures for deployment, monitoring, incidents
- **[Well-Architected](docs/WELL_ARCHITECTED.md)** — Self-assessment across all 6 pillars
- **[Security](docs/SECURITY.md)** — Security narrative with threat model

---

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Follow the CHP state machine ([.chp/STATE_MACHINE.md](.chp/STATE_MACHINE.md))
4. Ensure all FTR compliance checks pass
5. Submit a pull request

All changes must maintain FTR compliance. The CHP state machine enforces
peer review, FTR verification, and rollback capability for every change.
