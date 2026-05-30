# Strata CFO Resilience Matrix — Security Narrative

## Overview

This document provides a comprehensive security narrative for the Strata CFO Resilience
Matrix, addressing all FTR (Foundational Technical Review) security requirements and
AWS Well-Architected Security Pillar best practices.

---

## 1. Encryption at Rest

### 1.1 KMS Customer Master Key (CMK)

All data stores use a customer-managed KMS key (CMK), not default AWS-managed keys.
This provides full control over key rotation, access policies, and audit trail.

**KMS Key Configuration:**
- Key ID: `alias/strata-cfo-production`
- Rotation: Automatic (365-day period)
- Policy: Restricted to specific AWS services (Lambda, DynamoDB, S3, CloudWatch Logs, Secrets Manager)

**Encrypted Resources:**

| Resource | Encryption Method | Key |
|----------|------------------|-----|
| S3 (curated-data) | SSE-KMS | `alias/strata-cfo-production` |
| S3 (model-artifacts) | SSE-KMS | `alias/strata-cfo-production` |
| S3 (resilience-logs) | SSE-KMS | `alias/strata-cfo-production` |
| S3 (cloudtrail) | SSE-KMS | `alias/strata-cfo-production` |
| DynamoDB (all tables) | AWS managed KMS | CMK specified in SSESpecification |
| Secrets Manager | KMS CMK | `alias/strata-cfo-production` |
| CloudTrail logs | KMS CMK | `alias/strata-cfo-production` |
| Semantic Cache (S3) | SSE-KMS | `alias/strata-cfo-production` |

### 1.2 S3 Encryption Enforcement

All S3 buckets enforce KMS encryption via bucket policies:

```json
{
  "Sid": "EnforceKMSKey",
  "Effect": "Deny",
  "Principal": "*",
  "Action": "s3:PutObject",
  "Resource": "${BucketArn}/*",
  "Condition": {
    "StringNotEquals": {
      "s3:x-amz-server-side-encryption": "aws:kms"
    }
  }
}
```

This prevents any unencrypted data from being stored, even by authorized users.

### 1.3 S3 TLS Enforcement

All S3 buckets deny non-TLS access:

```json
{
  "Sid": "EnforceSSLEnly",
  "Effect": "Deny",
  "Principal": "*",
  "Action": "s3:*",
  "Resource": "${BucketArn}/*",
  "Condition": {
    "Bool": {
      "aws:SecureTransport": false
    }
  }
}
```

---

## 2. Encryption in Transit

### 2.1 API Gateway TLS

- All API Gateway endpoints use HTTPS (TLS 1.2)
- CORS configuration restricts allowed origins
- Request/response validation via API Gateway models

### 2.2 VPC-Level Isolation

- Lambda functions run in private subnets (no direct internet access)
- VPC Gateway endpoints for S3 and DynamoDB keep traffic within AWS network
- No data traverses the public internet for internal service calls

### 2.3 Bedrock Runtime

- Bedrock invocations use HTTPS via boto3
- VPC endpoints not required (Bedrock is a public service accessed via NAT Gateway)

---

## 3. Identity and Access Management

### 3.1 Least-Privilege IAM

Every Lambda function has a dedicated IAM role with explicit, least-privilege policies.

**Example: Gateway Function Role**
```json
{
  "PolicyName": "BedrockRuntimeAccess",
  "Effect": "Allow",
  "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
  "Resource": [
    "arn:aws:bedrock:${Region}::foundation-model/anthic.claude-3-5-sonnet-20241022-v1:0",
    "arn:aws:bedrock:${Region}::foundation-model/amazon.titan-text-premier-v1:0",
    "arn:aws:bedrock:${Region}::foundation-model/meta.llama3-70b-instruct-v1:0"
  ]
}
```

**Key IAM Decisions:**
- No wildcard `Resource: "*"` on data plane actions
- Specific S3 bucket ARNs (not `arn:aws:s3:::*`)
- Specific DynamoDB table ARNs (not `arn:aws:dynamodb:*:table/*`)
- KMS key ARN specified (not `*`)
- Secrets Manager ARN specified per function

### 3.2 ABAC Multi-Tenant Isolation

Cognito User Pool injects `tenant_id` into JWT claims via pre-token generation Lambda.
IAM policies enforce tenant isolation:

```json
{
  "Sid": "ABACDynamoDBAccess",
  "Effect": "Allow",
  "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:Query"],
  "Resource": "${MetricsTableArn}",
  "Condition": {
    "StringEquals": {
      "dynamodb:LeadingKeys": "${aws:PrincipalTag/tenant_id}"
    }
  }
}
```

S3 paths include tenant prefix:
```
tenants/{tenant_id}/data.json
```

### 3.3 Cognito Authentication

- User Pool with MFA (optional but configurable)
- Password policy: 12+ characters, uppercase, lowercase, numbers, symbols
- `tenant_id` as immutable custom attribute
- Pre-token generation Lambda injects tenant claims into JWT
- Access tokens: 60-minute validity
- Refresh tokens: 30-day validity

---

## 4. Network Security

### 4.1 Custom VPC Architecture

```
VPC: 10.0.0.0/16
├── Public Subnet A (10.0.0.0/24) — NAT Gateway A
├── Public Subnet B (10.0.1.0/24) — NAT Gateway B
├── Private Subnet A (10.0.2.0/24) — Lambda functions
└── Private Subnet B (10.0.3.0/24) — Lambda functions
```

### 4.2 Security Groups

Lambda Security Group:
- Egress: TCP/443 to 0.0.0.0/0 (for AWS service endpoints)
- No ingress (Lambda functions don't accept incoming connections)

### 4.3 VPC Endpoints

- **S3 Gateway Endpoint**: Routes S3 traffic within VPC (no NAT Gateway traversal)
- **DynamoDB Gateway Endpoint**: Routes DynamoDB traffic within VPC

### 4.4 NAT Gateway

- One per AZ for high availability
- Used for Bedrock Runtime calls (public service)
- Outbound only (Lambda → Bedrock)

---

## 5. Secrets Management

### 5.1 Secrets Manager

All credentials and configuration are stored in Secrets Manager:
- `strata/bedrock-config-production`: Bedrock model IDs, parameters
- `strata/opensearch-config-production`: OpenSearch connection details

**Retrieval Pattern:**
```python
client = boto3.client("secretsmanager")
response = client.get_secret_value(SecretId=BEDROCK_SECRET_ARN)
config = json.loads(response["SecretString"])
```

### 5.2 Zero Secrets in Code

- No hardcoded API keys, passwords, or tokens
- No credentials in environment variables (only secret ARN references)
- No credentials in SSM parameters (using Secrets Manager exclusively)
- All Lambda code retrieves secrets at runtime from Secrets Manager

---

## 6. Auditing and Logging

### 6.1 AWS CloudTrail

- Enabled for all control plane API calls
- Log file validation enabled (tamper detection)
- KMS-encrypted log files
- Delivered to dedicated S3 bucket with lifecycle policy
- CloudWatch Logs integration for real-time monitoring

### 6.2 VPC Flow Logs

- Enabled for entire VPC
- Traffic type: ALL (accepted + rejected)
- Delivered to CloudWatch Logs with 30-day retention
- IAM role with least-privilege access

### 6.3 CloudWatch Logs

- All 6 Lambda functions log to dedicated log groups
- API Gateway access logging (90-day retention)
- CloudTrail log group (90-day retention)
- VPC Flow Logs (30-day retention)
- Structured JSON format for CloudWatch Insights queries

---

## 7. Threat Model

| Threat | Mitigation | Control |
|--------|-----------|---------|
| Unauthorized API access | Cognito authorizer on all routes | API Gateway + Cognito |
| Tenant data cross-access | ABAC with tenant_id in partition keys | DynamoDB + IAM policy |
| Data at rest exposure | KMS CMK encryption | S3 + DynamoDB + Secrets Manager |
| Data in transit interception | TLS 1.2 everywhere | API Gateway + VPC endpoints |
| LLM output manipulation | Bedrock guardrails integration | Bedrock Runtime config |
| Credential exposure | Secrets Manager (zero in code) | Runtime retrieval only |
| Service outage | 6-layer resilience stack | Circuit breaker + fallback |
| Insider threat | Least-privilege IAM, CloudTrail audit | Per-function roles |
| DoS attack | API Gateway throttling + WAF-ready | Rate limiting on API |
| S3 data leakage | Block public access + bucket policies | S3 configuration |

---

## 8. Compliance Alignment

| Framework | Requirement | Implementation |
|-----------|------------|----------------|
| FTR | 100% IaC | SAM template.yaml |
| FTR | Custom VPC | 2 AZs, public + private subnets |
| FTR | KMS CMK | All data stores encrypted with CMK |
| FTR | Secrets Manager | Bedrock config, OpenSearch config |
| FTR | Least-privilege IAM | Per-function roles, explicit ARNs |
| FTR | CloudWatch logs/metrics/alarms | 6 log groups, 5 alarms, custom metrics |
| FTR | Multi-AZ | 2 AZs for VPC, Lambda, NAT Gateway |
| FTR | VPC Flow Logs + CloudTrail | Both enabled with log validation |
| SOC 2 | Access control | ABAC, least-privilege, audit trail |
| SOC 2 | Encryption | KMS CMK at rest, TLS in transit |
| SOC 2 | Monitoring | CloudWatch + X-Ray + CloudTrail |
| GDPR | Data isolation | Tenant-scoped partition keys |
| GDPR | Data lifecycle | TTL, lifecycle policies, deletion APIs |

---

## 9. Security Review Summary

The Strata CFO Resilience Matrix implements defense-in-depth security:
1. **Perimeter**: API Gateway with Cognito auth, VPC with private subnets
2. **Identity**: Least-privilege IAM, ABAC tenant isolation, MFA
3. **Data**: KMS CMK encryption at rest, TLS in transit
4. **Application**: Input validation, circuit breakers, rate limiting
5. **Monitoring**: CloudTrail, VPC Flow Logs, CloudWatch, X-Ray

All FTR security requirements are met with production-grade implementations.
