# Strata CFO Resilience Matrix — Operational Runbook

## Overview

This runbook provides operational procedures for managing the Strata CFO Resilience
Matrix in production. Follow these procedures for deployment, monitoring, incident
response, and maintenance.

## Table of Contents

1. [Deployment](#1-deployment)
2. [Health Checks](#2-health-checks)
3. [Monitoring](#3-monitoring)
4. [Incident Response](#4-incident-response)
5. [Rollback](#5-rollback)
6. [Maintenance](#6-maintenance)

---

## 1. Deployment

### 1.1 Initial Deployment

```bash
# Clone the repository
git clone <repo-url> strata-cfo
cd strata-cfo

# Install dependencies
pip install -r requirements.txt
pip install aws-sam-cli

# Validate template
sam validate --template-file template.yaml

# Build
sam build --template-file template.yaml

# Deploy to production
sam deploy \
  --template-file .sam/build/template.yaml \
  --stack-name strata-cfo-production \
  --region us-east-1 \
  --config-file samconfig.toml \
  --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
  --parameter-overrides \
    Environment=production \
    ChaosSchedule="cron(0 */6 * * ? *)"

# Verify deployment
./scripts/deploy.sh production
```

### 1.2 Environment Configuration

| Parameter | Production | Staging | Development |
|-----------|-----------|---------|-------------|
| Chaos Schedule | Every 6 hours | Daily | Daily at 8am |
| Provisioned Concurrency | 5 | 2 | 0 |
| Log Retention | 30 days | 14 days | 7 days |
| KMS Key Rotation | Enabled | Enabled | Enabled |

### 1.3 Post-Deployment Verification

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name strata-cfo-production \
  --query "Stacks[0].StackStatus"

# Verify Lambda functions
aws lambda list-functions \
  --query "Functions[?starts_with(FunctionName, 'strata-')].FunctionName"

# Check API Gateway endpoint
aws apigateway get-rest-apis \
  --query "Items[?name=='strata-cfo-production'].endpointConfiguration"

# Verify DynamoDB tables
aws dynamodb list-tables \
  --query "TableNames[?contains(@, 'strata-')]"
```

---

## 2. Health Checks

### 2.1 Automated Health Checks

| Check | Method | Frequency | Alert Threshold |
|-------|--------|-----------|-----------------|
| Lambda Function Health | CloudWatch Errors metric | 5min | > 5 errors |
| Gateway Latency | CloudWatch Duration P99 | 5min | > 5000ms |
| Circuit Breaker State | Custom metric (CircuitBreakerState) | 1min | State = OPEN |
| Chaos Pass Rate | Custom metric (ChaosPassRate) | 1h | < 90% |
| DynamoDB Throttles | CloudWatch metric | 5min | > 0 |
| S3 Access Errors | CloudWatch metric | 5min | > 0 |

### 2.2 Manual Health Check

```bash
# Test API Gateway (requires Cognito token)
curl -X POST https://<api-id>.execute-api.us-east-1.amazonaws.com/production/agents \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "agent_type": "cash_flow",
    "action": "forecast",
    "parameters": {"period": "Q4_2024"},
    "tenant_id": "test-tenant"
  }'

# Invoke gateway Lambda directly (for debugging)
aws lambda invoke \
  --function-name strata-gateway-production \
  --payload '{"prompt": "health check", "tenant_id": "system"}' \
  response.json

# Check circuit breaker state
aws dynamodb get-item \
  --table-name strata-circuit-breakers-production \
  --key '{"breaker_id": {"S": "gateway:anthic.claude-3-5-sonnet-20241022-v1:0"}}'
```

### 2.3 Chaos Test Execution

```bash
# Manual chaos test trigger
curl -X POST https://<api-id>.execute-api.us-east-1.amazonaws.com/production/chaos/trigger \
  -H "Authorization: Bearer <jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "scenarios": ["llm_outage", "rate_limiting"],
    "trigger_type": "manual"
  }'

# Check chaos results
aws dynamodb scan \
  --table-name strata-chaos-results-production \
  --filter-expression "attribute_exists(pass_rate)"
```

---

## 3. Monitoring

### 3.1 CloudWatch Dashboard

**Recommended Dashboard Widgets:**

1. **Gateway Health**: Error rate, latency P50/P99, invocation count
2. **Circuit Breaker States**: Per-model circuit breaker states (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
3. **Model Fallback Rate**: How often fallback models are used
4. **Chaos Test Results**: Pass/fail rate, latest run details
5. **Cost Tracking**: Bedrock invocation costs per model
6. **Cache Performance**: Hit rate, miss rate, cache latency

### 3.2 CloudWatch Insights Queries

```sql
-- High error rate in last 5 minutes
fields @timestamp, @message, error_type
| filter @message like /ERROR/
| stats count(*) by error_type, bin(5m)

-- Slow gateway invocations
fields @timestamp, request_id, total_latency_ms
| filter total_latency_ms > 5000
| sort total_latency_ms desc
| limit 20

-- Circuit breaker events
fields @timestamp, breaker, action
| filter @message like /circuit.breaker/
| stats count(*) by breaker, bin(1h)

-- Chaos test results
fields @timestamp, test_name, result, duration_ms
| filter @message like /chaos.test/
| stats avg(duration_ms), count(*) by test_name
```

### 3.3 X-Ray Tracing

Access the X-Ray console to:
- View service map showing all 6 Lambda layers
- Trace individual requests end-to-end
- Identify latency bottlenecks in the resilience stack
- Review error traces with full context

### 3.4 SNS Alert Configuration

Alarms notify via `strata-alerts-production` SNS topic. Subscribe:
- Email: `ops@company.com`
- Slack: Via SNS → Lambda → Slack webhook integration
- PagerDuty: Via SNS → PagerDuty integration

---

## 4. Incident Response

### 4.1 Circuit Breaker Open

**Symptom**: CloudWatch alarm `strata-circuit-breaker-open-production`

**Diagnosis**:
```bash
# Check which model's circuit is open
aws dynamodb scan \
  --table-name strata-circuit-breakers-production \
  --filter-expression "#s = :open" \
  --expression-attribute-names '{"#s": "state"}' \
  --expression-attribute-values '{":open": {"N": "2"}}'
```

**Response**:
1. Check Bedrock service health: https://health.aws.amazon.com/health/status
2. If Bedrock outage: No action needed, fallback will activate
3. If model-specific issue:
   ```bash
   # Force close circuit breaker to test recovery
   aws dynamodb update-item \
     --table-name strata-circuit-breakers-production \
     --key '{"breaker_id": {"S": "gateway:<model-id>"}}' \
     --update-expression "SET #s = :closed, failure_count = :zero" \
     --expression-attribute-names '{"#s": "state"}' \
     --expression-attribute-values '{":closed": {"N": "0"}, ":zero": {"N": "0"}}'
   ```
4. Monitor for 15 minutes for recurrence

### 4.2 All Models Exhausted

**Symptom**: Agent returns `[System Notice: Unable to generate full response...]`

**Response**:
1. Check Bedrock availability: https://health.aws.amazon.com/health/status
2. Verify network connectivity (VPC endpoints):
   ```bash
   aws ec2 describe-vpc-endpoints \
     --filters Name=service-name,Values=com.amazonaws.bedrock
   ```
3. If VPC endpoint issue, check NAT Gateway status
4. If Bedrock regional outage, the system will recover automatically
5. Manual intervention: Reset all circuit breakers

### 4.3 High Latency

**Symptom**: CloudWatch alarm `strata-gateway-latency-production`

**Response**:
1. Check Bedrock model latency in X-Ray traces
2. Check if degradation is activating (metrics → DegradationLevel)
3. If sustained > 10s: Check Bedrock service health
4. If transient: Normal — resilience stack handles via timeout + fallback

### 4.4 Data Curation Failures

**Symptom**: No curated datasets in S3 for > 4 hours

**Response**:
1. Check raw-logs/ prefix has new data
2. Check Lambda CloudWatch logs for curation errors
3. Check DynamoDB write capacity (PAY_PER_REQUEST should auto-scale)
4. Manual trigger if needed:
   ```bash
   aws lambda invoke \
     --function-name strata-curate-production \
     --invocation-type Event
   ```

---

## 5. Rollback

### 5.1 Automatic Rollback

The CHP state machine triggers automatic rollback if:
- CloudWatch alarm fires within 1 hour of deployment
- Lambda error rate exceeds 10% post-deployment
- Any FTR compliance check fails

### 5.2 Manual Rollback

```bash
# Check deployment history
aws cloudformation describe-stack-events \
  --stack-name strata-cfo-production \
  --max-items 20

# Rollback to previous version
aws cloudformation deploy \
  --template-file template-previous.yaml \
  --stack-name strata-cfo-production \
  --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
  --no-fail-on-empty-changeset

# Force circuit breaker reset after rollback
aws dynamodb scan \
  --table-name strata-circuit-breakers-production \
  --attributes-to-get breaker_id \
  --projection-expression "breaker_id"
```

---

## 6. Maintenance

### 6.1 Regular Maintenance Schedule

| Task | Frequency | Command |
|------|-----------|---------|
| KMS key rotation check | Monthly | Verify auto-rotation is active |
| CloudWatch log cleanup | Quarterly | Review retention policies |
| S3 lifecycle review | Quarterly | Verify archival rules |
| DynamoDB TTL review | Monthly | Check expires_at values |
| Chaos test review | Weekly | Review pass/fail trends |
| Secrets rotation | Quarterly | Rotate Bedrock config secrets |
| IAM policy audit | Monthly | Verify least-privilege |

### 6.2 KMS Key Rotation

Keys are set to auto-rotate (365-day period). Verify:
```bash
aws kms describe-key --key-id alias/strata-cfo-production --query "KeyMetadata.Enabled"
```

### 6.3 Scaling Adjustments

To adjust provisioned concurrency:
```bash
aws lambda update-function-configuration \
  --function-name strata-gateway-production \
  --provisioned-concurrency-config ProvisionedConcurrentExecutions=10
```

To adjust circuit breaker thresholds, redeploy with new parameters:
```bash
sam deploy --parameter-overrides CircuitBreakerThreshold=10
```

### 6.4 Disaster Recovery

- **RPO**: ~5 minutes (DynamoDB PointInTimeRecovery)
- **RTO**: ~15 minutes (SAM redeploy to secondary region)
- **DR Region**: us-west-2 (deploy via samconfig.toml staging config)
- **Backup**: S3 versioning + DynamoDB PITR + CloudTrail logs
