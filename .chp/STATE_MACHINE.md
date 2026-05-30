# Strata CFO Resilience Matrix — CHP State Machine

## Overview

The Change Health Process (CHP) governs all modifications to the Strata CFO Resilience Matrix
codebase and infrastructure. This ensures FTR (Foundational Technical Review) compliance is
maintained across every change.

## State Machine

```
                    ┌──────────────┐
                    │   PROPOSED   │  ← Change Request Created
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  REVIEWED    │  ← Peer Review Complete
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  APPROVED    │  ← FTR Compliance Verified
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  DEPLOYING   │  ← SAM Deploy Initiated
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐  ┌──▼───────┐  ┌▼──────────────┐
       │  DEPLOYED   │  │  FAILED  │  │  ROLLED_BACK   │
       │  (success)  │  │  (error) │  │  (auto/manual) │
       └──────┬──────┘  └──┬───────┘  └──────────────┘
              │            │
       ┌──────▼──────┐  ┌──▼───────┐
       │  MONITORING │  │  FIXING  │  ← Fix applied → back to REVIEWED
       └──────┬──────┘  └──────────┘
              │
       ┌──────▼──────┐
       │  STABLE     │  ← Success criteria met
       └─────────────┘
```

## States

| State        | Description                                      | Entry Condition                          |
|--------------|--------------------------------------------------|------------------------------------------|
| PROPOSED     | Change request created, awaiting review         | New PR / change request                   |
| REVIEWED     | Peer review completed, FTR checklist started      | Code review approved                      |
| APPROVED     | FTR compliance verified, ready for deployment     | All FTR checks pass                       |
| DEPLOYING    | SAM deploy in progress                            | Deploy script executed                    |
| DEPLOYED     | Deployment succeeded, monitoring started          | SAM deploy returns success                |
| FAILED       | Deployment failed                                 | SAM deploy returns error                  |
| ROLLED_BACK  | Automatic or manual rollback                      | Post-deploy validation failed            |
| MONITORING   | Post-deploy health checks running                | Deployment succeeded, monitoring active  |
| STABLE       | Deployment confirmed stable                      | No incidents for 24h                     |
| FIXING       | Fix being applied to failed deployment           | Fix PR created                            |

## Transitions

| From      | To        | Trigger                    | Actor    |
|-----------|-----------|----------------------------|----------|
| PROPOSED  | REVIEWED  | Code review approved       | Reviewer |
| REVIEWED  | APPROVED  | FTR compliance confirmed    | Approver |
| REVIEWED  | PROPOSED  | Changes requested          | Reviewer |
| APPROVED  | DEPLOYING | Deploy initiated           | Pipeline |
| DEPLOYING | DEPLOYED  | SAM deploy success          | System   |
| DEPLOYING | FAILED    | SAM deploy failure          | System   |
| DEPLOYED  | MONITORING| Health checks started       | System   |
| DEPLOYED  | ROLLED_BACK| Validation failed           | System   |
| FAILED    | FIXING    | Fix PR created              | Developer|
| FIXING    | REVIEWED  | Fix submitted for review    | Developer|
| MONITORING| STABLE    | 24h no incidents             | System   |
| MONITORING| ROLLED_BACK| Incident detected           | System   |

## FTR Compliance Checklist (APPROVED gate)

- [ ] 100% IaC — No manual console changes
- [ ] Custom VPC with private subnets
- [ ] KMS CMK encryption on all data stores
- [ ] Secrets Manager — zero secrets in code
- [ ] Least-privilege IAM — no wildcard resources
- [ ] CloudWatch logs, metrics, and alarms configured
- [ ] Multi-AZ deployment
- [ ] VPC Flow Logs + CloudTrail enabled
- [ ] README updated with architecture diagram
- [ ] Runbook updated with operational procedures
- [ ] Well-Architected review completed

## Governance

- **Change Policy**: All changes must go through this state machine
- **Rollback Policy**: Automatic rollback if CloudWatch alarms trigger within 1h of deploy
- **Review Policy**: Minimum 1 peer review required before APPROVED
- **FTR Policy**: All FTR checklist items must pass before deployment
