#!/usr/bin/env bash
# ============================================================================
# Strata CFO Resilience Matrix — Deployment Script
# ============================================================================
# Deploys the full 6-layer AI resilience system to AWS using SAM CLI.
#
# FTR Compliance:
# - All infrastructure defined in template.yaml (100% IaC)
# - No manual console steps required
# - Parameters validated before deployment
# - Clean deployment with rollback capability
#
# Prerequisites:
#   1. AWS CLI v2 configured with appropriate credentials
#   2. SAM CLI installed (pip install aws-sam-cli)
#   3. Python 3.12+ with pip
#   4. Appropriate IAM permissions for CloudFormation stack creation
#
# Usage:
#   ./scripts/deploy.sh [environment]
#   ./scripts/deploy.sh production
#   ./scripts/deploy.sh staging
#   ./scripts/deploy.sh development
# ============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENVIRONMENT="${1:-production}"
STACK_NAME="strata-cfo-${ENVIRONMENT}"
REGION="${AWS_REGION:-us-east-1}"
SAM_CONFIG="${PROJECT_DIR}/samconfig.toml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Install from https://aws.amazon.com/cli/"
        exit 1
    fi

    # Check SAM CLI
    if ! command -v sam &> /dev/null; then
        log_error "SAM CLI not found. Install with: pip install aws-sam-cli"
        exit 1
    fi

    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found. Requires Python 3.12+"
        exit 1
    fi

    # Verify AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Run 'aws configure' first."
        exit 1
    fi

    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    log_info "AWS Account: ${ACCOUNT_ID}"
    log_info "Region: ${REGION}"
    log_info "Environment: ${ENVIRONMENT}"
    log_info "Stack Name: ${STACK_NAME}"

    log_success "All prerequisites met"
}

validate_template() {
    log_info "Validating SAM template..."
    cd "${PROJECT_DIR}"

    sam validate \
        --template-file template.yaml \
        --region "${REGION}"

    log_success "Template validation passed"
}

install_dependencies() {
    log_info "Installing Python dependencies..."
    cd "${PROJECT_DIR}"

    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt -q --target ./lambda/layers/python
        log_success "Dependencies installed"
    else
        log_warning "No requirements.txt found"
    fi
}

build_sam() {
    log_info "Building SAM application..."
    cd "${PROJECT_DIR}"

    sam build \
        --template-file template.yaml \
        --region "${REGION}" \
        --build-dir .sam/build \
        --cached

    log_success "SAM build completed"
}

deploy_stack() {
    log_info "Deploying stack: ${STACK_NAME}..."
    cd "${PROJECT_DIR}"

    # Configuration based on environment
    case "${ENVIRONMENT}" in
        production)
            CHAOS_SCHEDULE="cron(0 */6 * * ? *)"
            PROVISIONED=5
            ;;
        staging)
            CHAOS_SCHEDULE="cron(0 0 * * ? *)"
            PROVISIONED=2
            ;;
        development)
            CHAOS_SCHEDULE="cron(0 8 * * ? *)"
            PROVISIONED=0
            ;;
        *)
            CHAOS_SCHEDULE="cron(0 */6 * * ? *)"
            PROVISIONED=0
            ;;
    esac

    sam deploy \
        --template-file .sam/build/template.yaml \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" \
        --config-file "${SAM_CONFIG}" \
        --no-confirm-changeset \
        --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
        --parameter-overrides \
            "Environment=${ENVIRONMENT}" \
            "ChaosSchedule=${CHAOS_SCHEDULE}" \
        --tags \
            Project=StrataCFO \
            Environment="${ENVIRONMENT}" \
            FTR=Compliant \
            ManagedBy=SAM

    log_success "Deployment completed successfully"
}

post_deploy() {
    log_info "Running post-deployment checks..."

    # Get stack outputs
    log_info "Stack outputs:"
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs" \
        --output table

    # Verify Lambda functions are deployed
    log_info "Verifying Lambda functions..."
    FUNCTIONS=("curate" "finetune" "gateway" "resilience" "chaos" "agents")
    for func in "${FUNCTIONS[@]}"; do
        FUNC_NAME="strata-${func}-${ENVIRONMENT}"
        if aws lambda get-function --function-name "${FUNC_NAME}" --region "${REGION}" &> /dev/null; then
            log_success "  ✓ ${FUNC_NAME}"
        else
            log_warning "  ✗ ${FUNC_NAME} not found"
        fi
    done

    # Verify DynamoDB tables
    log_info "Verifying DynamoDB tables..."
    TABLES=("resilience-metrics" "circuit-breakers" "chaos-results" "fine-tuning-jobs")
    for table_prefix in "${TABLES[@]}"; do
        TABLE_NAME="strata-${table_prefix}-${ENVIRONMENT}"
        if aws dynamodb describe-table --table-name "${TABLE_NAME}" --region "${REGION}" &> /dev/null; then
            log_success "  ✓ ${TABLE_NAME}"
        else
            log_warning "  ✗ ${TABLE_NAME} not found"
        fi
    done

    # Verify S3 buckets
    log_info "Verifying S3 buckets..."
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    BUCKETS=("curated-data" "model-artifacts" "resilience-logs")
    for bucket_prefix in "${BUCKETS[@]}"; do
        BUCKET_NAME="strata-${bucket_prefix}-${ACCOUNT_ID}-${ENVIRONMENT}"
        if aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" &> /dev/null; then
            log_success "  ✓ ${BUCKET_NAME}"
        else
            log_warning "  ✗ ${BUCKET_NAME} not found"
        fi
    done
}

print_summary() {
    echo ""
    echo "============================================"
    echo -e "${GREEN}Strata CFO Resilience Matrix${NC}"
    echo -e "Deployment: ${GREEN}SUCCESS${NC}"
    echo "============================================"
    echo ""
    echo "Environment: ${ENVIRONMENT}"
    echo "Stack Name:   ${STACK_NAME}"
    echo "Region:       ${REGION}"
    echo ""
    echo "Next Steps:"
    echo "  1. Configure Cognito users: aws cognito-idp admin-create-user ..."
    echo "  2. Test API Gateway: sam local invoke ..."
    echo "  3. Trigger chaos engine: aws lambda invoke ..."
    echo "  4. Monitor CloudWatch: https://${REGION}.console.aws.amazon.com/cloudwatch"
    echo ""
    echo "Documentation: docs/RUNBOOK.md"
}

# ---------------------------------------------------------------------------
# Main Deployment Flow
# ---------------------------------------------------------------------------
main() {
    echo ""
    echo "============================================"
    echo "Strata CFO Resilience Matrix — Deployment"
    echo "============================================"
    echo ""

    check_prerequisites
    validate_template
    install_dependencies
    build_sam
    deploy_stack
    post_deploy
    print_summary
}

main "$@"
