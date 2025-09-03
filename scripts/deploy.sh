#!/bin/bash

# S3 Batch Checksum Deployment Script

set -e

# Configuration
ENVIRONMENT=${1:-dev}

echo "Deploying S3 Batch Checksum infrastructure..."
echo "Environment: $ENVIRONMENT"

# Get AWS account and region from current AWS CLI configuration
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=$(aws configure get region)

# Fallback to us-east-1 if no region is configured
if [ -z "$AWS_REGION" ]; then
    AWS_REGION="us-east-1"
    echo "No default region configured, using: $AWS_REGION"
fi

echo "Account: $AWS_ACCOUNT"
echo "Region: $AWS_REGION"

# Navigate to project root
cd "$(dirname "$0")/.."

# Install dependencies using uv
echo "Installing dependencies with uv..."
if ! command -v uv &> /dev/null; then
    echo "uv not found. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi

# Install CDK dependencies
echo "Installing CDK dependencies..."
uv pip install -e .[cdk]

# Navigate to infrastructure directory
cd infrastructure

# Bootstrap CDK (if not already done)
echo "Bootstrapping CDK..."
cdk bootstrap aws://$AWS_ACCOUNT/$AWS_REGION

# Deploy the stack
echo "Deploying CDK stack..."
cdk deploy S3BatchChecksumStack-$ENVIRONMENT \
    --context account=$AWS_ACCOUNT \
    --context region=$AWS_REGION \
    --context environment=$ENVIRONMENT \
    --require-approval never

echo "Deployment completed successfully!"
echo ""
echo "Next steps:"
echo "1. Upload a CSV manifest file to the created manifest bucket"
echo "2. Invoke the Lambda function manually or via API"
echo "3. Monitor the S3 batch job progress in the AWS Console"
