#!/bin/bash

# CDK-Nag Security Analysis Script
# Runs static security analysis on CDK constructs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INFRASTRUCTURE_DIR="$PROJECT_ROOT/infrastructure"

echo "🔍 Running CDK-Nag Security Analysis..."
echo "Project: S3 Batch Checksum Validation"
echo "Infrastructure Directory: $INFRASTRUCTURE_DIR"
echo ""

# Check if CDK is installed
if ! command -v cdk &> /dev/null; then
    echo "❌ AWS CDK CLI not found. Please install it:"
    echo "npm install -g aws-cdk"
    exit 1
fi

# Check if Python dependencies are installed
if ! python3 -c "import cdk_nag" 2>/dev/null; then
    echo "❌ cdk-nag not found. Installing dependencies..."
    cd "$PROJECT_ROOT"
    if command -v uv &> /dev/null; then
        uv pip install -e ".[cdk]"
    else
        pip3 install -e ".[cdk]"
    fi
fi

# Change to infrastructure directory
cd "$INFRASTRUCTURE_DIR"

# Set environment variables for analysis
export CDK_NAG_ENABLED=true

# Run CDK synth with nag checks
echo "🔍 Synthesizing CDK stack with security analysis..."
echo ""

# Run for development environment
echo "📋 Analyzing Development Environment..."
if command -v uv &> /dev/null; then
    uv run cdk synth --context environment=dev --context account=123456789012 --context region=us-east-1
else
    cdk synth --context environment=dev --context account=123456789012 --context region=us-east-1
fi

echo ""
echo "📋 Analyzing Production Environment..."
if command -v uv &> /dev/null; then
    uv run cdk synth --context environment=prod --context account=123456789012 --context region=us-east-1
else
    cdk synth --context environment=prod --context account=123456789012 --context region=us-east-1
fi

echo ""
echo "✅ CDK-Nag analysis complete!"
echo ""
echo "📊 Review the output above for security findings."
echo "💡 Findings marked as 'SUPPRESSED' have been reviewed and accepted."
echo "⚠️  Any 'ERROR' or 'WARNING' findings should be addressed."
echo ""
echo "📖 For more information about specific rules, visit:"
echo "   https://github.com/cdklabs/cdk-nag/blob/main/RULES.md"
