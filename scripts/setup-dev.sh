#!/bin/bash

# Development environment setup script

set -e

echo "🚀 Setting up S3 Batch Checksum Validation development environment"
echo "=================================================================="

# Change to project root
cd "$(dirname "$0")/.."

# Check if uv is available
if command -v uv >/dev/null 2>&1; then
    echo "✅ uv detected - using for fast dependency management"
    PACKAGE_MANAGER="uv"
    INSTALL_CMD="uv pip install"
    RUN_CMD="uv run"
else
    echo "⚠️  uv not found - using pip3 (consider installing uv for faster builds)"
    echo "💡 Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh"
    PACKAGE_MANAGER="pip3"
    INSTALL_CMD="pip3 install"
    RUN_CMD="python3"
fi

echo ""
echo "📦 Installing Python dependencies..."
echo "-----------------------------------"

# Install test dependencies
echo "Installing test dependencies..."
if [ "$PACKAGE_MANAGER" = "uv" ]; then
    uv pip install -r requirements-test.txt
else
    pip3 install -r requirements-test.txt
fi

# Install development dependencies
echo "Installing development dependencies..."
if [ "$PACKAGE_MANAGER" = "uv" ]; then
    uv pip install -r requirements-dev.txt
else
    pip3 install -r requirements-dev.txt
fi

echo ""
echo "🔧 Checking system dependencies..."
echo "---------------------------------"

# Check Python version
if [ "$PACKAGE_MANAGER" = "uv" ]; then
    PYTHON_VERSION=$(uv run python --version 2>&1 | cut -d' ' -f2)
    PYTHON_CHECK_CMD="uv run python"
else
    PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2)
    PYTHON_CHECK_CMD="python3"
fi
echo "Python version: $PYTHON_VERSION"

if ! $PYTHON_CHECK_CMD -c "import sys; exit(0 if sys.version_info >= (3, 11) else 1)" 2>/dev/null; then
    echo "⚠️  Python 3.11+ recommended for best compatibility"
fi

# Check AWS CLI
if command -v aws >/dev/null 2>&1; then
    echo "✅ AWS CLI found: $(aws --version)"
    
    # Check AWS configuration
    if aws sts get-caller-identity >/dev/null 2>&1; then
        echo "✅ AWS credentials configured"
        ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        REGION=$(aws configure get region || echo "not set")
        echo "   Account: $ACCOUNT_ID"
        echo "   Region: $REGION"
    else
        echo "⚠️  AWS credentials not configured"
        echo "💡 Run: aws configure"
    fi
else
    echo "❌ AWS CLI not found"
    echo "💡 Install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
fi

# Check CDK
if command -v cdk >/dev/null 2>&1; then
    echo "✅ AWS CDK found: $(cdk --version)"
else
    echo "❌ AWS CDK not found"
    echo "💡 Install: npm install -g aws-cdk"
fi

# Check Node.js (required for CDK)
if command -v node >/dev/null 2>&1; then
    NODE_VERSION=$(node --version)
    echo "✅ Node.js found: $NODE_VERSION"
    
    # Check if version is 18+
    if ! node -e "process.exit(process.version.match(/^v(\d+)/)[1] >= 18 ? 0 : 1)"; then
        echo "⚠️  Node.js 18+ recommended for CDK"
    fi
else
    echo "❌ Node.js not found (required for CDK)"
    echo "💡 Install: https://nodejs.org/"
fi

echo ""
echo "🧪 Running quick test to verify setup..."
echo "---------------------------------------"

# Run a quick syntax check
if $PYTHON_CHECK_CMD -m py_compile lambda_functions/checksum_initiator/src/lambda_function.py 2>/dev/null; then
    echo "✅ Checksum initiator syntax check passed"
else
    echo "❌ Checksum initiator syntax check failed"
    exit 1
fi

if $PYTHON_CHECK_CMD -m py_compile lambda_functions/checksum_results_processor/src/lambda_function.py 2>/dev/null; then
    echo "✅ Checksum results processor syntax check passed"
else
    echo "❌ Checksum results processor syntax check failed"
    exit 1
fi

# Test import of key dependencies
if $PYTHON_CHECK_CMD -c "import boto3, pytest, moto; print('✅ Key dependencies importable')" 2>/dev/null; then
    echo "✅ Dependencies verification passed"
else
    echo "❌ Dependencies verification failed"
    exit 1
fi

echo ""
echo "📋 Setup Summary"
echo "==============="
echo "Package Manager: $PACKAGE_MANAGER"
if [ "$PACKAGE_MANAGER" = "uv" ]; then
    echo "Python Command: uv run python"
    echo "Install Command: uv pip install"
else
    echo "Python Command: python3"
    echo "Install Command: pip3 install"
fi

echo ""
echo "🎉 Development environment setup complete!"
echo ""
echo "Next steps:"
echo "  make test          # Run comprehensive tests"
echo "  make deploy        # Deploy to development environment"
echo "  ./scripts/test.sh  # Run tests directly"
echo ""

if [ "$PACKAGE_MANAGER" = "pip3" ]; then
    echo "💡 For faster dependency management, consider installing uv:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo ""
fi
