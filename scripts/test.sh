#!/bin/bash

# Test runner script for checksum validation Lambda functions

set -e

echo "🧪 Running Lambda Function Tests"
echo "================================"

# Change to project root
cd "$(dirname "$0")/.."

# Clean up any existing __pycache__ directories
echo "🧹 Cleaning up cache files..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true

# Set PYTHONPATH to include Lambda function source directories
export PYTHONPATH="${PWD}/lambda_functions/checksum_initiator/src:${PWD}/lambda_functions/checksum_results_processor/src:${PYTHONPATH}"

echo ""
echo "📦 Installing test dependencies..."

# Use uv if available, otherwise fall back to pip3
if command -v uv >/dev/null 2>&1; then
    echo "Using uv for fast dependency installation..."
    uv pip install pytest pytest-cov "moto[all]" boto3 || {
        echo "❌ Failed to install test dependencies with uv"
        echo "💡 Try: uv pip install -r requirements-test.txt"
        exit 1
    }
else
    echo "Using pip3 for dependency installation..."
    pip3 install -q pytest pytest-cov moto[all] boto3 || {
        echo "❌ Failed to install test dependencies with pip3"
        echo "💡 Try: pip3 install -r requirements-test.txt"
        exit 1
    }
fi

echo ""
echo "🔍 Running unit tests..."
echo "------------------------"

# Determine Python command (uv run if available, otherwise python3)
if command -v uv >/dev/null 2>&1; then
    PYTHON_CMD="uv run python"
else
    PYTHON_CMD="python3"
fi

# Run individual test files with better error handling
echo "Testing checksum initiator..."
$PYTHON_CMD -m pytest tests/test_checksum_initiator.py -v --tb=short || {
    echo "❌ Checksum initiator tests failed"
    exit 1
}

echo ""
echo "Testing checksum results processor..."
$PYTHON_CMD -m pytest tests/test_checksum_results_processor.py -v --tb=short || {
    echo "❌ Checksum results processor tests failed"
    exit 1
}

echo ""
echo "Testing object tagger..."
$PYTHON_CMD -m pytest tests/test_object_tagger.py -v --tb=short || {
    echo "❌ Object tagger tests failed"
    exit 1
}

echo ""
echo "Testing integration..."
$PYTHON_CMD -m pytest tests/test_integration.py -v --tb=short || {
    echo "❌ Integration tests failed"
    exit 1
}

echo ""
echo "📊 Running tests with coverage..."
echo "--------------------------------"

# Run coverage for checksum initiator
echo "Coverage for checksum initiator..."
PYTHONPATH="lambda_functions/checksum_initiator/src" $PYTHON_CMD -m pytest tests/test_checksum_initiator.py \
    --cov=lambda_function \
    --cov-report=term-missing \
    --cov-report=html:htmlcov \
    --tb=short \
    -v || {
    echo "❌ Checksum initiator coverage failed"
    exit 1
}

# Run coverage for checksum results processor  
echo "Coverage for checksum results processor..."
PYTHONPATH="lambda_functions/checksum_results_processor/src" $PYTHON_CMD -m pytest tests/test_checksum_results_processor.py \
    --cov=lambda_function \
    --cov-report=term-missing \
    --cov-append \
    --tb=short \
    -v || {
    echo "❌ Checksum results processor coverage failed"
    exit 1
}

# Run coverage for object tagger
echo "Coverage for object tagger..."
PYTHONPATH="lambda_functions/object_tagger/src" $PYTHON_CMD -m pytest tests/test_object_tagger.py \
    --cov=lambda_function \
    --cov-report=term-missing \
    --cov-append \
    --tb=short \
    -v || {
    echo "❌ Object tagger coverage failed"
    exit 1
}

# Run integration tests without coverage (they use different import mechanism)
echo "Running integration tests..."
$PYTHON_CMD -m pytest tests/test_integration.py \
    --tb=short \
    -v || {
    echo "❌ Integration coverage failed"
    exit 1
}

echo ""
echo "✅ Test run complete!"
echo "📈 Coverage report generated in htmlcov/index.html"
echo ""
echo "📋 Test Summary:"
echo "- Checksum Initiator: ✅"
echo "- Results Processor: ✅" 
echo "- Object Tagger: ✅"
echo "- Integration Tests: ✅"
echo "- Coverage Report: ✅"
