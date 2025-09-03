# Makefile for S3 Batch Checksum Validation

.PHONY: help install test clean deploy deploy-prod setup security-scan

# Default target
help:
	@echo "S3 Batch Checksum Validation - Available Commands"
	@echo "================================================="
	@echo ""
	@echo "Development:"
	@echo "  setup         - Set up development environment"
	@echo "  install       - Install all dependencies"
	@echo "  test          - Run all tests with coverage"
	@echo "  clean         - Clean up build artifacts and cache"
	@echo ""
	@echo "Security:"
	@echo "  security-scan     - Run CDK-Nag security analysis (requires Docker)"
	@echo ""
	@echo "Deployment:"
	@echo "  deploy        - Deploy to development environment"
	@echo "  deploy-prod   - Deploy to production environment"
	@echo ""

# Development setup
setup:
	@echo "🚀 Setting up development environment..."
	./scripts/setup-dev.sh

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	@if command -v uv >/dev/null 2>&1; then \
		echo "Using uv for fast dependency installation..."; \
		uv pip install -r requirements-test.txt; \
		uv pip install -r requirements-dev.txt; \
	else \
		echo "Using pip3 for dependency installation..."; \
		pip3 install -r requirements-test.txt; \
		pip3 install -r requirements-dev.txt; \
	fi

# Run tests
test:
	@echo "🧪 Running tests..."
	./scripts/test.sh

# Run security analysis
security-scan:
	@echo "🔍 Running CDK-Nag security analysis..."
	./scripts/run-cdk-nag.sh

# Clean build artifacts
clean:
	@echo "🧹 Cleaning up..."
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/
	rm -rf .uv_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type f -name ".coverage.*" -delete 2>/dev/null || true

# Deploy infrastructure
deploy:
	@echo "🚀 Deploying to development environment..."
	./scripts/deploy.sh dev

# Deploy to production
deploy-prod:
	@echo "🚀 Deploying to production environment..."
	./scripts/deploy.sh prod

# Quick development cycle
dev: test
	@echo "🔄 Development cycle completed!"

# CI/CD pipeline simulation
ci: clean install test
	@echo "🎯 CI pipeline simulation completed!"
