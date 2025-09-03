# Tests

Unit and integration tests for the S3 batch checksum validation system.

## Structure

```
tests/
├── unit/           # Unit tests for individual components
├── integration/    # Integration tests for full workflows
└── fixtures/       # Test data and mock responses
```

## Running Tests

```bash
# All tests
make test

# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# Specific test file
pytest tests/unit/test_checksum_initiator.py
```

## Test Requirements

- AWS credentials configured
- Test S3 bucket access
- DynamoDB local or test tables
- Lambda execution role permissions

## Writing Tests

- Use pytest fixtures for common setup
- Mock AWS services for unit tests
- Use real AWS resources for integration tests
- Clean up resources after tests
