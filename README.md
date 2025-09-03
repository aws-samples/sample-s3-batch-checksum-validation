# S3 Batch Checksum Validation

Automated checksum validation for S3 objects using batch operations. Generates and validates MD5/SHA256 checksums for media files during large-scale content ingest.

## Quick Start

### Deploy
```bash
make setup
make deploy
```

### Generate Test Data & Run Validation
```bash
# Generate test files
python scripts/s3-tools/synthetic-data/generate_synthetic_dataset.py my-bucket --max-size 10

# Create checksum payload
python scripts/s3-tools/batch-operations/s3_batch_input_generator.py s3://my-bucket/synthetic-data/ --output payload.json

# Invoke Lambda
aws lambda invoke --function-name checksum-initiator-dev --payload file://payload.json response.json
```

### Manual Invocation
```bash
aws lambda invoke \
  --function-name checksum-initiator-dev \
  --payload '{
    "bucket": "my-bucket",
    "keys": [
      {"key": "file1.txt"},
      {"key": "file2.txt", "version_id": "version123"}
    ]
  }' \
  response.json
```

## Architecture

- **Checksum Initiator Lambda**: Creates S3 batch operations jobs
- **Results Processor Lambda**: Processes batch job reports, stores checksums in DynamoDB
- **S3 Buckets**: Store manifests and reports
- **DynamoDB**: Stores checksum results with TTL

## Workflow

1. Lambda processes object lists → creates CSV manifests
2. S3 batch operations compute checksums (SHA256 + MD5)
3. Results processor extracts checksums → stores in DynamoDB
4. Checksums applied as S3 object tags

## Input Format

```json
{
  "bucket": "my-bucket",
  "keys": [
    {"key": "file.txt"},
    {"key": "file2.txt", "version_id": "abc123"},
    {"key": "file3.txt", "md5": "expected-hash", "sha256": "expected-hash"}
  ]
}
```

## Query Results

```bash
# Get checksum for object
aws dynamodb get-item \
  --table-name ChecksumResults-dev \
  --key '{"object_key": {"S": "bucket#path/file.txt#SHA256"}}'

# View object tags
aws s3api get-object-tagging --bucket my-bucket --key path/file.txt
```

## Development

```bash
# Run tests
make test

# Deploy to prod
make deploy-prod

# Monitor logs
aws logs tail /aws/lambda/checksum-initiator-dev --follow
```

## Prerequisites

- AWS CLI configured
- Python 3.11+
- AWS CDK CLI
- Node.js 18+
