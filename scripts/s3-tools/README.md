# S3 Tools

Utility tools for S3 batch checksum validation and synthetic test data generation.

## Structure

```
s3-tools/
├── batch-operations/           # S3 batch operations tools
│   ├── s3_batch_input_generator.py
│   └── s3_batch_input_generator
├── synthetic-data/             # Synthetic test data tools
│   ├── generate_synthetic_dataset.py
│   ├── setup_synthetic_data.sh
│   ├── validate_dataset.py
│   └── test_synthetic_integration.py
└── README.md
```

## Quick Start

### Generate Test Data
```bash
# Generate synthetic test files
python scripts/s3-tools/synthetic-data/generate_synthetic_dataset.py my-bucket --max-size 10

# Or use shell wrapper
export S3_STD_MV_TEST_BUCKET=my-bucket
./scripts/s3-tools/synthetic-data/setup_synthetic_data.sh --max-size 10
```

### Generate Batch Input
```bash
# Create Lambda payload from S3 objects
python scripts/s3-tools/batch-operations/s3_batch_input_generator.py s3://my-bucket/data/ --output payload.json

# Then invoke Lambda with the payload
aws lambda invoke --function-name checksum-initiator-dev --payload file://payload.json response.json
```

### Complete Workflow
```bash
# 1. Generate test data
python scripts/s3-tools/synthetic-data/generate_synthetic_dataset.py test-bucket --max-size 50

# 2. Create checksum payload
python scripts/s3-tools/batch-operations/s3_batch_input_generator.py s3://test-bucket/synthetic-data/ --output payload.json

# 3. Invoke Lambda
aws lambda invoke --function-name checksum-initiator-dev --payload file://payload.json response.json

# 4. Validate results
python scripts/s3-tools/synthetic-data/validate_dataset.py test-bucket
```

## Batch Operations Tools

### s3_batch_input_generator.py
Generates JSON payloads for the checksum initiator Lambda function.

**Key Options:**
- `--output FILE` - Save to file
- `--max-objects N` - Limit object count
- `--quiet` - Suppress informational output

## Synthetic Data Tools

### generate_synthetic_dataset.py
Creates test files optimized for S3 multipart testing.

**Generated Sizes:** 1GB, 5GB, 10GB, 50GB, 100GB, 500GB, 1TB, 5TB

**Key Options:**
- `--max-size N` - Maximum file size in GB
- `--prefix PREFIX` - S3 key prefix
- `--force` - Overwrite existing files

### validate_dataset.py
Validates generated test data against S3 constraints.

## Environment Variables

- `S3_STD_MV_TEST_BUCKET` - Default test bucket name

## Prerequisites

- Python 3.7+
- boto3
- AWS CLI configured
- S3 bucket permissions

## Installation

```bash
# Install dependencies
pip3 install -r requirements-dev.txt

# Make scripts executable
chmod +x scripts/s3-tools/batch-operations/s3_batch_input_generator
chmod +x scripts/s3-tools/synthetic-data/setup_synthetic_data.sh
```
