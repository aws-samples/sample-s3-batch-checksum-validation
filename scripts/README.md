# Scripts

Utility scripts for deployment, testing, and operations.

## Structure

```
scripts/
├── s3-tools/           # S3 utilities (batch operations, synthetic data)
├── deployment/         # Deployment and infrastructure scripts
├── monitoring/         # Monitoring and alerting scripts
└── maintenance/        # Cleanup and maintenance scripts
```

## Usage

```bash
# S3 tools
python scripts/s3-tools/synthetic-data/generate_synthetic_dataset.py bucket --max-size 10
python scripts/s3-tools/batch-operations/s3_batch_input_generator.py s3://bucket/data/

# Deployment
./scripts/deployment/deploy.sh dev
./scripts/deployment/deploy.sh prod

# Monitoring
./scripts/monitoring/check-batch-jobs.sh
./scripts/monitoring/validate-checksums.sh

# Maintenance
./scripts/maintenance/cleanup-old-jobs.sh
./scripts/maintenance/rotate-logs.sh
```

## Prerequisites

- AWS CLI configured
- Python 3.7+
- Appropriate IAM permissions
- Access to target S3 buckets and Lambda functions
