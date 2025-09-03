#!/usr/bin/env python3
import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks, NagSuppressions
from stacks.s3_batch_checksum_stack import S3BatchChecksumStack

app = cdk.App()

# Get configuration from context or environment
account = app.node.try_get_context("account") or "123456789012"
region = app.node.try_get_context("region") or "us-east-1"
environment = app.node.try_get_context("environment") or "dev"

env = cdk.Environment(account=account, region=region)

# Create the main stack
stack = S3BatchChecksumStack(
    app, 
    f"S3BatchChecksumStack-{environment}",
    env=env,
    environment=environment,
    description="Stack for S3 batch checksum validation Lambda function and supporting resources"
)

# Add cdk-nag checks
cdk.Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

app.synth()
