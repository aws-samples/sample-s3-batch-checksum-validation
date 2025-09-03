import unittest
import json
import os
import sys
import importlib.util
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3

# Import Lambda functions using importlib to avoid name collision
initiator_path = os.path.join(os.path.dirname(__file__), '../lambda_functions/checksum_initiator/src/lambda_function.py')
processor_path = os.path.join(os.path.dirname(__file__), '../lambda_functions/checksum_results_processor/src/lambda_function.py')

# Load initiator module
spec = importlib.util.spec_from_file_location("initiator", initiator_path)
initiator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(initiator)

# Load processor module
spec = importlib.util.spec_from_file_location("processor", processor_path)
processor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(processor)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete checksum validation workflow"""
    
    def setUp(self):
        """Set up test environment"""
        self.env_vars = {
            'MANIFEST_BUCKET': 'test-manifest-bucket',
            'BATCH_ROLE_ARN': 'arn:aws:iam::123456789012:role/TestBatchRole',
            'ACCOUNT_ID': '123456789012',
            'ENVIRONMENT': 'test',
            'CHECKSUM_TABLE_NAME': 'TestChecksumTable',
            'AWS_DEFAULT_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'testing',
            'AWS_SECRET_ACCESS_KEY': 'testing',
            'AWS_SECURITY_TOKEN': 'testing',
            'AWS_SESSION_TOKEN': 'testing'
        }
        
        self.env_patcher = patch.dict(os.environ, self.env_vars)
        self.env_patcher.start()
        
        self.sample_event = {
            'bucket': 'test-source-bucket',
            'keys': [
                {'key': 'file1.txt'},
                {'key': 'file2.txt', 'md5': 'abc123'}
            ]
        }
    
    def tearDown(self):
        """Clean up test environment"""
        self.env_patcher.stop()
    
    @mock_aws
    def test_end_to_end_workflow_simulation(self):
        """Test simulated end-to-end workflow without S3 Control dependency"""
        # Setup AWS services
        self._setup_aws_services()
        
        # Step 1: Test individual components of checksum initiator
        # Test CSV generation
        objects = [
            {'bucket': 'test-source-bucket', 'key': 'file1.txt'},
            {'bucket': 'test-source-bucket', 'key': 'file2.txt'}
        ]
        
        manifest_key = initiator.generate_csv_manifest(objects)
        self.assertIn('batch-jobs/manifests/manifest-', manifest_key)
        
        # Test DynamoDB entry creation with mock job results
        job_results = [
            {'algorithm': 'SHA256', 'job_id': 'test-sha256-job', 'status': 'created'},
            {'algorithm': 'MD5', 'job_id': 'test-md5-job', 'status': 'created'}
        ]
        request_id = 'test-request-id-12345'
        initiator.create_initial_checksum_entries(objects, job_results, request_id)
        
        # Verify DynamoDB records were created
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('TestChecksumTable')
        response = table.scan()
        self.assertEqual(len(response['Items']), 4)  # 2 objects × 2 algorithms
        
        # Step 2: Simulate batch job completion and create report
        self._create_batch_report()
        
        # Step 3: Test results processor components
        # Test CSV parsing
        s3_client = boto3.client('s3', region_name='us-east-1')
        response = s3_client.get_object(
            Bucket='test-manifest-bucket',
            Key='batch-jobs/reports/sha256/job-report.csv'
        )
        content = response['Body'].read().decode('utf-8')
        
        checksums = processor.parse_batch_report_csv(content, 'SHA256')
        self.assertEqual(len(checksums), 2)
        
        # Test DynamoDB updates
        result = processor.update_checksum_records(checksums, 'SHA256')
        self.assertEqual(result['updated_count'], 2)  # Two SHA256 records updated
        
        # Verify DynamoDB records were updated
        sha256_key = 'test-source-bucket#file1.txt#SHA256'
        response = table.get_item(Key={'object_key': sha256_key})
        item = response['Item']
        self.assertEqual(item['status'], 'succeeded')
        self.assertEqual(item['checksum'], 'ABC123DEF456')
        
        # Note: S3 object tagging is no longer performed by the results processor
        # The object tagger Lambda function is available as a standalone component
        # but is not automatically invoked in the main workflow
    
    def _setup_aws_services(self):
        """Setup AWS services for testing"""
        # Setup S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        
        # Create source bucket and objects for tagging
        s3_client.create_bucket(Bucket='test-source-bucket')
        s3_client.put_object(Bucket='test-source-bucket', Key='file1.txt', Body=b'test content 1')
        s3_client.put_object(Bucket='test-source-bucket', Key='file2.txt', Body=b'test content 2')
        
        # Setup DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
    
    def _create_batch_report(self):
        """Create a mock batch job report"""
        csv_content = '''test-source-bucket,file1.txt,,succeeded,200,,"{""checksum_hex"":""ABC123DEF456"",""checksumAlgorithm"":""SHA256""}"
test-source-bucket,file2.txt,,failed,403,,"{""error"":""Access denied""}"'''
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.put_object(
            Bucket='test-manifest-bucket',
            Key='batch-jobs/reports/sha256/job-report.csv',
            Body=csv_content
        )


if __name__ == '__main__':
    unittest.main()
