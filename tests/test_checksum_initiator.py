import unittest
import json
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3
from decimal import Decimal

# Add the Lambda function to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda_functions/checksum_initiator/src'))

import lambda_function


class TestChecksumInitiator(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        # Mock environment variables
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
        
        # Patch environment variables
        self.env_patcher = patch.dict(os.environ, self.env_vars)
        self.env_patcher.start()
        
        # Sample test data
        self.sample_event = {
            'bucket': 'test-source-bucket',
            'keys': [
                {'key': 'file1.txt'},
                {'key': 'file2.txt', 'version_id': 'version123'},
                {'key': 'file3.txt', 'md5': 'abc123', 'sha256': 'def456'}
            ]
        }
        
        self.sample_context = Mock()
    
    def tearDown(self):
        """Clean up test environment"""
        self.env_patcher.stop()
    
    @mock_aws
    def test_lambda_handler_success(self):
        """Test successful Lambda handler execution"""
        # Setup AWS mocks
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Mock S3 Control client
        with patch('lambda_function.get_s3control_client') as mock_get_s3control:
            mock_s3control = Mock()
            mock_get_s3control.return_value = mock_s3control
            
            # Mock successful job creation
            mock_s3control.create_job.side_effect = [
                {'JobId': 'sha256-job-123'},
                {'JobId': 'md5-job-456'}
            ]
            
            # Execute
            result = lambda_function.lambda_handler(self.sample_event, self.sample_context)
            
            # Assertions
            self.assertEqual(result['statusCode'], 200)
            body = json.loads(result['body'])
            self.assertIn('message', body)
            self.assertIn('request_id', body)
            self.assertIn('jobs', body)
            self.assertEqual(len(body['jobs']), 2)
            
            # Verify request_id is a valid UUID4
            import uuid
            request_id = body['request_id']
            self.assertIsInstance(request_id, str)
            # Verify it's a valid UUID by trying to parse it
            parsed_uuid = uuid.UUID(request_id)
            self.assertEqual(str(parsed_uuid), request_id)
            self.assertEqual(parsed_uuid.version, 4)  # UUID4
            
            # Verify request_id is stored in DynamoDB entries
            response = table.scan()
            items = response['Items']
            self.assertEqual(len(items), 6)  # 3 objects × 2 algorithms
            
            for item in items:
                self.assertEqual(item['request_id'], request_id)
                self.assertIn('job_id', item)
                self.assertEqual(item['status'], 'claimed')
    
    @mock_aws
    def test_request_id_uniqueness(self):
        """Test that each invocation generates a unique request_id"""
        # Setup AWS mocks
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Mock S3 Control client
        with patch('lambda_function.get_s3control_client') as mock_get_s3control:
            mock_s3control = Mock()
            mock_get_s3control.return_value = mock_s3control
            
            # Mock successful job creation
            mock_s3control.create_job.side_effect = [
                {'JobId': 'sha256-job-123'},
                {'JobId': 'md5-job-456'},
                {'JobId': 'sha256-job-789'},
                {'JobId': 'md5-job-012'}
            ]
            
            # Execute twice with same event
            result1 = lambda_function.lambda_handler(self.sample_event, self.sample_context)
            result2 = lambda_function.lambda_handler(self.sample_event, self.sample_context)
            
            # Extract request_ids
            body1 = json.loads(result1['body'])
            body2 = json.loads(result2['body'])
            request_id1 = body1['request_id']
            request_id2 = body2['request_id']
            
            # Verify they are different
            self.assertNotEqual(request_id1, request_id2)
            
            # Verify both are valid UUID4s
            import uuid
            uuid1 = uuid.UUID(request_id1)
            uuid2 = uuid.UUID(request_id2)
            self.assertEqual(uuid1.version, 4)
            self.assertEqual(uuid2.version, 4)
    
    def test_lambda_handler_missing_bucket(self):
        """Test Lambda handler with missing bucket parameter"""
        event = {'keys': [{'key': 'file1.txt'}]}
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 500)
        body = json.loads(result['body'])
        self.assertIn('error', body)
    
    def test_lambda_handler_missing_keys(self):
        """Test Lambda handler with missing keys parameter"""
        event = {'bucket': 'test-bucket'}
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 500)
        body = json.loads(result['body'])
        self.assertIn('error', body)
    
    def test_lambda_handler_empty_keys(self):
        """Test Lambda handler with empty keys list"""
        event = {'bucket': 'test-bucket', 'keys': []}
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 500)
        body = json.loads(result['body'])
        self.assertIn('error', body)
    
    @mock_aws
    def test_generate_csv_manifest(self):
        """Test CSV manifest generation"""
        # Setup S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        
        objects = [
            {'bucket': 'test-source-bucket', 'key': 'file1.txt'},
            {'bucket': 'test-source-bucket', 'key': 'file2.txt', 'version_id': 'version123'}
        ]
        
        # Mock the bucket parameter by patching the function
        with patch('lambda_function.get_manifest_bucket', return_value='test-manifest-bucket'):
            manifest_key = lambda_function.generate_csv_manifest(objects)
        
        # Verify manifest was uploaded
        self.assertIsNotNone(manifest_key)
        self.assertTrue(manifest_key.startswith('batch-jobs/manifests/'))
        
        # Verify manifest content
        response = s3_client.get_object(Bucket='test-manifest-bucket', Key=manifest_key)
        content = response['Body'].read().decode('utf-8')
        
        lines = content.strip().split('\n')
        self.assertEqual(len(lines), 2)  # Two objects
        
        # Verify CSV format
        self.assertIn('test-source-bucket,file1.txt', lines[0])
        self.assertIn('test-source-bucket,file2.txt,version123', lines[1])
    
    @mock_aws
    def test_create_initial_checksum_entries(self):
        """Test DynamoDB initial entries creation"""
        # Setup DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        objects = [
            {'bucket': 'test-source-bucket', 'key': 'file1.txt'},
            {'bucket': 'test-source-bucket', 'key': 'file2.txt', 'version_id': 'version123', 'provided_md5': 'expected_md5'}
        ]
        
        job_results = [
            {'algorithm': 'SHA256', 'job_id': 'sha256-job-123', 'status': 'created'},
            {'algorithm': 'MD5', 'job_id': 'md5-job-456', 'status': 'created'}
        ]
        
        request_id = "test-request-123"
        
        lambda_function.create_initial_checksum_entries(objects, job_results, request_id)
        
        # Verify entries were created
        response = table.scan()
        items = response['Items']
        
        # Should have 4 entries (2 objects × 2 algorithms)
        self.assertEqual(len(items), 4)
        
        # Verify entry structure
        sha256_entry = next(item for item in items if 'SHA256' in item['object_key'])
        self.assertEqual(sha256_entry['bucket'], 'test-source-bucket')
        self.assertEqual(sha256_entry['status'], 'claimed')
        self.assertEqual(sha256_entry['request_id'], request_id)
        self.assertEqual(sha256_entry['job_id'], 'sha256-job-123')


if __name__ == '__main__':
    unittest.main()
