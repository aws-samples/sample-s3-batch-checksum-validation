import unittest
import json
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3

# Add the Lambda function to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda_functions/checksum_results_processor/src'))

import lambda_function


class TestChecksumResultsProcessor(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        # Mock environment variables
        self.env_vars = {
            'CHECKSUM_TABLE_NAME': 'TestChecksumTable',
            'ENVIRONMENT': 'test',
            'AWS_DEFAULT_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'testing',
            'AWS_SECRET_ACCESS_KEY': 'testing',
            'AWS_SECURITY_TOKEN': 'testing',
            'AWS_SESSION_TOKEN': 'testing'
        }
        
        # Patch environment variables
        self.env_patcher = patch.dict(os.environ, self.env_vars)
        self.env_patcher.start()
        
        # Sample S3 event
        self.sample_s3_event = {
            'Records': [
                {
                    'eventSource': 'aws:s3',
                    's3': {
                        'bucket': {'name': 'test-manifest-bucket'},
                        'object': {'key': 'batch-jobs/reports/sha256/job-report.csv'}
                    }
                }
            ]
        }
        
        # Sample CSV content
        self.sample_csv_content = '''test-bucket,file1.txt,,succeeded,200,,"{""checksum_hex"":""ABC123"",""checksumAlgorithm"":""SHA256""}"
test-bucket,file2.txt,,failed,403,,"{""error"":""Access denied""}"'''
        
        self.sample_context = Mock()
    
    def tearDown(self):
        """Clean up test environment"""
        self.env_patcher.stop()
    
    @mock_aws
    def test_lambda_handler_success(self):
        """Test successful Lambda handler execution with S3 event"""
        # Setup AWS mocks
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        s3_client.create_bucket(Bucket='test-bucket')  # Create the bucket referenced in CSV
        s3_client.put_object(
            Bucket='test-manifest-bucket',
            Key='batch-jobs/reports/sha256/job-report.csv',
            Body=self.sample_csv_content
        )
        
        # Create the test object that will be tagged
        s3_client.put_object(
            Bucket='test-bucket',
            Key='file1.txt',
            Body='test content'
        )
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Create initial DynamoDB record with request_id
        test_request_id = "test-request-uuid-123"
        table.put_item(Item={
            'object_key': 'test-bucket#file1.txt#SHA256',
            'bucket': 'test-bucket',
            'key': 'file1.txt',
            'algorithm': 'SHA256',
            'status': 'claimed',
            'request_id': test_request_id
        })
        
        # Execute
        result = lambda_function.lambda_handler(self.sample_s3_event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertIn('message', body)
        self.assertIn('results', body)
        
        # Verify request_id handling
        results = body['results']
        self.assertEqual(len(results), 1)  # One S3 record processed
        
        result_data = results[0]
        self.assertIn('request_ids', result_data)
        self.assertIn(test_request_id, result_data['request_ids'])
        self.assertEqual(result_data['updated_records'], 1)
        
        # Verify DynamoDB record was updated with checksum
        updated_item = table.get_item(Key={'object_key': 'test-bucket#file1.txt#SHA256'})['Item']
        self.assertEqual(updated_item['checksum'], 'ABC123')
        self.assertEqual(updated_item['status'], 'succeeded')
        self.assertEqual(updated_item['request_id'], test_request_id)  # Should preserve original request_id
    
    def test_lambda_handler_no_records(self):
        """Test Lambda handler with no records"""
        event = {'Records': []}
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['results'], [])
    
    def test_lambda_handler_non_s3_event(self):
        """Test Lambda handler with non-S3 event"""
        event = {
            'Records': [{
                'eventSource': 'aws:sns',
                'sns': {'message': 'test'}
            }]
        }
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['results'], [])
    
    @mock_aws
    def test_request_id_collection_multiple_records(self):
        """Test that request_ids are properly collected from multiple records"""
        # Setup AWS mocks
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        
        # CSV content with multiple files
        csv_content = '''test-bucket,file1.txt,,succeeded,200,,"{""checksum_hex"":""ABC123"",""checksumAlgorithm"":""SHA256""}"
test-bucket,file2.txt,,succeeded,200,,"{""checksum_hex"":""DEF456"",""checksumAlgorithm"":""SHA256""}"
test-bucket,file3.txt,,succeeded,200,,"{""checksum_hex"":""GHI789"",""checksumAlgorithm"":""SHA256""}"'''
        
        s3_client.put_object(
            Bucket='test-manifest-bucket',
            Key='batch-jobs/reports/sha256/job-report.csv',
            Body=csv_content
        )
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Create initial DynamoDB records with different request_ids
        request_id_1 = "request-uuid-111"
        request_id_2 = "request-uuid-222"
        request_id_3 = "request-uuid-333"
        
        table.put_item(Item={
            'object_key': 'test-bucket#file1.txt#SHA256',
            'bucket': 'test-bucket',
            'key': 'file1.txt',
            'algorithm': 'SHA256',
            'status': 'claimed',
            'request_id': request_id_1
        })
        
        table.put_item(Item={
            'object_key': 'test-bucket#file2.txt#SHA256',
            'bucket': 'test-bucket',
            'key': 'file2.txt',
            'algorithm': 'SHA256',
            'status': 'claimed',
            'request_id': request_id_2
        })
        
        table.put_item(Item={
            'object_key': 'test-bucket#file3.txt#SHA256',
            'bucket': 'test-bucket',
            'key': 'file3.txt',
            'algorithm': 'SHA256',
            'status': 'claimed',
            'request_id': request_id_3
        })
        
        # Execute
        result = lambda_function.lambda_handler(self.sample_s3_event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        results = body['results']
        self.assertEqual(len(results), 1)  # One S3 record processed
        
        result_data = results[0]
        self.assertIn('request_ids', result_data)
        self.assertEqual(result_data['updated_records'], 3)
        
        # Verify all request_ids are collected
        collected_request_ids = result_data['request_ids']
        self.assertEqual(len(collected_request_ids), 3)
        self.assertIn(request_id_1, collected_request_ids)
        self.assertIn(request_id_2, collected_request_ids)
        self.assertIn(request_id_3, collected_request_ids)
    
    @mock_aws
    def test_request_id_handling_missing_request_id(self):
        """Test handling of records without request_id"""
        # Setup AWS mocks
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-manifest-bucket')
        s3_client.put_object(
            Bucket='test-manifest-bucket',
            Key='batch-jobs/reports/sha256/job-report.csv',
            Body=self.sample_csv_content
        )
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='TestChecksumTable',
            KeySchema=[{'AttributeName': 'object_key', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'object_key', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Create initial DynamoDB record WITHOUT request_id
        table.put_item(Item={
            'object_key': 'test-bucket#file1.txt#SHA256',
            'bucket': 'test-bucket',
            'key': 'file1.txt',
            'algorithm': 'SHA256',
            'status': 'claimed'
            # No request_id field
        })
        
        # Execute
        result = lambda_function.lambda_handler(self.sample_s3_event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        results = body['results']
        self.assertEqual(len(results), 1)
        
        result_data = results[0]
        self.assertIn('request_ids', result_data)
        # Should be empty list when no request_ids are found
        self.assertEqual(result_data['request_ids'], [])


if __name__ == '__main__':
    unittest.main()
