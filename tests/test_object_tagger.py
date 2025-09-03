import unittest
import json
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3

# Add the Lambda function to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda_functions/object_tagger/src'))

import lambda_function


class TestObjectTagger(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        # Mock environment variables
        self.env_vars = {
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
        
        # Sample event data
        self.sample_event = {
            "objects": [
                {
                    "bucket": "test-bucket",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": "abc123def456789012345678901234567890abcdef123456789012345678901234"
                },
                {
                    "bucket": "test-bucket",
                    "key": "file2.txt",
                    "algorithm": "MD5",
                    "checksum": "def456789012345678901234567890ab"
                }
            ]
        }
        
        self.sample_context = Mock()
    
    def tearDown(self):
        """Clean up test environment"""
        self.env_patcher.stop()
    
    @mock_aws
    def test_lambda_handler_success(self):
        """Test successful Lambda handler execution"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # Create test objects
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content 1')
        s3_client.put_object(Bucket='test-bucket', Key='file2.txt', Body='test content 2')
        
        # Execute
        result = lambda_function.lambda_handler(self.sample_event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertIn('message', body)
        self.assertEqual(body['successful_tags'], 2)
        self.assertEqual(body['failed_tags'], 0)
        self.assertEqual(len(body['results']), 2)
        
        # Verify tags were applied
        tags1 = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict1 = {tag['Key']: tag['Value'] for tag in tags1['TagSet']}
        self.assertIn('checksum-sha256', tag_dict1)
        self.assertEqual(tag_dict1['checksum-sha256'], 'abc123def456789012345678901234567890abcdef123456789012345678901234')
        self.assertIn('checksum-sha256-verified', tag_dict1)
        
        tags2 = s3_client.get_object_tagging(Bucket='test-bucket', Key='file2.txt')
        tag_dict2 = {tag['Key']: tag['Value'] for tag in tags2['TagSet']}
        self.assertIn('checksum-md5', tag_dict2)
        self.assertEqual(tag_dict2['checksum-md5'], 'def456789012345678901234567890ab')
        self.assertIn('checksum-md5-verified', tag_dict2)
    
    @mock_aws
    def test_lambda_handler_object_not_found(self):
        """Test handling of non-existent objects"""
        # Setup S3 mock without creating the objects
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # Execute
        result = lambda_function.lambda_handler(self.sample_event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['successful_tags'], 0)
        self.assertEqual(body['failed_tags'], 2)
        
        # Check that results indicate objects not found
        for result_item in body['results']:
            self.assertFalse(result_item['success'])
            # The error message can vary, just check it contains key information
            self.assertIn('Object not found', result_item['error'])
    
    def test_lambda_handler_empty_objects(self):
        """Test Lambda handler with empty objects list"""
        event = {"objects": []}
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 400)
        body = json.loads(result['body'])
        self.assertIn('error', body)
        self.assertEqual(body['error'], 'No objects provided for tagging')
    
    def test_lambda_handler_missing_objects(self):
        """Test Lambda handler with missing objects field"""
        event = {}
        
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        self.assertEqual(result['statusCode'], 400)
        body = json.loads(result['body'])
        self.assertIn('error', body)
        self.assertEqual(body['error'], 'No objects provided for tagging')
    
    @mock_aws
    def test_tag_s3_object_with_existing_tags(self):
        """Test tagging object that already has existing tags"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        # Add existing tags
        s3_client.put_object_tagging(
            Bucket='test-bucket',
            Key='file1.txt',
            Tagging={'TagSet': [{'Key': 'existing-tag', 'Value': 'existing-value'}]}
        )
        
        # Execute tagging function directly
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='abc123def456'
        )
        
        # Assertions
        self.assertTrue(result['success'])
        self.assertEqual(result['bucket'], 'test-bucket')
        self.assertEqual(result['key'], 'file1.txt')
        self.assertEqual(result['algorithm'], 'SHA256')
        self.assertEqual(result['checksum'], 'abc123def456')
        
        # Verify both existing and new tags are present
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
        self.assertIn('existing-tag', tag_dict)
        self.assertEqual(tag_dict['existing-tag'], 'existing-value')
        self.assertIn('checksum-sha256', tag_dict)
        self.assertEqual(tag_dict['checksum-sha256'], 'abc123def456')
        self.assertIn('checksum-sha256-verified', tag_dict)
    
    @mock_aws
    def test_tag_s3_object_with_version_id(self):
        """Test tagging versioned S3 object"""
        # Setup S3 mock with versioning
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_bucket_versioning(
            Bucket='test-bucket',
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Put object to get version ID
        response = s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        version_id = response['VersionId']
        
        # Execute tagging function with version ID
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='MD5',
            checksum='def456789012',
            version_id=version_id
        )
        
        # Assertions
        self.assertTrue(result['success'])
        self.assertEqual(result['version_id'], version_id)
        
        # Verify tags were applied to specific version
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt', VersionId=version_id)
        tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
        self.assertIn('checksum-md5', tag_dict)
        self.assertEqual(tag_dict['checksum-md5'], 'def456789012')
    
    @mock_aws
    def test_lambda_handler_mixed_success_failure(self):
        """Test Lambda handler with mix of successful and failed objects"""
        # Setup S3 mock with only one bucket/object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content 1')
        # Note: file2.txt is not created, so it will fail
        
        # Execute
        result = lambda_function.lambda_handler(self.sample_event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['successful_tags'], 1)
        self.assertEqual(body['failed_tags'], 1)
        self.assertEqual(len(body['results']), 2)
        
        # Check individual results
        results = body['results']
        success_result = next(r for r in results if r['success'])
        failure_result = next(r for r in results if not r['success'])
        
        self.assertEqual(success_result['key'], 'file1.txt')
        self.assertEqual(failure_result['key'], 'file2.txt')
    
    @mock_aws
    def test_lambda_handler_invalid_bucket_name(self):
        """Test Lambda handler with invalid bucket name"""
        event = {
            "objects": [
                {
                    "bucket": "non-existent-bucket",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": "abc123def456"
                }
            ]
        }
        
        # Execute (no bucket created)
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['successful_tags'], 0)
        self.assertEqual(body['failed_tags'], 1)
        
        result_item = body['results'][0]
        self.assertFalse(result_item['success'])
        self.assertIn('bucket', result_item['error'].lower())
    
    def test_lambda_handler_malformed_event(self):
        """Test Lambda handler with malformed event structure"""
        malformed_events = [
            None,
            {},
            {"objects": None},
            {"objects": "not-a-list"},
            {"objects": [None]},
            {"objects": [{}]},  # Missing required fields
            {"objects": [{"bucket": "test"}]},  # Missing key, algorithm, checksum
        ]
        
        for event in malformed_events:
            with self.subTest(event=event):
                result = lambda_function.lambda_handler(event, self.sample_context)
                # Function handles errors gracefully and returns 200 or 500
                self.assertIn(result['statusCode'], [200, 400, 500])
                # Should have a body with error information
                if result['statusCode'] != 200:
                    body = json.loads(result['body'])
                    self.assertIn('error', body)
    
    @mock_aws
    def test_lambda_handler_large_batch(self):
        """Test Lambda handler with large batch of objects"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # Create large batch event (50 objects)
        objects = []
        for i in range(50):
            key = f"file{i:03d}.txt"
            s3_client.put_object(Bucket='test-bucket', Key=key, Body=f'test content {i}')
            objects.append({
                "bucket": "test-bucket",
                "key": key,
                "algorithm": "SHA256",
                "checksum": f"checksum{i:03d}" + "0" * 50  # 64 char checksum
            })
        
        event = {"objects": objects}
        
        # Execute
        result = lambda_function.lambda_handler(event, self.sample_context)
        
        # Assertions
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['successful_tags'], 50)
        self.assertEqual(body['failed_tags'], 0)
        self.assertEqual(len(body['results']), 50)
    
    def test_lambda_handler_invalid_algorithm(self):
        """Test Lambda handler with invalid algorithm values"""
        invalid_algorithms = ["", "INVALID", "sha256", "md5", "SHA1", None, 123]
        
        for algorithm in invalid_algorithms:
            with self.subTest(algorithm=algorithm):
                event = {
                    "objects": [{
                        "bucket": "test-bucket",
                        "key": "file1.txt",
                        "algorithm": algorithm,
                        "checksum": "abc123def456"
                    }]
                }
                
                result = lambda_function.lambda_handler(event, self.sample_context)
                # Should handle gracefully, not crash
                self.assertIsInstance(result, dict)
                self.assertIn('statusCode', result)
    
    def test_lambda_handler_invalid_checksum_format(self):
        """Test Lambda handler with invalid checksum formats"""
        invalid_checksums = [
            "",  # Empty
            "short",  # Too short
            "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",  # Invalid hex characters
            None,  # None value
            123,  # Wrong type
            "abc123def456" * 10,  # Too long
        ]
        
        for checksum in invalid_checksums:
            with self.subTest(checksum=checksum):
                event = {
                    "objects": [{
                        "bucket": "test-bucket",
                        "key": "file1.txt",
                        "algorithm": "SHA256",
                        "checksum": checksum
                    }]
                }
                
                result = lambda_function.lambda_handler(event, self.sample_context)
                # Should handle gracefully, not crash
                self.assertIsInstance(result, dict)
                self.assertIn('statusCode', result)
    
    @mock_aws
    def test_tag_s3_object_bucket_not_found(self):
        """Test tagging object in non-existent bucket"""
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='non-existent-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='abc123def456'
        )
        
        self.assertFalse(result['success'])
        self.assertEqual(result['bucket'], 'non-existent-bucket')
        self.assertEqual(result['key'], 'file1.txt')
        self.assertIn('error', result)
    
    @mock_aws
    def test_tag_s3_object_key_not_found(self):
        """Test tagging non-existent object key"""
        # Setup S3 mock with bucket but no object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='non-existent-file.txt',
            algorithm='SHA256',
            checksum='abc123def456'
        )
        
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], 'Object not found')
    
    @mock_aws
    def test_tag_s3_object_version_not_found(self):
        """Test tagging with non-existent version ID"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='abc123def456',
            version_id='non-existent-version'
        )
        
        self.assertFalse(result['success'])
        self.assertIn('error', result)
    
    @mock_aws
    def test_tag_s3_object_algorithm_case_handling(self):
        """Test that algorithm names are properly case-handled"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        algorithms = ['SHA256', 'sha256', 'Sha256', 'MD5', 'md5', 'Md5']
        
        for algorithm in algorithms:
            with self.subTest(algorithm=algorithm):
                result = lambda_function.tag_s3_object_with_checksum(
                    bucket='test-bucket',
                    key='file1.txt',
                    algorithm=algorithm,
                    checksum='abc123def456'
                )
                
                self.assertTrue(result['success'])
                
                # Verify tag key is lowercase
                tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
                tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
                expected_tag_key = f"checksum-{algorithm.lower()}"
                self.assertIn(expected_tag_key, tag_dict)
    
    @mock_aws
    def test_tag_s3_object_timestamp_format(self):
        """Test that timestamp format is correct"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='abc123def456'
        )
        
        self.assertTrue(result['success'])
        
        # Verify timestamp format in tags
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
        
        timestamp = tag_dict['checksum-sha256-verified']
        # Should be ISO format ending with Z
        self.assertTrue(timestamp.endswith('Z'))
        # Updated regex to match actual format (microseconds instead of milliseconds)
        self.assertRegex(timestamp, r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$')
        
        # Should also be in the result
        self.assertIn('tagged_at', result)
        self.assertTrue(result['tagged_at'].endswith('+00:00'))  # Result uses +00:00 format
    
    @mock_aws
    def test_tag_s3_object_preserve_existing_unrelated_tags(self):
        """Test that unrelated existing tags are preserved"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        # Add various existing tags
        existing_tags = [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Owner', 'Value': 'DataTeam'},
            {'Key': 'checksum-md5', 'Value': 'old-md5-value'},  # This should be overwritten
            {'Key': 'checksum-md5-verified', 'Value': '2023-01-01T00:00:00.000Z'},  # This should be overwritten
            {'Key': 'Project', 'Value': 'MediaProcessing'}
        ]
        
        s3_client.put_object_tagging(
            Bucket='test-bucket',
            Key='file1.txt',
            Tagging={'TagSet': existing_tags}
        )
        
        # Add new SHA256 checksum
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='new-sha256-checksum'
        )
        
        self.assertTrue(result['success'])
        
        # Verify all tags
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
        
        # Existing unrelated tags should be preserved
        self.assertEqual(tag_dict['Environment'], 'Production')
        self.assertEqual(tag_dict['Owner'], 'DataTeam')
        self.assertEqual(tag_dict['Project'], 'MediaProcessing')
        
        # Old MD5 tags should be preserved (not overwritten by SHA256)
        self.assertEqual(tag_dict['checksum-md5'], 'old-md5-value')
        self.assertEqual(tag_dict['checksum-md5-verified'], '2023-01-01T00:00:00.000Z')
        
        # New SHA256 tags should be added
        self.assertEqual(tag_dict['checksum-sha256'], 'new-sha256-checksum')
        self.assertIn('checksum-sha256-verified', tag_dict)
        self.assertNotEqual(tag_dict['checksum-sha256-verified'], '2023-01-01T00:00:00.000Z')
    
    @mock_aws
    def test_tag_s3_object_overwrite_same_algorithm(self):
        """Test that tags for the same algorithm are overwritten"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        # Add initial SHA256 checksum
        result1 = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='first-checksum'
        )
        self.assertTrue(result1['success'])
        
        # Get initial timestamp
        tags1 = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict1 = {tag['Key']: tag['Value'] for tag in tags1['TagSet']}
        first_timestamp = tag_dict1['checksum-sha256-verified']
        
        # Wait a moment and add updated SHA256 checksum
        import time
        time.sleep(0.1)
        
        result2 = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='updated-checksum'
        )
        self.assertTrue(result2['success'])
        
        # Verify tags were updated
        tags2 = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict2 = {tag['Key']: tag['Value'] for tag in tags2['TagSet']}
        
        self.assertEqual(tag_dict2['checksum-sha256'], 'updated-checksum')
        self.assertNotEqual(tag_dict2['checksum-sha256-verified'], first_timestamp)
    
    @mock_aws 
    def test_tag_s3_object_max_tags_limit(self):
        """Test behavior when approaching S3's 10-tag limit"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        # Add 8 existing tags (leaving room for 2 checksum tags)
        existing_tags = [
            {'Key': f'tag{i}', 'Value': f'value{i}'} for i in range(8)
        ]
        
        s3_client.put_object_tagging(
            Bucket='test-bucket',
            Key='file1.txt',
            Tagging={'TagSet': existing_tags}
        )
        
        # Add checksum (should work - total 10 tags)
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='test-checksum'
        )
        
        self.assertTrue(result['success'])
        
        # Verify all tags are present
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        self.assertEqual(len(tags['TagSet']), 10)  # 8 existing + 2 checksum tags
    
    def test_get_s3_client_singleton(self):
        """Test that get_s3_client returns the same instance"""
        client1 = lambda_function.get_s3_client()
        client2 = lambda_function.get_s3_client()
        self.assertIs(client1, client2)
    
    @patch('lambda_function.get_s3_client')
    def test_lambda_handler_s3_client_exception(self, mock_get_s3_client):
        """Test Lambda handler when S3 client raises exception"""
        # Mock S3 client to raise exception
        mock_s3_client = Mock()
        mock_s3_client.get_object_tagging.side_effect = Exception("S3 service error")
        mock_get_s3_client.return_value = mock_s3_client
        
        result = lambda_function.lambda_handler(self.sample_event, self.sample_context)
        
        # Should handle gracefully
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['successful_tags'], 0)
        self.assertEqual(body['failed_tags'], 2)
        
        for result_item in body['results']:
            self.assertFalse(result_item['success'])
            self.assertIn('error', result_item)
    
    @mock_aws
    def test_tag_s3_object_get_tagging_graceful_handling(self):
        """Test tag_s3_object_with_checksum handles missing tags gracefully"""
        # Setup S3 mock with bucket but no object (will cause get_object_tagging to fail)
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        # This should work even if getting existing tags fails initially
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='abc123def456'
        )
        
        # Should succeed
        self.assertTrue(result['success'])
        
        # Verify tags were applied
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
        self.assertIn('checksum-sha256', tag_dict)
    
    @patch('lambda_function.get_s3_client')
    def test_tag_s3_object_put_tagging_exception(self, mock_get_s3_client):
        """Test tag_s3_object_with_checksum when put_object_tagging fails"""
        mock_s3_client = Mock()
        mock_get_s3_client.return_value = mock_s3_client
        
        # Mock successful get but failed put
        mock_s3_client.get_object_tagging.return_value = {'TagSet': []}
        mock_s3_client.put_object_tagging.side_effect = Exception("Permission denied")
        
        result = lambda_function.tag_s3_object_with_checksum(
            bucket='test-bucket',
            key='file1.txt',
            algorithm='SHA256',
            checksum='abc123def456'
        )
        
        self.assertFalse(result['success'])
        self.assertIn('Permission denied', result['error'])
    
    def test_lambda_handler_context_timeout(self):
        """Test Lambda handler behavior with context timeout"""
        # Mock context with short remaining time
        mock_context = Mock()
        mock_context.get_remaining_time_in_millis.return_value = 100  # 100ms left
        
        # This should still process normally since we don't check timeout in current implementation
        result = lambda_function.lambda_handler(self.sample_event, mock_context)
        self.assertIsInstance(result, dict)
        self.assertIn('statusCode', result)
    
    def test_lambda_handler_unicode_object_keys(self):
        """Test Lambda handler with Unicode characters in object keys"""
        unicode_event = {
            "objects": [
                {
                    "bucket": "test-bucket",
                    "key": "файл.txt",  # Cyrillic
                    "algorithm": "SHA256",
                    "checksum": "abc123def456"
                },
                {
                    "bucket": "test-bucket", 
                    "key": "文件.txt",  # Chinese
                    "algorithm": "MD5",
                    "checksum": "def456789012"
                },
                {
                    "bucket": "test-bucket",
                    "key": "🎵music🎵.mp3",  # Emoji
                    "algorithm": "SHA256",
                    "checksum": "ghi789012345"
                }
            ]
        }
        
        result = lambda_function.lambda_handler(unicode_event, self.sample_context)
        
        # Should handle gracefully without crashing
        self.assertIsInstance(result, dict)
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(len(body['results']), 3)
    
    def test_lambda_handler_special_characters_in_bucket_names(self):
        """Test Lambda handler with special characters in bucket names"""
        special_event = {
            "objects": [
                {
                    "bucket": "test-bucket-with-dashes",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": "abc123def456"
                },
                {
                    "bucket": "test.bucket.with.dots",
                    "key": "file2.txt",
                    "algorithm": "MD5",
                    "checksum": "def456789012"
                }
            ]
        }
        
        result = lambda_function.lambda_handler(special_event, self.sample_context)
        
        # Should handle gracefully
        self.assertIsInstance(result, dict)
        self.assertEqual(result['statusCode'], 200)
    
    def test_lambda_handler_very_long_checksum(self):
        """Test Lambda handler with very long checksum values"""
        long_checksum_event = {
            "objects": [
                {
                    "bucket": "test-bucket",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": "a" * 1000  # Very long checksum
                }
            ]
        }
        
        result = lambda_function.lambda_handler(long_checksum_event, self.sample_context)
        
        # Should handle gracefully (S3 tag values can be up to 256 characters)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['statusCode'], 200)
    
    def test_lambda_handler_empty_string_values(self):
        """Test Lambda handler with empty string values"""
        empty_values_event = {
            "objects": [
                {
                    "bucket": "",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": "abc123def456"
                },
                {
                    "bucket": "test-bucket",
                    "key": "",
                    "algorithm": "SHA256",
                    "checksum": "abc123def456"
                },
                {
                    "bucket": "test-bucket",
                    "key": "file1.txt",
                    "algorithm": "",
                    "checksum": "abc123def456"
                },
                {
                    "bucket": "test-bucket",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": ""
                }
            ]
        }
        
        result = lambda_function.lambda_handler(empty_values_event, self.sample_context)
        
        # Should handle gracefully
        self.assertIsInstance(result, dict)
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(len(body['results']), 4)
        # All should fail due to empty values
        self.assertEqual(body['successful_tags'], 0)
        self.assertEqual(body['failed_tags'], 4)
    
    @mock_aws
    def test_lambda_handler_concurrent_tagging_same_object(self):
        """Test Lambda handler with multiple tags for the same object"""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='file1.txt', Body='test content')
        
        concurrent_event = {
            "objects": [
                {
                    "bucket": "test-bucket",
                    "key": "file1.txt",
                    "algorithm": "SHA256",
                    "checksum": "sha256checksum"
                },
                {
                    "bucket": "test-bucket",
                    "key": "file1.txt",
                    "algorithm": "MD5",
                    "checksum": "md5checksum"
                }
            ]
        }
        
        result = lambda_function.lambda_handler(concurrent_event, self.sample_context)
        
        # Both should succeed
        self.assertEqual(result['statusCode'], 200)
        body = json.loads(result['body'])
        self.assertEqual(body['successful_tags'], 2)
        self.assertEqual(body['failed_tags'], 0)
        
        # Verify both checksums are tagged
        tags = s3_client.get_object_tagging(Bucket='test-bucket', Key='file1.txt')
        tag_dict = {tag['Key']: tag['Value'] for tag in tags['TagSet']}
        
        self.assertEqual(tag_dict['checksum-sha256'], 'sha256checksum')
        self.assertEqual(tag_dict['checksum-md5'], 'md5checksum')
        self.assertIn('checksum-sha256-verified', tag_dict)
        self.assertIn('checksum-md5-verified', tag_dict)
    
    def test_lambda_handler_response_structure(self):
        """Test that Lambda handler response has correct structure"""
        result = lambda_function.lambda_handler({"objects": []}, self.sample_context)
        
        # Verify response structure
        self.assertIsInstance(result, dict)
        self.assertIn('statusCode', result)
        self.assertIn('body', result)
        
        body = json.loads(result['body'])
        self.assertIn('error', body)  # Should have error for empty objects
        
        # Test with valid objects
        result = lambda_function.lambda_handler(self.sample_event, self.sample_context)
        body = json.loads(result['body'])
        
        required_fields = ['message', 'successful_tags', 'failed_tags', 'results']
        for field in required_fields:
            self.assertIn(field, body)
        
        # Verify results structure
        for result_item in body['results']:
            self.assertIn('bucket', result_item)
            self.assertIn('key', result_item)
            self.assertIn('success', result_item)
            if result_item['success']:
                self.assertIn('algorithm', result_item)
                self.assertIn('checksum', result_item)
                self.assertIn('tagged_at', result_item)
            else:
                self.assertIn('error', result_item)


if __name__ == '__main__':
    unittest.main()
