#!/usr/bin/env python3
"""Tests for s3_batch_input_generator.py"""

import pytest
import json
from unittest.mock import Mock, patch, mock_open
from botocore.exceptions import ClientError
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../scripts/s3-tools/batch-operations'))

from s3_batch_input_generator import S3BatchPayloadGenerator


class TestS3BatchPayloadGenerator:
    
    def setup_method(self):
        with patch('boto3.client'):
            self.generator = S3BatchPayloadGenerator()
    
    def test_parse_s3_path_valid(self):
        bucket, prefix = self.generator.parse_s3_path('s3://my-bucket/path/to/files')
        assert bucket == 'my-bucket'
        assert prefix == 'path/to/files'
    
    def test_parse_s3_path_invalid(self):
        with pytest.raises(ValueError):
            self.generator.parse_s3_path('http://bucket/path')
    
    @patch('boto3.client')
    def test_list_s3_objects_success(self, mock_client):
        mock_paginator = Mock()
        mock_client.return_value.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {
                        'Key': 'file1.txt',
                        'Size': 1024,
                        'LastModified': Mock(isoformat=Mock(return_value='2024-01-01T00:00:00')),
                        'ETag': '"abc123"'
                    }
                ]
            }
        ]
        
        generator = S3BatchPayloadGenerator()
        objects = generator.list_s3_objects('test-bucket', 'prefix/')
        
        assert len(objects) == 1
        assert objects[0]['key'] == 'file1.txt'
    
    def test_generate_lambda_payload(self):
        objects = [
            {'key': 'file1.txt', 'version_id': None},
            {'key': 'file2.txt', 'version_id': 'v123'}
        ]
        
        payload = self.generator.generate_lambda_payload('test-bucket', objects)
        
        assert payload['bucket'] == 'test-bucket'
        assert len(payload['keys']) == 2
        assert payload['keys'][1] == {'key': 'file2.txt', 'version_id': 'v123'}
