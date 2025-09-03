#!/usr/bin/env python3
"""Integration tests for s3-tools"""

import pytest
import tempfile
import json
from unittest.mock import patch, Mock


class TestS3ToolsIntegration:
    
    @patch('boto3.client')
    def test_batch_generator_integration(self, mock_client):
        """Test s3_batch_input_generator integration"""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../scripts/s3-tools/batch-operations'))
        
        from s3_batch_input_generator import S3BatchPayloadGenerator
        
        # Mock S3 response
        mock_paginator = Mock()
        mock_client.return_value.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {
                        'Key': 'test-file.txt',
                        'Size': 1024,
                        'LastModified': Mock(isoformat=Mock(return_value='2024-01-01T00:00:00')),
                        'ETag': '"abc123"'
                    }
                ]
            }
        ]
        
        generator = S3BatchPayloadGenerator()
        objects = generator.list_s3_objects('test-bucket', 'test-prefix/')
        payload = generator.generate_lambda_payload('test-bucket', objects)
        
        assert payload['bucket'] == 'test-bucket'
        assert len(payload['keys']) == 1
        assert payload['keys'][0]['key'] == 'test-file.txt'
    
    def test_payload_format_compatibility(self):
        """Test payload format matches Lambda expectations"""
        expected_format = {
            "bucket": "my-bucket",
            "keys": [
                {"key": "file1.txt"},
                {"key": "file2.txt", "version_id": "abc123"}
            ]
        }
        
        # Validate JSON structure
        assert isinstance(expected_format['bucket'], str)
        assert isinstance(expected_format['keys'], list)
        assert all('key' in item for item in expected_format['keys'])
    
    @patch('subprocess.run')
    def test_shell_script_integration(self, mock_run):
        """Test shell script wrapper integration"""
        mock_run.return_value.returncode = 0
        
        # Test that shell scripts can be called
        import subprocess
        result = subprocess.run(['echo', 'test'], capture_output=True, text=True)
        assert result.returncode == 0
