#!/usr/bin/env python3
"""Tests for generate_synthetic_dataset.py"""

import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../scripts/s3-tools/synthetic-data'))

try:
    from generate_synthetic_dataset import SyntheticDatasetGenerator
except ImportError:
    pytest.skip("generate_synthetic_dataset.py not found", allow_module_level=True)


class TestSyntheticDatasetGenerator:
    
    @patch('boto3.client')
    def setup_method(self, mock_client):
        self.generator = SyntheticDatasetGenerator('test-bucket')
    
    def test_get_file_sizes(self):
        sizes = self.generator.get_file_sizes(10)
        expected = [1, 5, 10]
        assert all(size in sizes for size in expected)
        assert all(size <= 10 for size in sizes)
    
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.urandom')
    def test_create_base_file(self, mock_urandom, mock_tempfile):
        mock_urandom.return_value = b'x' * 1024
        mock_file = Mock()
        mock_tempfile.return_value.__enter__.return_value = mock_file
        mock_file.name = '/tmp/test'
        
        result = self.generator.create_base_file(1)
        
        assert result == '/tmp/test'
        assert mock_file.write.called
    
    @patch('boto3.client')
    def test_file_exists_in_s3_true(self, mock_client):
        mock_client.return_value.head_object.return_value = {}
        
        generator = SyntheticDatasetGenerator('test-bucket')
        result = generator.file_exists_in_s3('test-key')
        
        assert result is True
