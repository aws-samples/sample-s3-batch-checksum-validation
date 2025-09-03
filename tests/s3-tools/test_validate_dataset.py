#!/usr/bin/env python3
"""Tests for validate_dataset.py"""

import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add the s3-tools directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../scripts/s3-tools/synthetic-data'))

try:
    from validate_dataset import DatasetValidator, main
except ImportError:
    pytest.skip("validate_dataset.py not found", allow_module_level=True)


class TestDatasetValidator:
    
    @patch('boto3.client')
    def setup_method(self, mock_client):
        """Setup test fixtures"""
        self.validator = DatasetValidator('test-bucket')
    
    def test_validate_file_size_valid(self):
        """Test file size validation - valid sizes"""
        # Test valid S3 multipart sizes
        assert self.validator.validate_file_size(5 * 1024**3) is True  # 5GB
        assert self.validator.validate_file_size(1 * 1024**3) is True  # 1GB
        assert self.validator.validate_file_size(100 * 1024**3) is True  # 100GB
    
    def test_validate_file_size_invalid(self):
        """Test file size validation - invalid sizes"""
        # Test sizes that don't align with multipart constraints
        assert self.validator.validate_file_size(0) is False  # Empty file
        assert self.validator.validate_file_size(6 * 1024**4) is False  # > 5TB limit
    
    def test_validate_multipart_constraints_valid(self):
        """Test multipart constraint validation - valid"""
        # 10GB file = 2 parts of 5GB each
        result = self.validator.validate_multipart_constraints(10 * 1024**3)
        assert result['valid'] is True
        assert result['parts'] == 2
        assert result['part_size'] == 5 * 1024**3
    
    def test_validate_multipart_constraints_invalid(self):
        """Test multipart constraint validation - invalid"""
        # File too large for S3 limits
        result = self.validator.validate_multipart_constraints(6 * 1024**4)  # 6TB
        assert result['valid'] is False
    
    @patch('boto3.client')
    def test_get_s3_object_info_success(self, mock_client):
        """Test S3 object info retrieval - success"""
        mock_client.return_value.head_object.return_value = {
            'ContentLength': 1024,
            'ETag': '"abc123"',
            'LastModified': Mock()
        }
        
        validator = DatasetValidator('test-bucket')
        info = validator.get_s3_object_info('test-key')
        
        assert info['size'] == 1024
        assert info['etag'] == 'abc123'
    
    @patch('boto3.client')
    def test_get_s3_object_info_not_found(self, mock_client):
        """Test S3 object info retrieval - not found"""
        from botocore.exceptions import ClientError
        mock_client.return_value.head_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )
        
        validator = DatasetValidator('test-bucket')
        info = validator.get_s3_object_info('nonexistent-key')
        
        assert info is None
    
    @patch.object(DatasetValidator, 'get_s3_object_info')
    def test_validate_object_success(self, mock_get_info):
        """Test object validation - success"""
        mock_get_info.return_value = {
            'size': 5 * 1024**3,  # 5GB
            'etag': 'abc123'
        }
        
        result = self.validator.validate_object('5gb.bin')
        
        assert result['valid'] is True
        assert result['size'] == 5 * 1024**3
    
    @patch.object(DatasetValidator, 'get_s3_object_info')
    def test_validate_object_not_found(self, mock_get_info):
        """Test object validation - not found"""
        mock_get_info.return_value = None
        
        result = self.validator.validate_object('missing.bin')
        
        assert result['valid'] is False
        assert 'not found' in result['error'].lower()


@patch('sys.argv', ['validate_dataset.py', 'test-bucket'])
@patch('boto3.client')
def test_main_success(mock_client):
    """Test main function success"""
    with patch.object(DatasetValidator, 'validate_dataset') as mock_validate:
        mock_validate.return_value = {'valid': 5, 'invalid': 0, 'total': 5}
        main()
        mock_validate.assert_called_once()


@patch('sys.argv', ['validate_dataset.py'])
def test_main_missing_args():
    """Test main function with missing arguments"""
    with pytest.raises(SystemExit):
        main()


if __name__ == '__main__':
    pytest.main([__file__])
