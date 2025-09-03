"""Pytest configuration for s3-tools tests"""

import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing"""
    client = Mock()
    client.head_object.return_value = {'ContentLength': 1024}
    client.get_paginator.return_value.paginate.return_value = []
    return client


@pytest.fixture
def sample_s3_objects():
    """Sample S3 objects for testing"""
    return [
        {
            'Key': 'file1.txt',
            'Size': 1024,
            'LastModified': Mock(isoformat=Mock(return_value='2024-01-01T00:00:00')),
            'ETag': '"abc123"'
        },
        {
            'Key': 'file2.txt', 
            'Size': 2048,
            'LastModified': Mock(isoformat=Mock(return_value='2024-01-02T00:00:00')),
            'ETag': '"def456"',
            'VersionId': 'v123'
        }
    ]
