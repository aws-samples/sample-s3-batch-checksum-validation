#!/usr/bin/env python3
"""
Simple integration test to verify synthetic data setup works with real S3 tests.
"""

import os
import sys
from pathlib import Path

# Add the real_tests directory to the path
REAL_TESTS_DIR = Path(__file__).parent / "real_tests"
sys.path.insert(0, str(REAL_TESTS_DIR))

def test_imports():
    """Test that we can import the synthetic data generator."""
    try:
        from generate_synthetic_dataset import SyntheticDatasetGenerator
        print("✓ Successfully imported SyntheticDatasetGenerator")
        return True
    except ImportError as e:
        print(f"✗ Failed to import SyntheticDatasetGenerator: {e}")
        return False

def test_real_tests_directory():
    """Test that real_tests directory structure is correct."""
    required_files = [
        "setup_real_tests.sh",
        "setup_synthetic_data.sh", 
        "generate_synthetic_dataset.py",
        "test_s3_std_mv.py",
        "validate_dataset.py"
    ]
    
    missing_files = []
    for filename in required_files:
        file_path = REAL_TESTS_DIR / filename
        if not file_path.exists():
            missing_files.append(filename)
    
    if missing_files:
        print(f"✗ Missing files in real_tests/: {missing_files}")
        return False
    else:
        print("✓ All required files present in real_tests/")
        return True

def test_environment_setup():
    """Test environment variable handling."""
    from test_real_s3 import get_test_bucket_name, should_skip_real_tests
    
    # Test bucket name generation
    bucket_name = get_test_bucket_name()
    print(f"✓ Generated bucket name: {bucket_name}")
    
    # Test skip configuration
    skip_tests = should_skip_real_tests()
    print(f"✓ Skip real tests: {skip_tests}")
    
    return True

def main():
    """Run all integration tests."""
    print("Testing synthetic data integration with real S3 tests...")
    print("=" * 60)
    
    tests = [
        ("Import Test", test_imports),
        ("Directory Structure", test_real_tests_directory),
        ("Environment Setup", test_environment_setup)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            if test_func():
                passed += 1
                print(f"✓ {test_name} PASSED")
            else:
                failed += 1
                print(f"✗ {test_name} FAILED")
        except Exception as e:
            failed += 1
            print(f"✗ {test_name} FAILED with exception: {e}")
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("✓ All integration tests passed!")
        return 0
    else:
        print("✗ Some integration tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
