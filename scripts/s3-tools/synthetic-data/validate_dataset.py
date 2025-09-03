#!/usr/bin/env python3
"""
Validate the synthetic dataset to ensure all files meet S3 multipart constraints.
"""

import os
import sys
import subprocess
import boto3
import argparse
from botocore.exceptions import ClientError


def validate_multipart_constraints(size_bytes: int) -> dict:
    """Validate a file size against S3 multipart constraints."""
    MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB minimum
    MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum
    MAX_PARTS = 10000  # S3 limit
    MAX_OBJECT_SIZE = 5 * 1024**4  # 5TB limit
    
    result = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'info': {}
    }
    
    # Check object size limit
    if size_bytes > MAX_OBJECT_SIZE:
        result['valid'] = False
        result['errors'].append(f"Object size {size_bytes / (1024**4):.2f}TB exceeds 5TB limit")
        return result
    
    # For small files, simple upload is fine
    if size_bytes < MIN_PART_SIZE:
        result['info']['upload_type'] = 'simple'
        result['info']['reason'] = f'File size {size_bytes / (1024**2):.1f}MB < 5MB minimum for multipart'
        return result
    
    # Calculate optimal part size
    # Start with 100MB default
    part_size = 100 * 1024 * 1024
    
    # Ensure minimum part size
    part_size = max(part_size, MIN_PART_SIZE)
    
    # Ensure maximum part size
    part_size = min(part_size, MAX_PART_SIZE)
    
    # Calculate number of parts
    num_parts = (size_bytes + part_size - 1) // part_size
    
    # If too many parts, increase part size
    if num_parts > MAX_PARTS:
        part_size = (size_bytes + MAX_PARTS - 1) // MAX_PARTS
        part_size = min(part_size, MAX_PART_SIZE)
        num_parts = (size_bytes + part_size - 1) // part_size
        
        if num_parts > MAX_PARTS:
            result['valid'] = False
            result['errors'].append(f"Cannot fit in {MAX_PARTS} parts even with max part size")
            return result
    
    result['info']['upload_type'] = 'multipart'
    result['info']['part_size'] = part_size
    result['info']['num_parts'] = num_parts
    result['info']['part_size_mb'] = part_size / (1024**2)
    result['info']['part_size_gb'] = part_size / (1024**3)
    
    # Validate constraints
    if part_size > MAX_PART_SIZE:
        result['valid'] = False
        result['errors'].append(f"Required part size {part_size / (1024**3):.2f}GB exceeds 5GB limit")
    
    if part_size < MIN_PART_SIZE:
        result['valid'] = False
        result['errors'].append(f"Part size {part_size / (1024**2):.1f}MB below 5MB minimum")
    
    if num_parts > MAX_PARTS:
        result['valid'] = False
        result['errors'].append(f"Requires {num_parts} parts, exceeds {MAX_PARTS} limit")
    
    # Warnings for edge cases
    if num_parts > 1000:
        result['warnings'].append(f"High part count ({num_parts}) may impact performance")
    
    if part_size == MAX_PART_SIZE:
        result['warnings'].append("Using maximum part size (5GB)")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Validate synthetic dataset")
    parser.add_argument("bucket", nargs='?', help="S3 bucket name (optional if S3_STD_MV_TEST_BUCKET is set)")
    parser.add_argument("--prefix", default="synthetic-data/", 
                       help="S3 key prefix for files to validate")
    
    args = parser.parse_args()
    
    # Get bucket name from argument, environment, or generate default
    bucket_name = args.bucket or os.environ.get('S3_STD_MV_TEST_BUCKET')
    
    if not bucket_name:
        # Generate default bucket name using same pattern as setup script
        try:
            import subprocess
            account_id = subprocess.check_output(
                ['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'],
                stderr=subprocess.DEVNULL
            ).decode().strip()
            
            region = subprocess.check_output(
                ['aws', 'configure', 'get', 'region'],
                stderr=subprocess.DEVNULL
            ).decode().strip()
            
            if not region:
                region = 'us-east-1'
                
            bucket_name = f"{account_id}-{region}-s3-std-mv-test"
            print(f"Using auto-generated bucket name: {bucket_name}")
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: Could not determine bucket name automatically.")
            print("Please either:")
            print("  1. Set S3_STD_MV_TEST_BUCKET environment variable")
            print("  2. Provide bucket name as argument")
            print("  3. Ensure AWS CLI is configured with valid credentials")
            sys.exit(1)
    
    # Initialize S3 client using default AWS profile and region
    s3_client = boto3.client('s3')
    
    print(f"Validating synthetic dataset in s3://{bucket_name}/{args.prefix}")
    print("=" * 80)
    
    try:
        # List objects
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=args.prefix
        )
        
        if 'Contents' not in response:
            print("No files found with the specified prefix")
            return
        
        # Sort by size
        objects = sorted(response['Contents'], key=lambda x: x['Size'])
        
        total_valid = 0
        total_invalid = 0
        total_size = 0
        
        for obj in objects:
            key = obj['Key']
            size = obj['Size']
            size_gb = size / (1024**3)
            total_size += size
            
            print(f"\nFile: {key}")
            print(f"Size: {size_gb:.1f}GB ({size:,} bytes)")
            
            # Validate constraints
            validation = validate_multipart_constraints(size)
            
            if validation['valid']:
                print("✅ VALID")
                total_valid += 1
                
                info = validation['info']
                if info['upload_type'] == 'simple':
                    print(f"   Upload: Simple upload ({info['reason']})")
                else:
                    print(f"   Upload: Multipart ({info['num_parts']} parts × {info['part_size_mb']:.1f}MB)")
                    
                    # Show efficiency metrics
                    efficiency = (info['num_parts'] * info['part_size']) / size * 100
                    print(f"   Efficiency: {efficiency:.1f}% (minimal overhead)")
                
            else:
                print("❌ INVALID")
                total_invalid += 1
                for error in validation['errors']:
                    print(f"   ERROR: {error}")
            
            # Show warnings
            for warning in validation['warnings']:
                print(f"   ⚠️  WARNING: {warning}")
        
        # Summary
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        print(f"Total files: {len(objects)}")
        print(f"Valid files: {total_valid}")
        print(f"Invalid files: {total_invalid}")
        print(f"Total dataset size: {total_size / (1024**4):.2f}TB")
        
        if total_invalid == 0:
            print("\n🎉 All files are valid for S3 multipart upload!")
            print("The dataset is ready for testing s3-std-mv operations.")
        else:
            print(f"\n⚠️  {total_invalid} files have constraint violations.")
            print("These files may fail during s3-std-mv operations.")
        
        # Cost estimate (rough)
        monthly_storage_cost = total_size * 0.023 / (1024**4)  # $0.023/GB/month for Standard
        print(f"\nEstimated monthly storage cost (S3 Standard): ${monthly_storage_cost:.2f}")
        
    except ClientError as e:
        print(f"Error accessing bucket: {e}")
        return 1
    
    return 0 if total_invalid == 0 else 1


if __name__ == "__main__":
    exit(main())
