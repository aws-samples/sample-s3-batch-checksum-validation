#!/usr/bin/env python3
"""
Generate synthetic datasets for testing S3 operations.
Creates files from 1GB to 5TB by concatenating smaller files using multipart uploads.

S3 Multipart Upload Constraints:
- Minimum part size: 5MB (except last part)
- Maximum part size: 5GB
- Maximum parts: 10,000
- Maximum object size: 5TB

Strategy (all parts stay within 5MB-5GB range):
- 1GB: Simple upload or small multipart (generated locally)
- 5GB: 5 × 1GB parts (5 parts, each 1GB)
- 10GB: 2 × 5GB parts (2 parts, each 5GB)
- 50GB: 10 × 5GB parts (10 parts, each 5GB)
- 100GB: 20 × 5GB parts (20 parts, each 5GB)
- 500GB: 100 × 5GB parts (100 parts, each 5GB)
- 1TB: 200 × 5GB parts (200 parts, each 5GB)
- 5TB: 1000 × 5GB parts (1000 parts, each 5GB at maximum part size)

This ensures all operations stay well within S3 limits while creating realistic test data.
The largest files use the maximum allowed part size (5GB) to minimize the number of parts.
"""

import os
import sys
import boto3
import hashlib
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
import time
from typing import List, Tuple


class SyntheticDatasetGenerator:
    def __init__(self, bucket_name: str):
        """Initialize the dataset generator using default AWS profile and region."""
        self.bucket_name = bucket_name
        # Always use default AWS profile and region configuration
        self.s3_client = boto3.client('s3')
        
        # Verify bucket exists and get region
        try:
            response = self.s3_client.head_bucket(Bucket=bucket_name)
            bucket_region = self.s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
            if bucket_region is None:
                bucket_region = 'us-east-1'
            print(f"Using bucket: {bucket_name} in region: {bucket_region}")
        except ClientError as e:
            print(f"Error accessing bucket {bucket_name}: {e}")
            sys.exit(1)

    def generate_local_file(self, size_gb: int, filename: str) -> str:
        """Generate a local file with random data."""
        print(f"Generating {size_gb}GB local file: {filename}")
        
        # Generate 1MB chunks to avoid memory issues
        chunk_size = 1024 * 1024  # 1MB
        total_chunks = size_gb * 1024  # 1GB = 1024 MB
        
        with open(filename, 'wb') as f:
            for i in range(total_chunks):
                # Generate pseudo-random data based on chunk index for consistency
                chunk_data = hashlib.sha256(f"chunk_{i}".encode()).digest() * (chunk_size // 32)
                f.write(chunk_data[:chunk_size])
                
                if (i + 1) % 100 == 0:  # Progress every 100MB
                    progress = (i + 1) / total_chunks * 100
                    print(f"  Progress: {progress:.1f}% ({i + 1}/{total_chunks} chunks)")
        
        print(f"Generated {filename} ({size_gb}GB)")
        return filename

    def calculate_optimal_part_size(self, file_size: int, target_parts: int = 1000) -> int:
        """Calculate optimal part size for multipart upload."""
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB minimum
        MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum
        MAX_PARTS = 10000  # S3 limit
        
        # Start with target number of parts
        part_size = (file_size + target_parts - 1) // target_parts
        
        # Ensure minimum part size
        part_size = max(part_size, MIN_PART_SIZE)
        
        # Ensure maximum part size
        part_size = min(part_size, MAX_PART_SIZE)
        
        # Calculate actual number of parts with constrained part size
        num_parts = (file_size + part_size - 1) // part_size
        
        # If we exceed max parts due to part size constraint, we have a problem
        if num_parts > MAX_PARTS:
            # Calculate minimum part size needed to stay within part limit
            min_required_part_size = (file_size + MAX_PARTS - 1) // MAX_PARTS
            
            if min_required_part_size > MAX_PART_SIZE:
                # File is too large for S3 multipart upload
                raise ValueError(f"File size {file_size / (1024**4):.2f}TB is too large. "
                               f"Would require part size of {min_required_part_size / (1024**3):.2f}GB "
                               f"which exceeds maximum part size of {MAX_PART_SIZE / (1024**3):.0f}GB")
            
            part_size = min_required_part_size
            # Round up to nearest MB for cleaner part sizes
            part_size = ((part_size + 1024 * 1024 - 1) // (1024 * 1024)) * (1024 * 1024)
            part_size = min(part_size, MAX_PART_SIZE)
        
        return part_size

    def object_exists(self, s3_key: str) -> bool:
        """Check if an S3 object exists."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                # Re-raise other errors
                raise

    def upload_file_multipart(self, local_file: str, s3_key: str, part_size: int = None, skip_if_exists: bool = True) -> bool:
        """Upload a file using multipart upload with progress tracking."""
        # Check if file already exists
        if skip_if_exists and self.object_exists(s3_key):
            print(f"Skipping upload - {s3_key} already exists")
            return True
            
        print(f"Uploading {local_file} to s3://{self.bucket_name}/{s3_key}")
        
        file_size = os.path.getsize(local_file)
        
        # S3 multipart upload constraints
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB minimum (except last part)
        MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum
        MAX_PARTS = 10000  # S3 limit
        
        # Calculate optimal part size if not provided
        if part_size is None:
            try:
                part_size = self.calculate_optimal_part_size(file_size)
            except ValueError as e:
                print(f"  Error: {e}")
                return False
        else:
            # Validate provided part size
            if part_size > MAX_PART_SIZE:
                print(f"  Error: Specified part size {part_size / (1024**3):.2f}GB exceeds maximum {MAX_PART_SIZE / (1024**3):.0f}GB")
                return False
            if part_size < MIN_PART_SIZE:
                print(f"  Warning: Part size {part_size / (1024**2):.1f}MB is below recommended minimum {MIN_PART_SIZE / (1024**2):.0f}MB")
        
        # Calculate number of parts with validated part size
        num_parts = (file_size + part_size - 1) // part_size
        
        # For files smaller than minimum multipart size, use simple upload
        if file_size < MIN_PART_SIZE:
            try:
                self.s3_client.upload_file(local_file, self.bucket_name, s3_key)
                print(f"  Uploaded {s3_key} (simple upload - {file_size / (1024**2):.1f}MB)")
                return True
            except ClientError as e:
                print(f"  Error uploading {s3_key}: {e}")
                return False
        
        print(f"  File size: {file_size / (1024**3):.2f}GB, Parts: {num_parts}, Part size: {part_size / (1024**2):.1f}MB")
        
        # Final validation of constraints
        if num_parts > MAX_PARTS:
            print(f"  Error: File would require {num_parts} parts (max {MAX_PARTS})")
            return False
        
        if part_size > MAX_PART_SIZE:
            print(f"  Error: Part size {part_size / (1024**3):.2f}GB exceeds maximum {MAX_PART_SIZE / (1024**3):.0f}GB")
            return False
        
        try:
            # Initiate multipart upload
            response = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            upload_id = response['UploadId']
            
            def upload_part(part_info: Tuple[int, int, int]) -> dict:
                """Upload a single part."""
                part_number, start_byte, part_size_actual = part_info
                
                with open(local_file, 'rb') as f:
                    f.seek(start_byte)
                    data = f.read(part_size_actual)
                
                response = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                
                return {
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                }
            
            # Prepare part information
            parts_info = []
            for i in range(num_parts):
                start_byte = i * part_size
                remaining = file_size - start_byte
                part_size_actual = min(part_size, remaining)
                parts_info.append((i + 1, start_byte, part_size_actual))
            
            # Upload parts concurrently
            parts = []
            max_workers = min(10, num_parts)  # Limit concurrent uploads
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_part = {executor.submit(upload_part, part_info): part_info[0] 
                                for part_info in parts_info}
                
                completed = 0
                for future in as_completed(future_to_part):
                    part_number = future_to_part[future]
                    try:
                        part_result = future.result()
                        parts.append(part_result)
                        completed += 1
                        progress = completed / num_parts * 100
                        print(f"    Part {part_number}/{num_parts} uploaded ({progress:.1f}%)")
                    except Exception as e:
                        print(f"    Error uploading part {part_number}: {e}")
                        # Abort multipart upload on error
                        self.s3_client.abort_multipart_upload(
                            Bucket=self.bucket_name,
                            Key=s3_key,
                            UploadId=upload_id
                        )
                        return False
            
            # Sort parts by part number
            parts.sort(key=lambda x: x['PartNumber'])
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            print(f"  Successfully uploaded {s3_key}")
            return True
            
        except ClientError as e:
            print(f"  Error with multipart upload for {s3_key}: {e}")
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    UploadId=upload_id
                )
            except:
                pass
            return False

    def concatenate_s3_objects(self, source_keys: List[str], dest_key: str, skip_if_exists: bool = True) -> bool:
        """Concatenate multiple S3 objects into a single object using multipart upload."""
        # Check if destination already exists
        if skip_if_exists and self.object_exists(dest_key):
            print(f"Skipping concatenation - {dest_key} already exists")
            return True
        
        # S3 multipart upload constraints
        MAX_PARTS = 10000  # S3 limit
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB minimum (except last part)
        MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum
        
        # Check if we have too many source objects for multipart upload
        if len(source_keys) > MAX_PARTS:
            print(f"Error: Cannot concatenate {len(source_keys)} objects - exceeds S3 limit of {MAX_PARTS} parts")
            return False
        
        # Get sizes of source objects to validate they meet size constraints
        source_sizes = []
        total_size = 0
        
        print(f"Validating {len(source_keys)} source objects...")
        for i, source_key in enumerate(source_keys):
            try:
                response = self.s3_client.head_object(Bucket=self.bucket_name, Key=source_key)
                size = response['ContentLength']
                source_sizes.append(size)
                total_size += size
                
                # Check minimum part size (except for the last part)
                if i < len(source_keys) - 1 and size < MIN_PART_SIZE:
                    print(f"Error: Source object {source_key} is {size / (1024**2):.1f}MB, "
                          f"below minimum part size of {MIN_PART_SIZE / (1024**2):.1f}MB")
                    return False
                
                # Check maximum part size
                if size > MAX_PART_SIZE:
                    print(f"Error: Source object {source_key} is {size / (1024**3):.2f}GB, "
                          f"exceeds maximum part size of {MAX_PART_SIZE / (1024**3):.0f}GB")
                    return False
                    
            except ClientError as e:
                print(f"Error getting size of {source_key}: {e}")
                return False
        
        print(f"Concatenating {len(source_keys)} objects into {dest_key}")
        print(f"  Total size: {total_size / (1024**3):.2f}GB, Parts: {len(source_keys)}")
        
        # Validate total size doesn't exceed S3 object size limit (5TB)
        MAX_OBJECT_SIZE = 5 * 1024**4  # 5TB
        if total_size > MAX_OBJECT_SIZE:
            print(f"Error: Total size {total_size / (1024**4):.2f}TB exceeds S3 object size limit of 5TB")
            return False
        
        # Show part size validation info
        min_part_gb = min(source_sizes) / (1024**3)
        max_part_gb = max(source_sizes) / (1024**3)
        print(f"  Part sizes: {min_part_gb:.1f}GB to {max_part_gb:.1f}GB (limits: {MIN_PART_SIZE / (1024**2):.0f}MB - {MAX_PART_SIZE / (1024**3):.0f}GB)")
        
        # Validate all parts are within size limits
        for size in source_sizes[:-1]:  # All but last part must meet minimum
            if size < MIN_PART_SIZE:
                print(f"Error: Part size {size / (1024**2):.1f}MB below minimum {MIN_PART_SIZE / (1024**2):.0f}MB")
                return False
        
        for size in source_sizes:  # All parts must be under maximum
            if size > MAX_PART_SIZE:
                print(f"Error: Part size {size / (1024**3):.2f}GB exceeds maximum {MAX_PART_SIZE / (1024**3):.0f}GB")
                return False
        
        try:
            # Initiate multipart upload for destination
            response = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=dest_key
            )
            upload_id = response['UploadId']
            
            def copy_part(part_info: Tuple[int, str]) -> dict:
                """Copy a source object as a part of the destination."""
                part_number, source_key = part_info
                
                copy_source = {
                    'Bucket': self.bucket_name,
                    'Key': source_key
                }
                
                response = self.s3_client.upload_part_copy(
                    Bucket=self.bucket_name,
                    Key=dest_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    CopySource=copy_source
                )
                
                return {
                    'ETag': response['CopyPartResult']['ETag'],
                    'PartNumber': part_number
                }
            
            # Copy each source object as a part
            parts = []
            max_workers = min(10, len(source_keys))
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_part = {executor.submit(copy_part, (i + 1, source_key)): i + 1 
                                for i, source_key in enumerate(source_keys)}
                
                completed = 0
                for future in as_completed(future_to_part):
                    part_number = future_to_part[future]
                    try:
                        part_result = future.result()
                        parts.append(part_result)
                        completed += 1
                        progress = completed / len(source_keys) * 100
                        print(f"    Part {part_number}/{len(source_keys)} copied ({progress:.1f}%)")
                    except Exception as e:
                        print(f"    Error copying part {part_number}: {e}")
                        # Abort multipart upload on error
                        self.s3_client.abort_multipart_upload(
                            Bucket=self.bucket_name,
                            Key=dest_key,
                            UploadId=upload_id
                        )
                        return False
            
            # Sort parts by part number
            parts.sort(key=lambda x: x['PartNumber'])
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=dest_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            print(f"  Successfully created {dest_key}")
            return True
            
        except ClientError as e:
            print(f"  Error concatenating objects into {dest_key}: {e}")
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=dest_key,
                    UploadId=upload_id
                )
            except:
                pass
            return False

    def generate_dataset(self, prefix: str = "synthetic-data/", cleanup_local: bool = True, skip_existing: bool = True):
        """Generate the complete synthetic dataset from 1GB to 5TB."""
        print(f"Starting synthetic dataset generation in s3://{self.bucket_name}/{prefix}")
        if skip_existing:
            print("Will skip files that already exist")
        
        # S3 multipart constraints
        MIN_PART_SIZE = 5 * 1024 * 1024  # 5MB minimum
        MAX_PART_SIZE = 5 * 1024 * 1024 * 1024  # 5GB maximum
        MAX_PARTS = 10000  # S3 limit
        MAX_OBJECT_SIZE = 5 * 1024**4  # 5TB limit
        
        # Step 1: Generate 1GB file locally
        local_1gb = "synthetic_1gb.bin"
        if not os.path.exists(local_1gb):
            self.generate_local_file(1, local_1gb)
        else:
            print(f"Using existing local file: {local_1gb}")
        
        # Step 2: Upload 1GB file to S3
        s3_1gb = f"{prefix}1gb.bin"
        if not self.upload_file_multipart(local_1gb, s3_1gb, skip_if_exists=skip_existing):
            print("Failed to upload 1GB file")
            return False
        
        # Define the file generation plan
        # Each entry: (size_name, size_gb, source_file, multiplier, description)
        generation_plan = [
            ("5gb", 5, s3_1gb, 5, "5 × 1GB parts"),
            ("10gb", 10, f"{prefix}5gb.bin", 2, "2 × 5GB parts"),
            ("50gb", 50, f"{prefix}5gb.bin", 10, "10 × 5GB parts"),
            ("100gb", 100, f"{prefix}5gb.bin", 20, "20 × 5GB parts"),
            ("500gb", 500, f"{prefix}5gb.bin", 100, "100 × 5GB parts"),
            ("1tb", 1000, f"{prefix}5gb.bin", 200, "200 × 5GB parts"),
            ("5tb", 5000, f"{prefix}5gb.bin", 1000, "1000 × 5GB parts"),
        ]
        
        # Generate each file according to the plan
        for size_name, size_gb, source_file, multiplier, description in generation_plan:
            print(f"\n--- Creating {size_gb}GB file ({size_name}) ---")
            dest_key = f"{prefix}{size_name}.bin"
            
            # Skip if already exists
            if skip_existing and self.object_exists(dest_key):
                print(f"Skipping {dest_key} - already exists")
                continue
            
            # Check if source file exists
            if not self.object_exists(source_file):
                print(f"Missing source file: {source_file}")
                print(f"Skipping {size_name} file creation...")
                continue
            
            # Validate the plan against S3 constraints
            total_size_bytes = size_gb * 1024**3
            source_size_bytes = total_size_bytes // multiplier
            
            # Check total size limit
            if total_size_bytes > MAX_OBJECT_SIZE:
                print(f"Error: {size_gb}GB file exceeds S3 object size limit of {MAX_OBJECT_SIZE / (1024**4):.0f}TB")
                continue
            
            # Check part count limit
            if multiplier > MAX_PARTS:
                print(f"Error: {size_gb}GB file would require {multiplier} parts (max {MAX_PARTS})")
                continue
            
            # Check part size limits
            if source_size_bytes > MAX_PART_SIZE:
                print(f"Error: Source file size {source_size_bytes / (1024**3):.1f}GB exceeds max part size {MAX_PART_SIZE / (1024**3):.0f}GB")
                continue
            
            if multiplier > 1 and source_size_bytes < MIN_PART_SIZE:
                print(f"Error: Source file size {source_size_bytes / (1024**2):.1f}MB below min part size {MIN_PART_SIZE / (1024**2):.0f}MB")
                continue
            
            # Create source file list
            source_files = [source_file] * multiplier
            
            print(f"Creating {size_gb}GB file with {description}")
            print(f"Validation:")
            print(f"  - Parts: {multiplier} (max {MAX_PARTS})")
            print(f"  - Part size: {source_size_bytes / (1024**3):.1f}GB (range: {MIN_PART_SIZE / (1024**2):.0f}MB - {MAX_PART_SIZE / (1024**3):.0f}GB)")
            print(f"  - Total size: {size_gb}GB (max {MAX_OBJECT_SIZE / (1024**4):.0f}TB)")
            
            # Create the file
            if not self.concatenate_s3_objects(source_files, dest_key, skip_if_exists=skip_existing):
                print(f"Failed to create {size_gb}GB file")
                continue
            
            print(f"Successfully created {dest_key} ({size_gb}GB)")
        
        # Cleanup local file if requested
        if cleanup_local and os.path.exists(local_1gb):
            os.remove(local_1gb)
            print(f"Cleaned up local file: {local_1gb}")
        
        print("\nDataset generation complete!")
        
        # List final results with validation info
        print("\nGenerated files:")
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                # Sort by size for better display
                objects = sorted(response['Contents'], key=lambda x: x['Size'])
                
                for obj in objects:
                    size_gb = obj['Size'] / (1024**3)
                    
                    # Calculate what the multipart characteristics would be
                    if obj['Size'] >= MIN_PART_SIZE:
                        try:
                            optimal_part_size = self.calculate_optimal_part_size(obj['Size'])
                            num_parts = (obj['Size'] + optimal_part_size - 1) // optimal_part_size
                            print(f"  s3://{self.bucket_name}/{obj['Key']} ({size_gb:.1f}GB)")
                            print(f"    Multipart: {num_parts} parts × {optimal_part_size / (1024**2):.1f}MB")
                        except ValueError as e:
                            print(f"  s3://{self.bucket_name}/{obj['Key']} ({size_gb:.1f}GB) - ERROR: {e}")
                    else:
                        print(f"  s3://{self.bucket_name}/{obj['Key']} ({size_gb:.3f}GB) - simple upload")
                
                # Summary statistics
                total_size_tb = sum(obj['Size'] for obj in objects) / (1024**4)
                print(f"\nSummary:")
                print(f"  Total files: {len(objects)}")
                print(f"  Total size: {total_size_tb:.2f}TB")
                print(f"  Size range: {min(obj['Size'] for obj in objects) / (1024**3):.1f}GB - {max(obj['Size'] for obj in objects) / (1024**3):.1f}GB")
                
            else:
                print("  No files found with the specified prefix")
        except ClientError as e:
            print(f"Error listing final results: {e}")
        
        return True


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic datasets for S3 testing")
    parser.add_argument("bucket", nargs='?', help="S3 bucket name (optional if S3_STD_MV_TEST_BUCKET is set)")
    parser.add_argument("--prefix", default="synthetic-data/", 
                       help="S3 key prefix for generated files (default: synthetic-data/)")
    parser.add_argument("--force", action="store_true",
                       help="Overwrite existing files instead of skipping them")
    
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
    
    generator = SyntheticDatasetGenerator(bucket_name=bucket_name)
    
    generator.generate_dataset(
        prefix=args.prefix,
        cleanup_local=True,  # Always cleanup local files
        skip_existing=not args.force
    )


if __name__ == "__main__":
    main()
