#!/usr/bin/env python3
"""
S3 Batch Operations JSON Payload Generator

This utility takes an S3 path as input, lists all objects in that path,
and generates a JSON payload suitable for the S3 batch checksum Lambda function.
"""

import argparse
import json
import sys
from typing import List, Dict, Optional
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3BatchPayloadGenerator:
    """Generator for S3 batch operations JSON payloads."""
    
    def __init__(self):
        """Initialize the S3 client."""
        try:
            self.s3_client = boto3.client('s3')
        except NoCredentialsError:
            print("Error: AWS credentials not found. Please configure your credentials.")
            sys.exit(1)
    
    def parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and prefix.
        
        Args:
            s3_path: S3 path in format s3://bucket/prefix
            
        Returns:
            Tuple of (bucket, prefix)
            
        Raises:
            ValueError: If the S3 path format is invalid
        """
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        
        if not bucket:
            raise ValueError("Invalid S3 path: bucket name is required")
        
        return bucket, prefix
    
    def list_s3_objects(self, bucket: str, prefix: str) -> List[Dict[str, str]]:
        """List objects in the specified S3 path.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 object prefix
            
        Returns:
            List of dictionaries containing object information
        """
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append({
                            'bucket': bucket,
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'etag': obj['ETag'].strip('"'),
                            'version_id': obj.get('VersionId')  # Include version if available
                        })
            
            print(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")
            return objects
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{bucket}' does not exist")
            elif error_code == 'AccessDenied':
                print(f"Error: Access denied to bucket '{bucket}'")
            else:
                print(f"Error listing objects: {e}")
            sys.exit(1)
    
    def generate_lambda_payload(self, bucket: str, objects: List[Dict[str, str]]) -> Dict:
        """Generate JSON payload for S3 batch checksum Lambda function.
        
        Args:
            bucket: S3 bucket name
            objects: List of S3 objects
            
        Returns:
            Dictionary containing the Lambda payload
        """
        keys = []
        
        for obj in objects:
            key_obj = {"key": obj['key']}
            
            # Add version_id if present
            if obj.get('version_id'):
                key_obj["version_id"] = obj['version_id']
            
            keys.append(key_obj)
        
        payload = {
            "bucket": bucket,
            "keys": keys
        }
        
        return payload
    
    def save_payload_locally(self, payload: Dict, filename: str) -> None:
        """Save JSON payload to local file.
        
        Args:
            payload: JSON payload to save
            filename: Local filename
        """
        try:
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(payload, jsonfile, indent=2)
            
            print(f"JSON payload saved locally as: {filename}")
            
        except IOError as e:
            print(f"Error saving JSON file: {e}")
            sys.exit(1)
    
    def print_payload(self, payload: Dict) -> None:
        """Print JSON payload to stdout.
        
        Args:
            payload: JSON payload to print
        """
        print("\n" + "="*50)
        print("LAMBDA PAYLOAD:")
        print("="*50)
        print(json.dumps(payload, indent=2))
        print("="*50)


def main():
    """Main entry point for the S3 batch payload generator."""
    parser = argparse.ArgumentParser(
        description="Generate JSON payloads for S3 batch checksum Lambda function",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate payload and print to stdout
  python s3_batch_input_generator.py s3://my-bucket/data/
  
  # Generate payload and save to file
  python s3_batch_input_generator.py s3://my-bucket/data/ --output /tmp/payload.json
  
  # Generate payload with object limit
  python s3_batch_input_generator.py s3://my-bucket/logs/ --max-objects 1000
        """
    )
    
    parser.add_argument(
        'input_path',
        help='S3 path to list objects from (e.g., s3://bucket/prefix/)'
    )
    
    parser.add_argument(
        '--output',
        help='Local filename for the JSON output (default: print to stdout)'
    )
    
    parser.add_argument(
        '--max-objects',
        type=int,
        help='Maximum number of objects to include in payload'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress informational output (only show payload)'
    )
    
    args = parser.parse_args()
    
    # Validate input path
    if not args.input_path.startswith('s3://'):
        if not args.quiet:
            print("Error: Input path must be an S3 path (s3://bucket/prefix)")
        sys.exit(1)
    
    # Initialize generator
    generator = S3BatchPayloadGenerator()
    
    try:
        # Parse input path
        input_bucket, input_prefix = generator.parse_s3_path(args.input_path)
        if not args.quiet:
            print(f"Listing objects from s3://{input_bucket}/{input_prefix}")
        
        # List objects
        objects = generator.list_s3_objects(input_bucket, input_prefix)
        
        if not objects:
            if not args.quiet:
                print("No objects found in the specified path")
            sys.exit(0)
        
        # Limit objects if specified
        if args.max_objects and len(objects) > args.max_objects:
            objects = objects[:args.max_objects]
            if not args.quiet:
                print(f"Limited to first {args.max_objects} objects")
        
        # Generate Lambda payload
        payload = generator.generate_lambda_payload(input_bucket, objects)
        
        # Save to file if specified
        if args.output:
            generator.save_payload_locally(payload, args.output)
        
        # Print payload if not saving to file or if not quiet
        if not args.output or not args.quiet:
            generator.print_payload(payload)
        
        if not args.quiet:
            print(f"\nSuccessfully generated payload with {len(objects)} objects")
        
    except ValueError as e:
        if not args.quiet:
            print(f"Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        if not args.quiet:
            print("\nOperation cancelled by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
