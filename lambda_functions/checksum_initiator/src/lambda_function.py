import json
import boto3
import csv
import io
import logging
import os
import uuid
from typing import Dict, Any, List
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients lazily
_s3_client = None
_s3control_client = None
_dynamodb = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
    return _s3_client

def get_s3control_client():
    global _s3control_client
    if _s3control_client is None:
        _s3control_client = boto3.client('s3control')
    return _s3control_client

def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb')
    return _dynamodb

# Environment variable getters
def get_manifest_bucket():
    return os.environ['MANIFEST_BUCKET']

def get_batch_role_arn():
    return os.environ['BATCH_ROLE_ARN']

def get_account_id():
    return os.environ['ACCOUNT_ID']

def get_environment():
    return os.environ['ENVIRONMENT']

def get_checksum_table_name():
    return os.environ['CHECKSUM_TABLE_NAME']


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler for initiating S3 batch checksum operations
    
    This function processes a list of S3 objects and creates S3 Batch Operations jobs
    using the S3ComputeObjectChecksum operation to calculate SHA256 and MD5 checksums.
    The operation computes checksums without modifying the original objects.
    
    Execution order:
    1. Validate input and normalize object keys
    2. Generate CSV manifest file for S3 Batch Operations
    3. Create S3 Batch Operations jobs (SHA256 and MD5)
    4. Create initial DynamoDB tracking entries after successful job creation
    
    Expected event format:
    {
        "bucket": "source-bucket-name",
        "keys": [
            {"key": "file1.txt"},
            {"key": "file2.txt", "version_id": "version123"},
            {"key": "file3.txt", "md5": "expected_md5", "sha256": "expected_sha256"}
        ]
    }
    
    Returns:
        Dict containing statusCode, body with job details, and object count
    """
    try:
        # Generate unique request ID for this processing request
        request_id = str(uuid.uuid4())
        logger.info(f"Starting processing with request_id: {request_id}")
        logger.info(f"Processing event: {json.dumps(event)}")
        
        # Extract bucket and keys from event
        bucket = event.get('bucket')
        keys = event.get('keys', [])
        
        if not bucket or not keys:
            raise ValueError("Event must contain 'bucket' and 'keys' fields")
        
        # Normalize objects format
        objects = []
        for key_info in keys:
            if isinstance(key_info, str):
                objects.append({'bucket': bucket, 'key': key_info})
            elif isinstance(key_info, dict):
                objects.append({
                    'bucket': bucket,
                    'key': key_info['key'],
                    'version_id': key_info.get('version_id'),
                    'provided_md5': key_info.get('md5'),
                    'provided_sha256': key_info.get('sha256')
                })
        
        logger.info(f"Processing {len(objects)} objects from bucket: {bucket}")
        
        # Generate CSV manifest and upload to S3
        manifest_key = generate_csv_manifest(objects)
        
        # Create S3 batch jobs for both algorithms
        job_results = create_batch_jobs(manifest_key, len(objects))
        
        # Create initial DynamoDB entries after successful job creation
        create_initial_checksum_entries(objects, job_results, request_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Batch checksum jobs created successfully',
                'request_id': request_id,
                'manifest_key': manifest_key,
                'object_count': len(objects),
                'jobs': job_results
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def generate_csv_manifest(objects: List[Dict[str, Any]]) -> str:
    """Generate CSV manifest file for S3 batch operations"""
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    manifest_key = f"batch-jobs/manifests/manifest-{timestamp}.csv"
    
    # Create CSV content
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    
    for obj in objects:
        row = [obj['bucket'], obj['key']]
        if obj.get('version_id'):
            row.append(obj['version_id'])
        writer.writerow(row)
    
    # Upload to S3
    get_s3_client().put_object(
        Bucket=get_manifest_bucket(),
        Key=manifest_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv',
        Metadata={
            'generated-by': 'checksum-initiator',
            'object-count': str(len(objects)),
            'created-at': datetime.now(timezone.utc).isoformat()
        }
    )
    
    logger.info(f"Generated manifest: s3://{get_manifest_bucket()}/{manifest_key}")
    return manifest_key


def create_initial_checksum_entries(objects: List[Dict[str, Any]], job_results: List[Dict[str, Any]], request_id: str) -> None:
    """Create initial DynamoDB entries for tracking checksum jobs
    
    Args:
        objects: List of S3 objects to create entries for
        job_results: List of batch job results containing job_id and algorithm
        request_id: Unique request identifier for this processing request
    """
    table = get_dynamodb().Table(get_checksum_table_name())
    current_time = datetime.now(timezone.utc).isoformat()
    ttl_timestamp = int(datetime.now(timezone.utc).timestamp() + (10 * 24 * 60 * 60))  # 10 days
    
    # Create a mapping of algorithm to job_id for logging
    job_id_map = {job['algorithm']: job['job_id'] for job in job_results if job.get('status') == 'created'}
    
    logger.info(f"[{request_id}] Creating DynamoDB entries for {len(objects)} objects with job associations:")
    for algorithm, job_id in job_id_map.items():
        logger.info(f"[{request_id}]   {algorithm} algorithm -> Job ID: {job_id}")
    
    with table.batch_writer() as batch:
        for obj in objects:
            for algorithm in ['SHA256', 'MD5']:
                object_key = f"{obj['bucket']}#{obj['key']}#{algorithm}"
                job_id = job_id_map.get(algorithm, 'UNKNOWN')
                
                item = {
                    'object_key': object_key,
                    'bucket': obj['bucket'],
                    'key': obj['key'],
                    'algorithm': algorithm,
                    'status': 'claimed',
                    'claimed_at': current_time,
                    'ttl': ttl_timestamp,
                    'request_id': request_id,
                    'job_id': job_id
                }
                
                # Add optional fields
                if obj.get('version_id'):
                    item['version_id'] = obj['version_id']
                if obj.get('provided_md5') and algorithm == 'MD5':
                    item['provided_checksum'] = obj['provided_md5']
                if obj.get('provided_sha256') and algorithm == 'SHA256':
                    item['provided_checksum'] = obj['provided_sha256']
                
                batch.put_item(Item=item)
                
                # Log the association between DynamoDB record and batch job
                version_info = f" (version: {obj.get('version_id')})" if obj.get('version_id') else ""
                checksum_info = f" [expected: {item.get('provided_checksum', 'none')}]" if item.get('provided_checksum') else ""
                logger.info(f"[{request_id}] Created DynamoDB record: {object_key}{version_info} -> Job ID: {job_id}{checksum_info}")
    
    logger.info(f"[{request_id}] Created {len(objects) * 2} initial DynamoDB entries associated with batch jobs")


def create_batch_jobs(manifest_key: str, object_count: int) -> List[Dict[str, Any]]:
    """Create S3 batch operations jobs for checksum calculation using S3ComputeObjectChecksum
    
    This function creates S3 Batch Operations jobs that use the S3ComputeObjectChecksum operation
    to calculate checksums for objects. This is the dedicated operation for computing checksums
    without modifying the original objects. Uses ChecksumType='FULL_OBJECT' to compute checksums
    for the entire object content.
    
    Args:
        manifest_key: S3 key of the CSV manifest file containing object list
        object_count: Number of objects to process (for logging)
        
    Returns:
        List of job dictionaries containing algorithm, job_id, and status
    """
    jobs = []
    
    for algorithm in ['SHA256', 'MD5']:
        try:
            response = get_s3control_client().create_job(
                AccountId=get_account_id(),
                ConfirmationRequired=False,
                Operation={
                    'S3ComputeObjectChecksum': {
                        'ChecksumAlgorithm': algorithm,
                        'ChecksumType': 'FULL_OBJECT'
                    }
                },
                Manifest={
                    'Spec': {
                        'Format': 'S3BatchOperations_CSV_20180820',
                        'Fields': ['Bucket', 'Key']
                    },
                    'Location': {
                        'ObjectArn': f'arn:aws:s3:::{get_manifest_bucket()}/{manifest_key}',
                        'ETag': get_s3_client().head_object(Bucket=get_manifest_bucket(), Key=manifest_key)['ETag'].strip('"'),
                    }
                },
                Priority=10,
                RoleArn=get_batch_role_arn(),
                Report={
                    'Bucket': f'arn:aws:s3:::{get_manifest_bucket()}',
                    'Format': 'Report_CSV_20180820',
                    'Enabled': True,
                    'Prefix': f'batch-jobs/reports/{algorithm.lower()}/',
                    'ReportScope': 'AllTasks',
                    'ExpectedBucketOwner': get_account_id()
                },
                Description=f'{algorithm} checksum computation job - {datetime.now(timezone.utc).isoformat()}'
            )
            
            job_id = response['JobId']
            jobs.append({
                'algorithm': algorithm,
                'job_id': job_id,
                'status': 'created'
            })
            
            logger.info(f"Created {algorithm} batch job: {job_id}")
            
        except Exception as e:
            logger.error(f"Failed to create {algorithm} batch job: {str(e)}")
            jobs.append({
                'algorithm': algorithm,
                'error': str(e),
                'status': 'failed'
            })
    
    return jobs
