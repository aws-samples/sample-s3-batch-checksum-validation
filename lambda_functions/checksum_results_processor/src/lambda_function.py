import json
import boto3
from botocore.exceptions import ClientError
import csv
import io
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from urllib.parse import unquote_plus

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients lazily
_s3_client = None
_dynamodb = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
    return _s3_client

def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb')
    return _dynamodb

# Environment variable getters
def get_checksum_table_name():
    return os.environ['CHECKSUM_TABLE_NAME']


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler for processing S3 batch checksum results"""
    try:
        logger.info(f"Processing S3 event: {json.dumps(event)}")
        
        results = []
        for record in event.get('Records', []):
            if record.get('eventSource') == 'aws:s3':
                result = process_s3_event(record)
                results.append(result)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed batch reports',
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def process_s3_event(record: Dict[str, Any]) -> Dict[str, Any]:
    """Process individual S3 event record"""
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    
    logger.info(f"Processing report: s3://{bucket}/{key}")
    logger.info(f"S3 event details - Bucket: {bucket}, Key: {key}")
    
    # Determine algorithm from key path
    algorithm = None
    if '/sha256/' in key.lower():
        algorithm = 'SHA256'
    elif '/md5/' in key.lower():
        algorithm = 'MD5'
    else:
        logger.warning(f"Cannot determine algorithm from key: {key}")
        return {'key': key, 'status': 'skipped', 'reason': 'unknown_algorithm'}
    
    # Download and process the report
    try:
        logger.info(f"Attempting to get object: Bucket={bucket}, Key={key}")
        response = get_s3_client().get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Parse CSV and extract checksums
        checksums = parse_batch_report_csv(content, algorithm)
        
        # Update DynamoDB records
        update_result = update_checksum_records(checksums, algorithm)
        
        return {
            'key': key,
            'algorithm': algorithm,
            'status': 'processed',
            'total_records': len(checksums),
            'updated_records': update_result['updated_count'],
            'request_ids': update_result['request_ids']
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error(f"S3 ClientError accessing s3://{bucket}/{key}: {error_code} - {str(e)}")
        if error_code == 'AccessDenied':
            logger.error(f"Access denied to s3://{bucket}/{key} - check IAM permissions")
        elif error_code == 'NoSuchBucket':
            logger.error(f"Bucket {bucket} does not exist")
        elif error_code == 'NoSuchKey':
            logger.error(f"Object {key} does not exist in bucket {bucket}")
        return {
            'key': key,
            'status': 'error',
            'error': f'S3 access error: {error_code}',
            'bucket': bucket
        }
    except Exception as e:
        logger.error(f"Error processing report {key}: {str(e)}")
        return {'key': key, 'status': 'error', 'error': str(e)}


def parse_batch_report_csv(content: str, algorithm: str) -> List[Dict[str, Any]]:
    """Parse S3 batch operations report CSV"""
    checksums = []
    reader = csv.reader(io.StringIO(content))
    
    for row in reader:
        if len(row) < 7:
            continue
            
        bucket = row[0].strip()
        key = row[1].strip()
        version_id = row[2].strip() if row[2] else None
        task_status = row[3].strip()
        result_code = row[4].strip()
        result_string = row[5].strip() if row[5] else None
        json_result = row[6].strip() if len(row) > 6 and row[6] else None
        
        # URL decode key if needed
        try:
            key = unquote_plus(key)
        except Exception:
            pass
        
        checksum_info = {
            'bucket': bucket,
            'key': key,
            'version_id': version_id,
            'task_status': task_status,
            'result_code': result_code,
            'result_string': result_string,
            'algorithm': algorithm,
            'processed_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Extract checksum from JSON result
        if task_status.lower() == 'succeeded' and json_result:
            try:
                result_data = json.loads(json_result)
                if result_data.get('checksumAlgorithm') == algorithm:
                    checksum_hex = result_data.get('checksum_hex')
                    checksum_info['checksum'] = checksum_hex if checksum_hex else None
                    checksum_info['checksum_base64'] = result_data.get('checksum_base64')
                    checksum_info['etag'] = result_data.get('ETag')
                else:
                    checksum_info['checksum'] = None
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON result for {bucket}/{key}: {str(e)}")
                checksum_info['checksum'] = None
                checksum_info['error'] = f"JSON parse error: {str(e)}"
        else:
            checksum_info['checksum'] = None
            if task_status.lower() != 'succeeded':
                # Try to extract error from JSON result
                if json_result:
                    try:
                        result_data = json.loads(json_result)
                        checksum_info['error'] = result_data.get('error', f"Task failed: {task_status}")
                    except json.JSONDecodeError:
                        checksum_info['error'] = result_string or f"Task failed: {task_status}"
                else:
                    checksum_info['error'] = result_string or f"Task failed: {task_status}"
        
        checksums.append(checksum_info)
    
    logger.info(f"Parsed {len(checksums)} records from batch report")
    return checksums


def update_checksum_records(checksums: List[Dict[str, Any]], algorithm: str) -> Dict[str, Any]:
    """Update existing DynamoDB records with checksum results and tag S3 objects"""
    table = get_dynamodb().Table(get_checksum_table_name())
    updated_count = 0
    request_ids = set()
    
    for checksum_info in checksums:
        bucket = checksum_info['bucket'].strip()
        key = checksum_info['key'].strip()
        
        # URL decode key if needed (for direct calls to this function)
        try:
            key = unquote_plus(key)
        except Exception:
            pass
        
        item_key = f"{bucket}#{key}#{algorithm}"
        
        try:
            # Check if record exists and get request_id
            existing_record = table.get_item(Key={'object_key': item_key})
            if 'Item' not in existing_record:
                logger.warning(f"Record not found for {item_key}")
                continue
            
            # Extract request_id from existing record
            record_request_id = existing_record['Item'].get('request_id')
            if record_request_id:
                request_ids.add(record_request_id)
            
            # Determine status
            task_status = checksum_info.get('task_status', '').lower()
            if task_status == 'succeeded' and checksum_info.get('checksum'):
                status = 'succeeded'
            elif task_status == 'failed':
                status = 'failed'
            else:
                status = 'processed'
            
            # Build update expression
            update_expression = "SET #status = :status, #processed_at = :processed_at"
            expression_attribute_names = {'#status': 'status', '#processed_at': 'processed_at'}
            expression_attribute_values = {
                ':status': status,
                ':processed_at': checksum_info['processed_at']
            }
            
            # Add optional fields
            if checksum_info.get('checksum'):
                update_expression += ", #checksum = :checksum"
                expression_attribute_names['#checksum'] = 'checksum'
                expression_attribute_values[':checksum'] = checksum_info['checksum']
            
            if checksum_info.get('task_status'):
                update_expression += ", #task_status = :task_status"
                expression_attribute_names['#task_status'] = 'task_status'
                expression_attribute_values[':task_status'] = checksum_info['task_status']
            
            if checksum_info.get('error'):
                update_expression += ", #error = :error"
                expression_attribute_names['#error'] = 'error'
                expression_attribute_values[':error'] = checksum_info['error']
            
            # Update record
            table.update_item(
                Key={'object_key': item_key},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            
            # Update DynamoDB record with checksum result
            # Note: S3 object tagging removed - results processor now focuses solely on data processing
            
            updated_count += 1
            logger.debug(f"Updated record for {item_key} with status: {status}")
            
        except Exception as e:
            logger.error(f"Error updating record {item_key}: {str(e)}")
            continue
    
    logger.info(f"Updated {updated_count} {algorithm} checksum records")
    return {
        'updated_count': updated_count,
        'request_ids': list(request_ids)
    }






