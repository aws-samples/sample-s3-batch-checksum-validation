import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client lazily
_s3_client = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
    return _s3_client


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for tagging S3 objects with verified checksums
    
    Expected event format:
    {
        "objects": [
            {
                "bucket": "source-bucket",
                "key": "file1.txt",
                "algorithm": "SHA256",
                "checksum": "abc123...",
                "version_id": "version123" (optional)
            }
        ]
    }
    """
    try:
        logger.info(f"Processing tagging request: {json.dumps(event)}")
        
        objects = event.get('objects', [])
        if not objects:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No objects provided for tagging'
                })
            }
        
        results = []
        successful_tags = 0
        failed_tags = 0
        
        for obj in objects:
            try:
                result = tag_s3_object_with_checksum(
                    bucket=obj['bucket'],
                    key=obj['key'],
                    algorithm=obj['algorithm'],
                    checksum=obj['checksum'],
                    version_id=obj.get('version_id')
                )
                results.append(result)
                if result['success']:
                    successful_tags += 1
                else:
                    failed_tags += 1
                    
            except Exception as e:
                logger.error(f"Error processing object {obj.get('bucket', 'unknown')}/{obj.get('key', 'unknown')}: {str(e)}")
                results.append({
                    'bucket': obj.get('bucket'),
                    'key': obj.get('key'),
                    'success': False,
                    'error': str(e)
                })
                failed_tags += 1
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Processed {len(objects)} objects',
                'successful_tags': successful_tags,
                'failed_tags': failed_tags,
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Internal error: {str(e)}'
            })
        }


def tag_s3_object_with_checksum(bucket: str, key: str, algorithm: str, checksum: str, version_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Tag S3 object with verified checksum
    
    Returns:
        Dict with success status and details
    """
    try:
        # Get existing tags
        get_tags_params = {'Bucket': bucket, 'Key': key}
        if version_id:
            get_tags_params['VersionId'] = version_id
        
        try:
            response = get_s3_client().get_object_tagging(**get_tags_params)
            existing_tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except get_s3_client().exceptions.NoSuchKey:
            logger.warning(f"Object not found for tagging: s3://{bucket}/{key}")
            return {
                'bucket': bucket,
                'key': key,
                'success': False,
                'error': 'Object not found'
            }
        except Exception as e:
            logger.warning(f"Could not get existing tags for s3://{bucket}/{key}: {str(e)}")
            existing_tags = {}
        
        # Add checksum tags
        tag_key = f"checksum-{algorithm.lower()}"
        existing_tags[tag_key] = checksum
        existing_tags[f"{tag_key}-verified"] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        # Convert back to tag set format
        tag_set = [{'Key': k, 'Value': v} for k, v in existing_tags.items()]
        
        # Apply tags
        put_tags_params = {
            'Bucket': bucket,
            'Key': key,
            'Tagging': {'TagSet': tag_set}
        }
        if version_id:
            put_tags_params['VersionId'] = version_id
        
        get_s3_client().put_object_tagging(**put_tags_params)
        
        version_info = f" (version: {version_id})" if version_id else ""
        logger.info(f"Tagged s3://{bucket}/{key}{version_info} with {algorithm} checksum: {checksum}")
        
        return {
            'bucket': bucket,
            'key': key,
            'algorithm': algorithm,
            'checksum': checksum,
            'version_id': version_id,
            'success': True,
            'tagged_at': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error tagging S3 object s3://{bucket}/{key}: {str(e)}")
        return {
            'bucket': bucket,
            'key': key,
            'success': False,
            'error': str(e)
        }
