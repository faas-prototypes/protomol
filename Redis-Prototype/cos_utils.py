import logging
from pathlib import Path
import ibm_boto3
from ibm_botocore.client import Config
import protomol_template_service as template_service


logging.getLogger('ibm_boto3').setLevel(logging.CRITICAL)
logging.getLogger('ibm_botocore').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

logger = logging.getLogger('pywren-protomol')


def get_ibm_cos_client(config):
    return ibm_boto3.client(service_name='s3',
                            ibm_api_key_id=config['ibm_cos']['api_key'],
                            config=Config(signature_version='oauth'),
                            endpoint_url=config['ibm_cos']['endpoint'])


def upload_to_cos(src, target_key):
    with open(src, 'rb') as f:
        template_service.save_file(target_key,f.read())

def upload_to_cos_2(src, target_key):
   template_service.save_file(target_key,src)


def upload_bytes_to_cos(cos_client, bytes_data, target_bucket, target_key):
    cos_client.put_object(Bucket=target_bucket, Key=target_key, Body=bytes_data)
    logger.info('Copy completed for {}/{}'.format(target_bucket, target_key))


def clean_from_cos(config, bucket, prefix):
    print("clean from cos for {} {}".format(bucket, prefix))
    cos_client = get_ibm_cos_client(config)
    objs = cos_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while 'Contents' in objs:
        keys = [obj['Key'] for obj in objs['Contents']]
        formatted_keys = {'Objects': [{'Key': key} for key in keys]}
        cos_client.delete_objects(Bucket=bucket, Delete=formatted_keys)
        logger.info(f'Removed {objs["KeyCount"]} objects from {prefix}')
        objs = cos_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
