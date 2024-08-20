import boto3
from datetime import datetime
import os
import json
import requests

import settings
from conf.logger_config import log_info, log_error, log_success,log_msg


s3_client = boto3.client('s3')

def download_json_from_s3(s3_bucket_name, s3_prefix, input_folder_path):
    """
    Download Legacy_consolidated.json and Modernized_consolidated.json from S3 to local file system.
    :param s3_bucket_name: Name of s3 bucket
    :param s3_prefix:  s3 prefix folder name inside the bucket
    :param input_folder_path: Local path where the JSON file will be saved
    :return:
    """
    timestamps = {}
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix)
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json'):
                #extract timestamp from folder name
                timestamp_folder = key.split('/')[1]
                timestamp_str = timestamp_folder.split('_',1)[1]
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
                if timestamp not in timestamps:
                    timestamps[timestamp] = []
                timestamps[timestamp].append(key)

        # pick the latest timestamp from the list
        latest_timestamp = max(timestamps.keys())
        latest_json_keys = timestamps[latest_timestamp]
        for latest_json_key in latest_json_keys:
            local_file = os.path.join(input_folder_path,os.path.basename(latest_json_key))
            s3_client.download_file(s3_bucket_name, latest_json_key, local_file)
            log_success(f"Downloaded {latest_json_key.split('/')[2]} file to {local_file}", timestamp)

    except Exception as e:
        #print(f'Error downloading JSON file from S3: {e}')
        log_error(f"error downloading the file {e}", timestamp)

def upload_json_to_s3(s3_bucket_name, filename, timestamp,output_data):
    json_data = json.dumps(output_data)
    folder_name = f'8.000_{timestamp}'
    key = f'output_comparison/{folder_name}/{filename}'
    try:
        s3_client.put_object(Bucket=s3_bucket_name, Key=key, Body=json_data)
        log_success(f"Output {filename} is saved to AWS S3 successfully:  ", timestamp)
        log_msg("------------------------------------------------------")
    except Exception as e:
        log_error(f"Error uploading output file {filename} to AWS S3 {e}", timestamp)

def upload_comparison_output_file_to_s3(s3_bucket_name, filename, output_file,timestamp):
    folder_name = f'8.000_{timestamp}'
    key = f'output_comparison/{folder_name}/{filename}'
    try:
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        with open(output_file, 'rb') as file:
            s3_client.put_object(Bucket=s3_bucket_name, Key=key, Body=file, ContentType=content_type)
            log_success(f"Output Comparison file {filename} is uploaded to AWS S3 successfully:  ", timestamp)
            log_msg("==================================================================================")
    except Exception as e:
        log_error(f"Error uploading Output Comparison file {filename} to AWS S3 {e}", timestamp)


