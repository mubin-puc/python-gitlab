import boto3
import os

s3 = boto3.client('s3')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')

reports = ['pytest_report_script1.html', 'pytest_report_script2.html']

for report in reports:
    if os.path.exists(report):
        try:
            s3.upload_file(report, s3_bucket_name, f'reports/{report}')
            print(f"Report {report} successfully uploaded to S3 bucket: {s3_bucket_name}")
        except Exception as e:
            print(f"Failed to upload {report} to S3: {e}")
    else:
        print(f"Report {report} not found.")