import boto3
import json
import os

from botocore.exceptions import ClientError
from helpers.log_helper import create_log
from io import BytesIO


class S3Client:
    """Client for managing connections to and operations with AWS S3."""

    def __init__(self):
        self.logger = create_log('s3_client')
        self.s3_client = boto3.client(
            's3',
            region_name=os.environ.get('AWS_REGION', 'us-east-1'))

    def fetch_state(self):
        self.logger.info('Fetching cached query parameters from S3')
        try:
            output_stream = BytesIO()
            self.s3_client.download_fileobj(
                os.environ['S3_BUCKET'], os.environ['S3_RESOURCE'],
                output_stream)
            return json.loads(output_stream.getvalue())
        except ClientError as e:
            self.logger.error(
                'Error retrieving file {file} from S3 bucket {bucket}: {error}'
                .format(file=os.environ['S3_RESOURCE'],
                        bucket=os.environ['S3_BUCKET'], error=e))
            raise S3ClientError(
                'Error retrieving file {file} from S3 bucket {bucket}: {error}'
                .format(file=os.environ['S3_RESOURCE'],
                        bucket=os.environ['S3_BUCKET'], error=e)) from None

    def set_state(self, state):
        self.logger.info(
            'Setting cached query parameters to {} in S3'.format(state))
        try:
            input_stream = BytesIO(json.dumps(state).encode())
            self.s3_client.upload_fileobj(
                input_stream, os.environ['S3_BUCKET'],
                os.environ['S3_RESOURCE'])
        except ClientError as e:
            self.logger.error(
                'Error uploading file {file} to S3 bucket {bucket}: {error}'
                .format(file=os.environ['S3_RESOURCE'],
                        bucket=os.environ['S3_BUCKET'], error=e))
            raise S3ClientError(
                'Error uploading file {file} to S3 bucket {bucket}: {error}'
                .format(file=os.environ['S3_RESOURCE'],
                        bucket=os.environ['S3_BUCKET'], error=e)) from None


class S3ClientError(Exception):
    def __init__(self, message=None):
        self.message = message
