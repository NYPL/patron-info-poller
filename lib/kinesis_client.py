import boto3
import os
import time

from botocore.exceptions import ClientError
from helpers.log_helper import create_log


class KinesisClient:
    """Client for managing connections to and operations with AWS Kinesis."""

    def __init__(self):
        self.logger = create_log('kinesis_client')
        self.kinesis_client = boto3.client(
            'kinesis',
            region_name=os.environ['AWS_REGION'],
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
        )

    def send_records(self, encoded_records):
        """
        Sends list of Avro encoded patron records (represented as byte strings)
        to Kinesis stream.
        """
        batch_size = int(os.environ['KINESIS_BATCH_SIZE'])
        for i in range(0, len(encoded_records), batch_size):
            encoded_batch = encoded_records[i:i + batch_size]
            kinesis_records = [{'Data': record, 'PartitionKey':
                                str(int(time.time() * 1000000000))}
                               for record in encoded_batch]
            self._send_kinesis_format_records(kinesis_records, 1)

    def _send_kinesis_format_records(self, kinesis_records, call_count):
        """
        Sends list of Kinesis records to Kinesis stream. This method is
        recursively called when Kinesis fails to retrieve some of the records.
        """
        if call_count > 5:
            self.logger.error(
                'Failed to send records to Kinesis 5 times in a row')
            raise KinesisClientError(
                'Failed to send records to Kinesis 5 times in a row') from None

        try:
            self.logger.info(
                'Sending ({count}) records to {stream} Kinesis stream'.format(
                    count=len(kinesis_records),
                    stream=os.environ['KINESIS_STREAM_NAME']))
            response = self.kinesis_client.put_records(
                Records=kinesis_records,
                StreamName=os.environ['KINESIS_STREAM_NAME'])
            if response['FailedRecordCount'] > 0:
                self.logger.warning(
                    'Failed to send {} records to Kinesis'.format(
                        response['FailedRecordCount']))
                failed_records = []
                for i in range(len(response['Records'])):
                    if 'ErrorCode' in response['Records'][i]:
                        failed_records.append(kinesis_records[i])
                self._send_kinesis_format_records(failed_records, call_count+1)
        except ClientError as e:
            self.logger.error(
                'Error sending records to Kinesis: {}'.format(e))
            raise KinesisClientError(
                'Error sending records to Kinesis: {}'.format(e)) from None


class KinesisClientError(Exception):
    def __init__(self, message=None):
        self.message = message
