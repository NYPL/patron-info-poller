import avro.schema
import base64
import json
import os
import requests

from avro.errors import AvroException
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from helpers.log_helper import create_log
from io import BytesIO
from requests.exceptions import JSONDecodeError, RequestException


class AvroEncoder:
    """
    Class for encoding records using the PatronInfo Avro schema obtained from
    the Platform API.
    """

    def __init__(self):
        self.logger = create_log('avro_encoder')
        self.schema = avro.schema.parse(self._get_json_schema())

    def encode_batch(self, patrons_df):
        """
        Encodes each row of a dataframe using the PatronInfo Avro schema. The
        dataframe should have the same columns and column names as the schema.

        Returns a list of byte strings where each string is an encoded record.
        """
        self.logger.info(
            'Encoding ({}) records using PatronInfo schema'.format(
                len(patrons_df)))
        encoded_records = []
        datum_writer = DatumWriter(self.schema)
        with BytesIO() as output_stream:
            encoder = BinaryEncoder(output_stream)
            decoded_records = json.loads(
                patrons_df.to_json(orient='records'))
            for patron_record in decoded_records:
                try:
                    datum_writer.write(patron_record, encoder)
                    encoded_records.append(output_stream.getvalue())
                    output_stream.seek(0)
                    output_stream.truncate(0)
                except AvroException as e:
                    self.logger.error(
                        'Failed to encode record with Avro schema: {}'.format(
                            e))
                    raise AvroEncoderError(
                        'Failed to encode record with Avro schema: {}'.format(
                            e)) from None
        return encoded_records

    def decode_record(self, record):
        """
        Decodes single record represented as a byte string. Currently only
        used for testing purposes.
        """
        datum_reader = DatumReader(self.schema)
        with BytesIO(record) as input_stream:
            decoder = BinaryDecoder(input_stream)
            return datum_reader.read(decoder)

    def _get_json_schema(self):
        url = os.environ['PATRON_INFO_SCHEMA_URL']
        self.logger.debug('Getting Avro schema from {}'.format(url))

        try:
            response = requests.get(url)
            response.raise_for_status()
        except RequestException as e:
            self.logger.error(
                'Failed to retrieve schema from {url}: {error}'.format(
                    url=url, error=e))
            raise AvroEncoderError(
                'Failed to retrieve schema from {url}: {error}'.format(
                    url=url, error=e)) from None

        try:
            json_response = response.json()
            return json_response['data']['schema']
        except (JSONDecodeError, KeyError) as e:
            self.logger.error(
                'Retrieved schema is malformed: {errorType} {errorMessage}'
                .format(errorType=type(e), errorMessage=e))
            raise AvroEncoderError(
                'Retrieved schema is malformed: {errorType} {errorMessage}'
                .format(errorType=type(e), errorMessage=e)) from None


class AvroEncoderError(Exception):
    def __init__(self, message=None):
        self.message = message
