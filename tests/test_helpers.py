import os


class TestHelpers:
    ENV_VARS = {
        'ENVIRONMENT': 'test_environment',
        'AWS_REGION': 'test_aws_region',
        'SIERRA_DB_PORT': 'test_sierra_port',
        'SIERRA_DB_NAME': 'test_sierra_name',
        'SIERRA_DB_HOST': 'test_sierra_host',
        'SIERRA_DB_USER': 'test_sierra_user',
        'SIERRA_DB_PASSWORD': 'test_sierra_password',
        'REDSHIFT_DB_NAME': 'test_redshift_name',
        'REDSHIFT_DB_HOST': 'test_redshift_host',
        'REDSHIFT_DB_USER': 'test_redshift_user',
        'REDSHIFT_DB_PASSWORD': 'test_redshift_password',
        'REDSHIFT_TABLE': 'test_redshift_table',
        'GEOCODER_API_BASE_URL': 'https://test_geocoder_url',
        'GEOCODER_API_BENCHMARK': 'test_geocoder_benchmark',
        'GEOCODER_API_VINTAGE': 'test_geocoder_vintage',
        'GEOCODER_API_KEY': 'test_geocoder_key',
        'PATRON_INFO_SCHEMA_URL': 'https://test_schema_url',
        'KINESIS_STREAM_ARN': 'test_kinesis_stream',
        'KINESIS_BATCH_SIZE': '2',
        'S3_BUCKET': 'test_s3_bucket',
        'S3_RESOURCE': 'test_s3_resource',
        'ACTIVE_PATRON_BATCH_SIZE': '4',
        'DELETED_PATRON_BATCH_SIZE': '3'
    }

    @classmethod
    def set_env_vars(cls):
        for key, value in cls.ENV_VARS.items():
            os.environ[key] = value

    @classmethod
    def clear_env_vars(cls):
        for key in cls.ENV_VARS.keys():
            if key in os.environ:
                del os.environ[key]
