import os


class TestHelpers:
    ENV_VARS = {
        'ENVIRONMENT': 'test_environment',
        'AWS_REGION': 'test_aws_region',
        'AWS_ACCESS_KEY_ID': 'test_aws_key_id',
        'AWS_SECRET_ACCESS_KEY': 'test_aws_secret_key',
        'SIERRA_DB_PORT': 'test_sierra_port',
        'SIERRA_DB_NAME': 'test_sierra_name',
        'SIERRA_DB_HOST': 'test_sierra_host',
        'SIERRA_DB_USER': 'test_sierra_user',
        'SIERRA_DB_PASSWORD': 'test_sierra_password',
        'REDSHIFT_CLUSTER': 'test_redshift_cluster',
        'REDSHIFT_DB_NAME': 'test_redshift_name',
        'REDSHIFT_DB_USER': 'test_redshift_user',
        'REDSHIFT_DB_PASSWORD': 'test_redshift_password',
        'GEOCODER_API_BASE_URL': 'https://test_geocoder_url',
        'GEOCODER_API_BENCHMARK': 'test_geocoder_benchmark',
        'GEOCODER_API_VINTAGE': 'test_geocoder_vintage',
        'PATRON_INFO_SCHEMA_URL': 'https://test_schema_url',
        'KINESIS_STREAM_NAME': 'test_kinesis_stream',
        'KINESIS_BATCH_SIZE': '2',
        'S3_BUCKET': 'test_s3_bucket',
        'S3_RESOURCE': 'test_s3_resource',
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
