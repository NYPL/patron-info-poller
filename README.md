# PatronInfoPoller

The PatronInfoPoller periodically checks for patron data from Sierra, sends it to a geocoding API, obfuscates it, and writes the results to PatronInfo Kinesis streams for ingest into the [BIC](https://github.com/NYPL/BIC).

## Running locally
* `cd` into this directory
* Add your AWS credentials (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) to the config file for the environment you want to run
  * Alternatively, you can manually export them (e.g. `export AWS_ACCESS_KEY_ID=<key>`)
* Run `ENVIRONMENT=<env> python3 main.py`
  * `<env>` should be the config filename without the `.yaml` suffix
  * `make run` will run the poller using the development environment
* Alternatively, to build and run a Docker container, run:
```
docker image build -t patron-info-poller:local .

docker container run -e ENVIRONMENT=<env> -e AWS_ACCESS_KEY_ID=<aws_key> -e AWS_SECRET_ACCESS_KEY=<aws_secret_key> patron-info-poller:local
```

## Environment variables

| Name        | Notes           |
| ------------- | ------------- |
| `AWS_REGION` | Always `us-east-1`. The AWS region used for the Redshift, S3, KMS, and Kinesis clients. |
| `SIERRA_DB_PORT` | Always `1032` |
| `SIERRA_DB_NAME` | Always `iii` |
| `SIERRA_DB_HOST` | Obfuscated Sierra host (either test, QA, or prod) |
| `SIERRA_DB_USER` | Obfuscated Sierra user. There is only one user, so this is always the same. |
| `SIERRA_DB_PASSWORD` | Obfuscated Sierra password for the user. There is only one user, so this is always the same. |
| `REDSHIFT_CLUSTER` | Always `nypl-dw-production` |
| `REDSHIFT_DB_NAME` | Which Redshift database to query (either `dev` or `production`) |
| `REDSHIFT_TABLE` | Which Redshift table to query |
| `REDSHIFT_DB_USER` | Obfuscated Redshift user |
| `BCRYPT_SALT` | Obfuscated bcrypt salt |
| `GEOCODER_API_BASE_URL` | Always `https://geocoding.geo.census.gov/geocoder/geographies/addressbatch`. API endpoint to which the poller sends batch geocoding requests. |
| `GEOCODER_API_BENCHMARK` | Always `Public_AR_Current`. Which dataset should be used to address match. `Public_AR_Current` automatically uses the most recent. |
| `GEOCODER_API_VINTAGE` | Always `Current_Current`. Which dataset should be used to geocode matched addresses. `Current_Current` automatically uses the most recent. |
| `PATRON_INFO_SCHEMA_URL` | Platform API endpoint from which to retrieve the PatronInfo Avro schema |
| `KINESIS_STREAM_NAME` | Name of the Kinesis stream to which the poller sends the encoded data (either PatronInfo-qa or PatronInfo-production) |
| `KINESIS_BATCH_SIZE` | How many records should be sent to Kinesis at once. Kinesis supports up to 500 records per batch. |
| `S3_BUCKET` | S3 bucket for the cache. This differs between QA and prod and should be empty when not using the cache locally. |
| `S3_RESOURCE` | Name of the resource for the S3 cache. This differs between QA and prod and should be empty when not using the cache locally. |
| `SIERRA_BATCH_SIZE` | How many patron ids should be queried from Sierra at once |
| `LOG_LEVEL` (optional) | What level of logs should be output. Set to `warning` by default. |
| `MAX_BATCHES` (optional) | The maximum number of times the poller should poll Sierra per session. If this is not set, the poller will continue querying until all new records in Sierra have been processed. |
| `IGNORE_CACHE` (optional) | Whether fetching and setting the state from S3 should not be done. If this is true, the `STARTING_CREATION_DT`, `STARTING_UPDATE_DT`, and `STARTING_DELETION_DATE` environment variables will be used for the initial state (or `2020-01-01 00:00:00-05` by default). |
| `BACKFILL` (optional) | Whether only the new patrons routine should be run. When `MAX_BATCHES` is not set and `STARTING_CREATION_DT` is, this is used to backfill data. |
| `STARTING_CREATION_DT` (optional) | If `IGNORE_CACHE` is true, the datetime to use in the `WHERE` clause of the newly created patrons Sierra query. If `IGNORE_CACHE` is false, this field is not read. |
| `STARTING_UPDATE_DT` (optional) | If `IGNORE_CACHE` is true, the datetime to use in the `WHERE` clause of the newly updated patrons Sierra query. If `IGNORE_CACHE` is false, this field is not read. |
| `STARTING_DELETION_DATE` (optional) | If `IGNORE_CACHE` is true, the datetime to use in the `WHERE` clause of the newly deleted patrons Sierra query. If `IGNORE_CACHE` is false, this field is not read. |