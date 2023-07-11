# PatronInfoPoller

The PatronInfoPoller periodically checks for patron data from Sierra, sends it to two separate geocoders, obfuscates it, and writes the results to PatronInfo Kinesis streams for ingest into the [BIC](https://github.com/NYPL/BIC).

## Running locally
* Because the NYC geocoder requires the dockerized version of the Geosupport application, the poller must be run via Docker
* To build and run the poller, run the following, where `<env>` is the config filename without the `.yaml` suffix:
```
docker image build -t patron-info-poller:local .

docker container run -e ENVIRONMENT=<env> -e AWS_ACCESS_KEY_ID=<aws_key> -e AWS_SECRET_ACCESS_KEY=<aws_secret_key> patron-info-poller:local
```
* If you add your AWS credentials directly to the `devel.yaml` config file, you can also use `make run` to build and run the poller in the development environment
* Note that running the poller with `production.yaml` will actually send records to the production Kinesis stream -- it is not meant to be used for development purposes 

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

[`main`](https://github.com/NYPL/patron-info-poller/tree/main) has the latest and greatest commits, [`qa`](https://github.com/NYPL/patron-info-poller/tree/qa) has what's in our QA environment, and [`production`](https://github.com/NYPL/patron-info-poller/tree/production) has what's in our production environment.

### Ideal Workflow
- Cut a feature branch off of `main`
- Commit changes to your feature branch
- File a pull request against `main` and assign a reviewer
  - In order for the PR to be accepted, it must pass all unit tests, have no lint issues, and update the CHANGELOG (or contain the Skip-Changelog label in GitHub)
- After the PR is accepted, merge into `main`
- Merge `main` > `qa`
- Deploy app to QA and confirm it works
- Merge `qa` > `production`
- Deploy app to production and confirm it works

## Deployment
The poller is deployed as an AWS ECS service to [qa](https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters/patron-info-poller-qa/services) and [prod](https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters/patron-info-poller-production/services) environments. To upload a new QA version of this service, create a new release in GitHub off of the `qa` branch and tag it `qa-vX.X.X`. The GitHub Actions deploy-qa workflow will then deploy the code to ECR and update the ECS service appropriately. To deploy to production, create the release from the `production` branch and tag it `production-vX.X.X`. To trigger the app to run immediately (rather than waiting for the next scheduled event), run:
```bash
# In production, use patron-info-poller-production:5 for the task definition
aws ecs run-task --cluster patron-info-poller-qa --task-definition  patron-info-poller-qa:4 --count 1 --region us-east-1 --profile nypl-digital-dev
```

## Environment variables
The first 14 unencrypted variables (every variable through `DELETED_PATRON_BATCH_SIZE`) plus all of the encrypted variables in each environment file are required by the poller to run. There are then seven additional optional variables that can be used for development purposes -- `devel.yaml` sets each of these. Note that the `qa_env` and `production_env` files are actually read by the deployed service, so do not change these files unless you want to change how the service will behave in the wild -- these are not meant for local testing.

| Name        | Notes           |
| ------------- | ------------- |
| `AWS_REGION` | Always `us-east-1`. The AWS region used for the Redshift, S3, KMS, and Kinesis clients. |
| `SIERRA_DB_PORT` | Always `1032` |
| `SIERRA_DB_NAME` | Always `iii` |
| `SIERRA_DB_HOST` | Encrypted Sierra host (either test, QA, or prod) |
| `SIERRA_DB_USER` | Encrypted Sierra user. There is only one user, so this is always the same. |
| `SIERRA_DB_PASSWORD` | Encrypted Sierra password for the user. There is only one user, so this is always the same. |
| `REDSHIFT_DB_NAME` | Which Redshift database to query (either `dev`, `qa`, or `production`) |
| `REDSHIFT_TABLE` | Which Redshift table to query |
| `REDSHIFT_DB_HOST` | Encrypted Redshift cluster endpoint |
| `REDSHIFT_DB_USER` | Encrypted Redshift user |
| `REDSHIFT_DB_PASSWORD` | Encrypted Redshift password for the user |
| `GEOCODER_API_KEY` | Encrypted key for using the Geocoder API -- at the moment a key is best practice but is not strictly necessary to use the API |
| `KINESIS_STREAM_ARN` | Encrypted ARN for the Kinesis stream the poller sends the encoded data to |
| `BCRYPT_SALT` | Encrypted bcrypt salt |
| `GEOCODER_API_BASE_URL` | Always `https://geocoding.geo.census.gov/geocoder/geographies/addressbatch`. API endpoint to which the poller sends batch geocoding requests. |
| `GEOCODER_API_BENCHMARK` | Always `Public_AR_Current`. Which dataset should be used to address match. `Public_AR_Current` automatically uses the most recent. |
| `GEOCODER_API_VINTAGE` | Always `Current_Current`. Which dataset should be used to geocode matched addresses. `Current_Current` automatically uses the most recent. |
| `PATRON_INFO_SCHEMA_URL` | Platform API endpoint from which to retrieve the PatronInfo Avro schema |
| `KINESIS_BATCH_SIZE` | How many records should be sent to Kinesis at once. Kinesis supports up to 500 records per batch. |
| `S3_BUCKET` | S3 bucket for the cache. This differs between QA and prod and should be empty when not using the cache locally. |
| `S3_RESOURCE` | Name of the resource for the S3 cache. This differs between QA and prod and should be empty when not using the cache locally. |
| `ACTIVE_PATRON_BATCH_SIZE` | How many newly created or updated patron ids should be queried from Sierra at once |
| `DELETED_PATRON_BATCH_SIZE` | How many newly deleted patron ids should be queried from Sierra at once |
| `LOG_LEVEL` (optional) | What level of logs should be output. Set to `info` by default. |
| `MAX_BATCHES` (optional) | The maximum number of times the poller should poll Sierra per session. If this is not set, the poller will continue querying until all new records in Sierra have been processed. |
| `IGNORE_CACHE` (optional) | Whether fetching and setting the state from S3 should not be done. If this is true, the `STARTING_CREATION_DT`, `STARTING_UPDATE_DT`, and `STARTING_DELETION_DATE` environment variables will be used for the initial state (or `2020-01-01 00:00:00-05` by default). |
| `IGNORE_KINESIS` (optional) | Whether sending records to Kinesis should not be done |
| `STARTING_CREATION_DT` (optional) | If `IGNORE_CACHE` is true, the datetime to use in the `WHERE` clause of the newly created patrons Sierra query. If `IGNORE_CACHE` is false, this field is not read. |
| `STARTING_UPDATE_DT` (optional) | If `IGNORE_CACHE` is true, the datetime to use in the `WHERE` clause of the newly updated patrons Sierra query. If `IGNORE_CACHE` is false, this field is not read. |
| `STARTING_DELETION_DATE` (optional) | If `IGNORE_CACHE` is true, the datetime to use in the `WHERE` clause of the newly deleted patrons Sierra query. If `IGNORE_CACHE` is false, this field is not read. |