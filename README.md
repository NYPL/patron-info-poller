# PatronInfoPoller

The PatronInfoPoller periodically checks for patron data from Sierra, sends it to a geocoding API, obfuscates it, and writes the results to a Kinesis stream for ingest into the [BIC](https://github.com/NYPL/BIC).

## Environment Variables

| Name        | Notes           |
| ------------- | ------------- |
| `AWS_REGION` | Always `us-east-1`. The AWS region used for the S3, KMS, and Kinesis clients. |
| `SIERRA_DB_PORT` | Always `1032` |
| `SIERRA_DB_NAME` | Always `iii` |
| `SIERRA_DB_HOST` | Encrypted Sierra host (either test, QA, or prod) |
| `SIERRA_DB_USER` | Encrypted Sierra user. There is only one user, so this is always the same. |
| `SIERRA_DB_PASSWORD` | Encrypted Sierra password for the user. There is only one user, so this is always the same. |
| `REDSHIFT_CLUSTER` | Always `nypl-dw-production` |
| `REDSHIFT_DB_NAME` | Which Redshift database to query (either `dev` or `production`) |
| `REDSHIFT_DB_USER` | Encrypted Redshift user. |
| `REDSHIFT_DB_PASSWORD` | Encrypted Redshift password for the user. |
| `GEOCODER_API_BASE_URL` | Always `https://geocoding.geo.census.gov/geocoder/geographies/addressbatch`. API endpoint to send geocoding requests to. |
| `GEOCODER_API_BENCHMARK` | Always `Public_AR_Current`. Which dataset should be used to address match. `Public_AR_Current` automatically uses the most recent. |
| `GEOCODER_API_VINTAGE` | Always `Current_Current`. Which dataset should be used to geocode matched addresses. `Current_Current` automatically uses the most recent. |
| `SIERRA_BATCH_SIZE` | How many patron ids should be queried from Sierra at once. |
| `LOG_LEVEL` (optional) | What level of logs should be output. Set to `warning` by default. |