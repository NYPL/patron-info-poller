## 2022-01-19 -- v0.0.4
### Added
- Added Github actions workflows for running tests and deploying to QA and
production

## 2022-12-27 -- v0.0.3
### Added
- Added controller to allow for different types of pipeline runs
- Added helper to try and reformat addresses that could not be geocoded
- Implemented multi-threading for bcrypt obfuscation
### Fixed
- Updated Sierra queries and added processing to prevent duplicate rows caused
by joining the record_metadata and patron_record_address tables

## 2022-12-08 -- v0.0.2
### Added
- Initial commit of Avro encoder, S3 client, and Kinesis client
- Initial commit of main method
### Fixed
- Reformatted Sierra and Geocoder API clients to follow new linter guidelines

## 2022-11-29 -- v0.0.1
### Added
- Initial commit of Sierra and Geocoder API clients