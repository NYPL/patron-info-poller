## 2023-04-05 -- v1.0.4
- Use nypl-py-utils v1.0.1
- Ignore NULL dates when querying Sierra 

## 2023-04-03 -- v1.0.3
### Fixed
- Handle case when all addresses have been geocoded by the census geocoder
- Translate all addresses to ascii before sending to NYC geocoder to avoid
errors

## 2023-03-27 -- v1.0.0
### Added
- Added NYC-specific geocoder to try and geocode any address that weren't found
by the census geocoder
### Fixed
- Updated address_helper to use usaddress package to parse addresses rather
than hardcoded regular expressions

## 2023-03-23 -- v0.0.7
### Fixed
- Use nypl-py-utils v1.0.0
- Revert to using regular connections when querying Sierra and close each
connection after each query
- Update geocoder retry policy to try again with a smaller batch size

## 2023-02-14 -- v0.0.6
### Added
- Replaced generic classes/functions with nypl-py-utils
### Fixed
- Use connection pooling (implemented in nypl-py-utils) to prevent having to
open and close the Sierra connection each iteration

## 2023-02-07 -- v0.0.5
### Fixed
- Open new database connections each iteration to prevent
idle_in_transaction_session_timeout
- Add behavior for if Sierra returns no results

## 2023-01-19 -- v0.0.4
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