# A small example that demonstrates issue with ghcr.io/goccy/bigquery-emulator when using Big Query Storage Write API.

Running test:
`./mvnw clean install`

Actual result:
`[ERROR]   BigQueryEmulatorWriteApplicationTests.saveTest » Unknown io.grpc.StatusRuntimeException: UNKNOWN: failed to find stream from projects/test-project/datasets/local-dataset/tables/local-table/_default` 