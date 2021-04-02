# singlestore-atomic

## V2
Simple test to process records concurrently with atomic "SELECT ... WHERE id = ? FOR UPDATE"

1. Install a local SingleStore DB with https://hub.docker.com/r/memsql/cluster-in-a-box
2. Add your DSN to the dsn variable
3. Launch the test with: go run main.go

## Errors

The test seems to lock too many rows, and the throughput is very bad



