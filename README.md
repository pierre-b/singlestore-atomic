# singlestore-atomic

Simple test to process records concurrently with atomic "SELECT ... LIMIT 1 FOR UPDATE"

1. Install a local SingleStore DB with https://hub.docker.com/r/memsql/cluster-in-a-box
2. Add your DNS to the dsn variable
3. Launch the test with: go run main.go

## Errors

The test produces the following errors:

`scan id error: Error 1857: The connection cleanup process failed to clean up a connection. The open transaction has rolled back.`



