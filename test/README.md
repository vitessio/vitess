## Github CI Workflows

This document has a short outline of how tests are run in CI, how to add new tests and where these are configured.

### Adding a new test

Unit tests are run by the unit test runner, one per platform, currently mysql57, mysql80, and mysql84.
The workflow first installs the required database server before calling `make unit_test`.

To add a new end-to-end (e2e) test (also called _cluster end to end_ tests):

* Add a new object to test/config.json with the appropriate `Shard` value
* Add any required dependencies to the test's `Needs` array in config.json
* The cluster_endtoend.yml workflow will automatically pick up the new shard

Available `Needs` values:
* `xtrabackup` - Install Percona Server and XtraBackup
* `minio` - Install Minio S3 server  
* `consul` - Run `make tools` for Consul/ZooKeeper
* `larger-runner` - Use 16-core runner
* `memory-check` - Verify 15GB+ RAM
* `limit-resources` - Apply MySQL resource limits
* `binlog-compression` - Enable binlog transaction compression

### Vitess test runner
The `.github/workflows` directory contains one yaml file per workflow. e2e tests are run using the `test.go` script 
in the repository root.

This script invokes the vitess e2e test framework using a json configuration file `test/config.json` which has one object per test. 
Each test is of the form:

```javascript
"vtgate": {
			"File": "unused.go",
			"Packages": ["vitess.io/vitess/go/test/endtoend/vtgate"],
			"Args": [],
			"Command": [],
			"Manual": false,
			"Shard": "vtgate_queries",
			"Needs": ["larger-runner"],
			"Tags": []
		},
```
The important parameters here are Packages which define the Go packages to test, Args for any `go test` flags, the Shard which says 
which group of tests to run together, and Needs which specifies CI dependencies.

### Known Issue

* Each VM does not seem to be able to create a lot of vttablets. For this reason we have had to split a few VReplication 
e2e tests across Shards. We need to identify and if possible fix this limitation so that we can reduce the number of test Shards
