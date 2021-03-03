##Github CI Workflows

This document has a short outline of how tests are run in CI, how to add new tests and where these are configured.

### Adding a new test

Unit tests are run by the unit test runner, one per platform, currently percona56, mysql57, mysql80, mariadb101, mariadb102, mariadb103.
The workflow first installs the required database server before calling `make unit_test`.

To add a new end-to-end (e2e) test (also called _cluster end to end_ tests):
* Add a new object to test/config.json
* If you are creating a new test _shard_:
  * update `clusterList` in `ci_workflow_gen.go`
  * `make generate_ci_workflows`
* If you are adding a new database platform, update the `templates\unit_test.tpl` to add 
  the platform specific packages and update `unitTestDatabases`


### Vitess test runner
The `.github/workflows` directory contains one yaml file per workflow. e2e tests are run using the `test.go` script 
in the repository root.

This script invokes the vitess e2e test framework using a json configuration file `test/config.json` which has one object per test. 
Each test is of the form:

```javascript
"vtgate": {
			"File": "unused.go",
			"Args": ["vitess.io/vitess/go/test/endtoend/vtgate"],
			"Command": [],
			"Manual": false,
			"Shard": 17,
			"RetryMax": 0,
			"Tags": []
		},
```
The important parameters here are Args which define the arguments to `go test` and the Shard which says 
which Test VM should run this test. All tests which have a common Shard value are run in the same test vm.

### Known Issue

* Each VM does not seem to be able to create a lot of vttablets. For this reason we have had to split a few VReplication 
e2e tests across Shards. We need to identify and if possible fix this limitation so that we can reduce the number of test Shards
  