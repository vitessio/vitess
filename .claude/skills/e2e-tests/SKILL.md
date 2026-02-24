---
name: e2e-tests
description: Run and debug Vitess end-to-end tests. Use when working with tests under go/test/endtoend/, when asked to run e2e tests, debug e2e test failures, or investigate test flakiness.
---

# Vitess End-to-End Tests

## What end-to-end tests do

End-to-end tests spin up real Vitess binaries (vtgate, vttablet, vtctld, mysqlctl, etcd, vtorc) and real MySQL instances on the local machine. They exercise the full production stack: topology, replication, query routing, cluster operations.

All end-to-end tests live under `go/test/endtoend/`. The cluster harness lives in `go/test/endtoend/cluster/`.

## Building binaries

Tests invoke binaries from `$VTROOT/bin/`. If source code for the binaries has changed, rebuild before running tests. Rebuilding is not needed if only test code under `go/test/endtoend/` has changed, since test code is compiled by `go test` at run time.

```bash
source build.env
NOVTADMINBUILD=1 make build
```

Always rebuild after code changes so tests run against up-to-date binaries.

## Running tests

```bash
source build.env
go test -count=1 -timeout 10m -run ^TestName$ vitess.io/vitess/go/test/endtoend/<pkg>
```

Use `-timeout` generously. End-to-end tests start entire clusters (etcd, MySQL, vttablet, vtgate) and can take minutes. Default 10m is safe for most tests. `-count=1` disables caching, which is essential since these tests have side effects.

To run a specific subtest:

```bash
go test -count=1 -timeout 10m -run ^TestParent/SubTest$ vitess.io/vitess/go/test/endtoend/<pkg>
```

## VTDATAROOT

`VTDATAROOT` is where all runtime data lives during test execution: MySQL data directories, logs, socket files, backups, and tmp directories. `source build.env` sets it to `$VTROOT/vtdataroot` by default.

Each test cluster creates a subdirectory `vtroot_<port>/` under `VTDATAROOT` and sets `VTDATAROOT` to that subdirectory for the duration of the test.

### Layout

```
$VTDATAROOT/
  vtroot_<port>/              # per-test-cluster root
    vt_0000000100/            # per-tablet directory (tablet UID 100)
      mysql.sock              # MySQL socket
      mysql.pid
      ...
    tmp_<port>/               # log directory for this cluster
      vtgate-stderr.txt
      vttablet-stderr.txt
      mysqlctl-stderr.txt
      *.INFO, *.WARNING, *.ERROR   # glog files
    backups/                  # backup data
```

### Reading logs

When a test fails, read logs from the tmp directory inside the cluster's VTDATAROOT:

```
$VTDATAROOT/vtroot_<port>/tmp_<port>/
```

Look for `*-stderr.txt` files and glog files (`*.INFO`, `*.WARNING`, `*.ERROR`).

### Clear between runs

Stale data in `VTDATAROOT` causes test failures: port conflicts, leftover MySQL data, stale socket files. Clear it between runs:

```bash
rm -rf $VTDATAROOT/vtroot_*
```

Do this before re-running tests that failed, especially if the previous run did not tear down cleanly (crash, timeout, ctrl-c).

## Test structure

Each end-to-end test package starts a `cluster.LocalProcessCluster` (topo, vtctld, MySQL, vttablets, vtgate) before tests run, and tears it down after. Individual `Test*` functions run queries or operations against the live cluster.

## Debugging failures

1. Clear VTDATAROOT: `rm -rf $VTDATAROOT/vtroot_*`
2. Rebuild binaries: `make build`
3. Run the failing test with `-v`
4. On failure, read logs from `$VTDATAROOT/vtroot_*/tmp_*/`
5. Check `*-stderr.txt` files first for startup errors
6. Check glog `*.ERROR` and `*.WARNING` files for runtime errors
