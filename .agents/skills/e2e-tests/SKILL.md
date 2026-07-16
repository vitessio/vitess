---
name: e2e-tests
description: >-
  Run and debug Vitess end-to-end tests. Use when working with tests under
  go/test/endtoend/, when asked to run e2e tests, debug e2e test failures, or
  investigate test flakiness.
---

# Vitess End-to-End Tests

Vitess end-to-end tests live under `go/test/endtoend/`. Docker must be running
before invoking them.

## Running tests

Run every end-to-end package (not recommended, will take a very long time and
is unlikely to succeed):

```bash
make e2e
```

Run one package:

```bash
make e2e PKG=./go/test/endtoend/vreplication
```

Run every package with the race detector:

```bash
make e2e_race
```

Run one package with the race detector:

```bash
make e2e_race PKG=./go/test/endtoend/vreplication
```

Run one test:

```bash
make e2e 'PKG=./go/test/endtoend/<package> -run ^TestName$$ -v'
```

Run one subtest:

```bash
make e2e 'PKG=./go/test/endtoend/<package> -run ^TestParent/SubTest$$ -v'
```

The doubled `$$` passes a literal `$` to the `go test` regular expression.

## Logs

Use `-v` to show test progress and `t.Log` output.

Every component's container logs stream to one file per component under the
test's artifact directory. Pass `-artifacts` to retain them:

```bash
mkdir -p /tmp/vitesst-artifacts
make e2e 'PKG=./go/test/endtoend/<package> -run ^TestName$$ -v -artifacts -outputdir /tmp/vitesst-artifacts'
```

When a test fails, inspect its verbose output and the files under the artifact
directory. During a hung test, use `docker ps -a` to identify containers that
did not become healthy.
