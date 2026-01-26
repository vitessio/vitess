## Overview

Vitess is a cloud native, horizontally scalable database platform built around MySQL. It provides sharding, orchestration, and operational tooling for running large MySQL fleets.

## Project Structure

This tree highlights the main repo areas. Most core services and shared libraries live under `go/`, while deployment examples, docs, and tooling live at the top level.

```
.
├── changelog/      Release notes
├── config/         Config defaults and templates
├── doc/            Documentation
├── docker/         Docker images and scripts
├── examples/       Example deployments
├── go/
│   ├── cmd/        Main binaries and CLI entry points
│   │   ├── mysqlctl/       MySQL control CLI
│   │   ├── mysqlctld/      MySQL daemon wrapper
│   │   ├── vtadmin/        VTAdmin API server
│   │   ├── vtbackup/       Backup and restore CLI
│   │   ├── vtctld/         Cluster management daemon
│   │   ├── vtctldclient/   CLI for vtctld APIs
│   │   ├── vtgate/         Query routing gateway
│   │   ├── vtorc/          Automated recovery service
│   │   └── vttablet/       Tablet server
│   ├── vt/         Core Vitess services like vtgate and vttablet
│   ├── mysql/      MySQL protocol and server code
│   ├── sqltypes/   SQL type system
│   ├── test/       Go test helpers and fixtures
│   ├── tools/      Code generation and tooling
│   └── ...         Shared Go libraries
├── java/           Java client and JDBC driver
├── misc/           Scripts and git hooks
├── proto/          Protobuf definitions
├── support/        Support assets and demo apps
├── test/           Test configs and CI helpers
├── tools/          Repository tooling
└── web/            VTAdmin web UI
```

## Development

### Environment setup

Install third-party dependencies and tools, then load environment variables that point to the repo and built binaries.

```console
make tools
source dev.env
```

### Build

Builds all Go binaries into `./bin` and also builds the VTAdmin web UI unless `NOVTADMINBUILD` is set. If you are not touching VTAdmin, set `NOVTADMINBUILD=1` to skip the web UI build.

```console
make build
```

### Unit tests

Runs Go unit tests across the repository. Narrow the package list for faster feedback when iterating. Use `-count=1` to avoid cached results.

```console
go test -count=1 ./go/...
```

### End-to-end tests

End-to-end tests bring up real Vitess components like etcd, mysqld, vttablets, and vtgates on the same machine. They can take long to finish and are occasionally flaky. Tests use `VTDATAROOT` which defaults to `./vtdataroot`. Make sure to clear that directory between runs to reduce the chance of flakiness and stale test state.

The tests use real binaries in `$VTROOT/bin`, built from source. Rebuild the binaries before each end-to-end run so they contain the most up-to-date code, then run the end-to-end suites under `go/test/endtoend`. If you are not touching VTAdmin, set `NOVTADMINBUILD=1` to skip the web UI build.  Run only one package at a time, and preferably run only one test at a time. Use `-count=1` to avoid cached results.

```console
rm -rf ./vtdataroot
NOVTADMINBUILD=1 make build
go test -count=1 ./go/.../endtoend/...
```

### Formatting and linting

Run golangci-lint for linting and formatting (`gofumpt` and `goimports`):

```console
golangci-lint run ./go/...
```

Or only for current staged and unstaged changes:

```console
golangci-lint run --new-from-rev=HEAD
```

### Protobufs

Regenerate Go, gRPC, and vtadmin-web types after editing files under `proto/`.

```console
make proto
```

## Patterns

Follow these conventions to keep changes consistent with the rest of the codebase.

- Write modern, idiomatic Go.
- Use `github.com/stretchr/testify` for tests.
- Prefer table driven tests.
- Document each test to explain what it is validating.
