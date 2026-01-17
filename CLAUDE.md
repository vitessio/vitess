# AGENTS.md

## Overview

Vitess is a cloud native, horizontally scalable database platform built around MySQL. It provides sharding, orchestration, and operational tooling for running large MySQL fleets. Core services like vtgate and vttablet handle query routing, tablet management, and topology coordination.

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

```sh
make tools
source dev.env
```

### Build

Builds all Go binaries into `./bin` and also builds the VTAdmin web UI unless `NOVTADMINBUILD` is set. Set `NOVTADMINBUILD=1` to skip the web UI build.

```sh
make build
```

### Unit tests

Runs Go unit tests across the repository. Narrow the package list for faster feedback when iterating.

```sh
go test ./go/...
```

### End to end tests

Build first so that binaries contain the most up to date code, then run the end-to-end suites under `go/test/endtoend`. These tests bring up real Vitess components like topology services, mysqld, vttablets, and vtgates on the same machine, so they can take longer to finish. Tests use `VTDATAROOT` which defaults to `./vtdataroot`. Run only one package at a time, and preferably run only one test at a time.

```sh
make build
go test ./go/.../endtoend/...
```

### Formatting and linting

Use go fmt for standard formatting:

```sh
go fmt ./go/...
```

Use goimports for import grouping with the Vitess local prefix:

```sh
goimports -local vitess.io/vitess -w path/to/file.go
```

Run golangci-lint for linting:

```sh
golangci-lint run ./go/...
```

### Protobufs

Regenerate Go, gRPC, and vtadmin-web types after editing files under `proto/`.

```sh
make proto
```

## Patterns

Follow these conventions to keep changes consistent with the rest of the codebase.

- Write modern, idiomatic Go.
- Use `github.com/stretchr/testify` for tests.
- Prefer table driven tests.
- Document each test to explain what it is validating.
