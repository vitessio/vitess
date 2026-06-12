#!/bin/bash
# Run go test with coverage for one or more Go package directories.
# Usage: run_codecov.sh [dir ...]
# Example: run_codecov.sh go/vt/topo go/vt/vtctl
#
# With no arguments, runs coverage on all packages under go/.

set -euo pipefail

DIRS=("${@:-go}")

COVERPKG=$(printf 'vitess.io/vitess/%s/...,' "${DIRS[@]}"); COVERPKG="${COVERPKG%,}"

go test -count=1 \
  -covermode=atomic \
  -coverpkg="$COVERPKG" \
  -coverprofile=coverage.out \
  $(printf './%s/... ' "${DIRS[@]}")
