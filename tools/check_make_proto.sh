#!/bin/bash

source build.env

first_output=$(git status --porcelain)

make proto

second_output=$(git status --porcelain)

diff=$(diff <( echo "$first_output") <( echo "$second_output"))

if [[ "$diff" != "" ]]; then
  echo "ERROR: Regenerated proto files do not match the current version."
  echo -e "List of files containing differences:\n$diff"
  echo "=== DEBUG: ls bin/ ==="
  ls -la bin/ || true
  echo "=== DEBUG: which protoc-gen-go-grpc ==="
  which protoc-gen-go-grpc || true
  echo "=== DEBUG: bin/protoc-gen-go-grpc --version ==="
  bin/protoc-gen-go-grpc --version 2>&1 || echo "(failed)"
  echo "=== DEBUG: file bin/protoc-gen-go-grpc ==="
  file bin/protoc-gen-go-grpc 2>&1 || true
  echo "=== DEBUG: go version ==="
  go version
  echo "=== DEBUG: env vars ==="
  env | grep -E "^(GO|VTROOT|PATH)" | head -20
  echo "=== DEBUG: which go install of v1.2.0 ==="
  DBG_TMP=$(mktemp -d)
  GOBIN="$DBG_TMP" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0 2>&1 || true
  "$DBG_TMP"/protoc-gen-go-grpc --version 2>&1 || echo "(failed)"
  exit 1
fi
