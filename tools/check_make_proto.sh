#!/bin/bash

source build.env

first_output=$(git status --porcelain)

make proto

second_output=$(git status --porcelain)

diff=$(diff <( echo "$first_output") <( echo "$second_output"))

if [[ "$diff" != "" ]]; then
  echo "ERROR: Regenerated proto files do not match the current version."
  echo -e "List of files containing differences:\n$diff"
  echo "DEBUG: protoc-gen-go-grpc version info:"
  bin/protoc-gen-go-grpc --version 2>&1 || true
  go version
  echo "DEBUG: full content diff for each modified file:"
  git diff --no-color --text -- go/vt/proto/ | head -400
  exit 1
fi
