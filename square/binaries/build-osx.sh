#!/bin/bash

set -ex

godir="/vt"
vtdir="/vt/src/vitess.io/vitess"

mkdir -p bin

find go/cmd -maxdepth 1 -mindepth 1 -type d > cmds.txt

while read p; do
  GOOS=darwin GOARCH=amd64 go build -o bin/$(basename ${p}) vitess.io/vitess/${p}
done <cmds.txt

tar czf vitess-osx-build.tar.gz bin web config