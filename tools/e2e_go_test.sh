#!/bin/bash
source build.env
echo "running tests for " "$@"
go test -v "$@" -alsologtostderr -count=1