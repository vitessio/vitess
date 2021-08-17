#!/bin/bash
source build.env
echo "running tests for " "$@"
go test -failfast -v "$@" -alsologtostderr -count=1