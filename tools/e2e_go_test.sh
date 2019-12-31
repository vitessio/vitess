#!/bin/bash
source build.env
echo "running tests for $1"
go test -v $1 -count=1