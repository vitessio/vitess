#!/bin/bash
source build.env
echo "running tests for $@ "
go test -v $@ -count=1