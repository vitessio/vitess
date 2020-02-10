#!/bin/bash
echo "running tests for $@ "
go test -v $@ -count=1