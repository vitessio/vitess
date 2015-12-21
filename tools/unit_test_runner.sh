#!/bin/bash

# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# Custom Go unit test runner which runs all unit tests in parallel except for
# known flaky unit tests.
# Flaky unit tests are run sequentially in the second phase and retried up to
# three times.

# Why are there flaky unit tests?
#
# Some of the Go unit tests are inherently flaky e.g. because they use the
# real timer implementation and might fail when things take longer as usual.
# In particular, this happens when the system is under load and threads do not
# get scheduled as fast as usual. Then, the expected timings do not match.

if [ -n "$VT_GO_PARALLEL" ]; then
  GO_PARALLEL="-p $VT_GO_PARALLEL"
fi

# All Go packages with test files.
# Output per line: <full Go package name> <all _test.go files in the package>*
packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/... | sort)

# Flaky tests have the suffix "_flaky_test.go".
all_except_flaky_tests=$(echo "$packages_with_tests" | grep -vE ".+ .+_flaky_test\.go" | cut -d" " -f1)
flaky_tests=$(echo "$packages_with_tests" | grep -E ".+ .+_flaky_test\.go" | cut -d" " -f1)

# Run non-flaky tests.
echo "$all_except_flaky_tests" | xargs godep go test $GO_PARALLEL
if [ $? -ne 0 ]; then
  echo "ERROR: Go unit tests failed. See above for errors."
  echo
  echo "This should NOT happen. Did you introduce a flaky unit test?"
  echo "If so, please rename it to the suffix _flaky_test.go."
  exit 1
fi

# Run flaky tests sequentially. Retry when necessary.
for pkg in $flaky_tests; do
  max_attempts=3
  attempt=1
  # Set a timeout because some tests may deadlock when they flake.
  until godep go test -timeout 30s $GO_PARALLEL $pkg; do
    echo "FAILED (try $attempt/$max_attempts) in $pkg (return code $?). See above for errors."
    if [ $((++attempt)) -gt $max_attempts ]; then
      echo "ERROR: Flaky Go unit tests in package $pkg failed too often (after $max_attempts retries). Please reduce the flakiness."
      exit 1
    fi
  done
done
