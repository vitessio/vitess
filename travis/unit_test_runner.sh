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

blacklist="misc/flaky_unit_tests.txt"

# Go packages which have flaky tests.
# (Filters out comments and gets only the package names.)
flaky_packages=$(grep -oE '^[^#[:space:]]+' $blacklist | sort)

# All Go packages with test files.
packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}}{{end}}' ./go/... | sort)

all_except_flaky_tests=$(comm -23 <(echo "$packages_with_tests") <(echo "$flaky_packages"))
flaky_tests=$(comm -12 <(echo "$packages_with_tests") <(echo "$flaky_packages"))
nonexisting_flaky_tests=$(comm -13 <(echo "$packages_with_tests") <(echo "$flaky_packages"))

if [ -n "$nonexisting_flaky_tests" ]; then
  echo "ERROR: The following tests are listed in the blacklist file $blacklist but no longer in the source tree:"
  echo
  echo "$nonexisting_flaky_tests"
  exit 1
fi

# Run non-flaky tests.
echo "$all_except_flaky_tests" | xargs godep go test $VT_GO_PARALLEL
if [ $? -ne 0 ]; then
  echo "ERROR: Go unit tests failed. See above for errors."
  echo
  echo "This should NOT happen. Did you introduce a flaky unit test?"
  echo "If so, please enable retries for it in: $blacklist and re-run this test."
  exit 1
fi

# Run flaky tests sequentially. Retry when necessary.
for pkg in $flaky_tests; do
  max_attempts=3
  attempt=1
  # Set a timeout because some tests may deadlock when they flake.
  until godep go test -timeout 30s $VT_GO_PARALLEL $pkg; do
    echo "FAILED (try $attempt/$max_attempts) in $pkg (return code $?). See above for errors."
    if [ $((++attempt)) -gt $max_attempts ]; then
      echo "ERROR: Flaky Go unit tests in package $pkg failed too often (after $max_attempts retries). Please fix them."
      exit 1
    fi
  done
done
