#!/bin/bash

# Run coverage and upload to coveralls.io.
# Requires the secret COVERALLS_TOKEN env variable to be set.

set -e

# Required for packages which link the MySQL C library.
export CGO_LDFLAGS="-L${VT_MYSQL_ROOT}/lib"

if [ -n "$VT_GO_PARALLEL" ]; then
  GO_PARALLEL="-p $VT_GO_PARALLEL"
fi

# Faster default coverage test: No instrumentation outside the package under test.
per_package_coverage='{{if len .TestGoFiles}}godep go test $GO_PARALLEL -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}{{end}}'
# For each Go package with test files, generate a line like this:
#   godep go test -coverpkg "$(deplist ... <pkg>)" -coverprofile=... <pkg>
# When run by the shell, $(deplist ... <pkg>) will generate the list of
# dependent packages (including <pkg>) and add them to the -coverpkg option.
# This way, these packages will get instrumented for coverage as well.
full_coverage='{{if len .TestGoFiles}}godep go test $GO_PARALLEL -coverpkg "$(deplist -oneline -include_input_pkg -p github.com/youtube/vitess -t {{.ImportPath}})" -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}{{end}}'

if [ "$1" == "--full" ]; then
  generate_shell_commands="$full_coverage"
else
  generate_shell_commands="$per_package_coverage"
fi

# Remove any output from a previous incomplete run.
[ -f unit_test_goveralls.txt ] && rm unit_test_goveralls.txt

go list -f "$generate_shell_commands" ./go/... | while read line; do
  # Allow tests to fail e.g. due to flakiness.
  # The impact on coverage due to failed tests should be minimal.
  set +e
  # Filter out go test complaints that test imports are not required to be
  # instrumented. That's wrong because it would result in less coverage.
  time sh -c "$line" 2>&1 | grep -v "warning: no packages being tested depend on " | tee --append unit_test_goveralls.txt
  set -e
done

echo
echo "Top 10 of Go packages with worst coverage:"
sort -n -k 5 unit_test_goveralls.txt | head -n10
echo
if [ "$1" == "--full" ]; then
  echo "NOTE: Coverage may be low because it is counted across *all* dependent packages."
else
  echo "NOTE: Coverage may be low because coverage is not counted across *all* dependent packages. Use --full to run such a coverage analysis."
fi
[ -f unit_test_goveralls.txt ] && rm unit_test_goveralls.txt

# Merge all .coverprofile files.
find -name .coverprofile | xargs gocovmerge > all.coverprofile
# -shallow ensures that goveralls does not return with a failure \
# if Coveralls returns a 500 http error or higher (e.g. when the site is in read-only mode). \
goveralls -shallow -coverprofile=all.coverprofile -service=travis-ci
