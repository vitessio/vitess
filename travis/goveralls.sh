#!/bin/bash

# Run coverage and upload to coveralls.io.
# Requires the secret COVERALLS_TOKEN env variable to be set.

set -e

go list -f '{{if len .TestGoFiles}}go test $(VT_GO_PARALLEL) -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}{{end    }}' ./go/... | xargs -i sh -c {} | tee unit_test_goveralls.txt
gover ./go/
# -shallow ensures that goveralls does not return with a failure \
# if Coveralls returns a 500 http error or higher (e.g. when the site is in read-only mode). \
goveralls -shallow -coverprofile=gover.coverprofile -service=travis-ci
echo
echo "Top 10 of Go packages with worst coverage:"
sort -n -k 5 unit_test_goveralls.txt | head -n10
[ -f unit_test_goveralls.txt ] && rm unit_test_goveralls.txt
