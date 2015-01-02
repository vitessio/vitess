# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

.PHONY: all build test clean unit_test unit_test_cover unit_test_race queryservice_test integration_test bson site_test site_integration_test

all: build test

# Values to be burned into the binary at build-time.
LDFLAGS = "\
	-X github.com/youtube/vitess/go/vt/servenv.buildHost   '$$(hostname)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildUser   '$$(whoami)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildGitRev '$$(git rev-parse HEAD)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildTime   '$$(date)'\
"

build:
	go install -ldflags ${LDFLAGS} ./go/...

# Set VT_TEST_FLAGS to pass flags to python tests.
# For example, verbose output: export VT_TEST_FLAGS=-v
test: unit_test queryservice_test integration_test
site_test: unit_test site_integration_test

clean:
	go clean -i ./go/...
	rm -rf java/vtocc-client/target java/vtocc-jdbc-driver/target third_party/acolyte

unit_test:
	go test ./go/...

# Run the code coverage tools, compute aggregate.
# If you want to improve in a directory, run:
#   go test -coverprofile=coverage.out && go tool cover -html=coverage.out
unit_test_cover:
	go test -cover ./go/... | misc/parse_cover.py

unit_test_race:
	go test -race ./go/...

# Run coverage and upload to coveralls.io.
# Requires the secret COVERALLS_TOKEN env variable to be set.
unit_test_goveralls:
	go list -f '{{if len .TestGoFiles}}go test -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}{{end}}' ./go/... | xargs -i sh -c {}
	gover ./go/
	if ! [ -z "$$COVERALLS_TOKEN" ]; then \
		goveralls -coverprofile=gover.coverprofile -repotoken $$COVERALLS_TOKEN; \
	fi

queryservice_test:
	echo $$(date): Running test/queryservice_test.py...
	if [ -e "/usr/bin/memcached" ]; then \
		time test/queryservice_test.py -m -e vtocc $$VT_TEST_FLAGS || exit 1 ; \
		time test/queryservice_test.py -m -e vttablet $$VT_TEST_FLAGS || exit 1 ; \
	else \
		time test/queryservice_test.py -e vtocc $$VT_TEST_FLAGS || exit 1 ; \
		time test/queryservice_test.py -e vttablet $$VT_TEST_FLAGS || exit 1 ; \
	fi

# These tests should be run by users to check that Vitess works in their environment.
site_integration_test_files = \
	keyrange_test.py \
	keyspace_test.py \
	mysqlctl.py \
	secure.py \
	tabletmanager.py \
	update_stream.py \
	vtdb_test.py \
	vtgatev2_test.py \
	zkocc_test.py

# These tests should be run by developers after making code changes.
# Integration tests are grouped into 3 suites.
# - small: under 30 secs
# - medium: 30 secs - 1 min
# - large: over 1 min
small_integration_test_files = \
	initial_sharding.py \
	initial_sharding_bytes.py \
	vertical_split.py \
	vertical_split_vtgate.py \
	schema.py \
	keyspace_test.py \
	keyrange_test.py \
	mysqlctl.py \
	sharded.py \
	secure.py \
	binlog.py \
	clone.py

medium_integration_test_files = \
	tabletmanager.py \
	reparent.py \
	vtdb_test.py \
	rowcache_invalidator.py

large_integration_test_files = \
	vtgatev2_test.py \
	zkocc_test.py

# The following tests are considered too flaky to be included
# in the continous integration test suites
ci_skip_integration_test_files = \
	resharding_bytes.py \
	resharding.py \
	update_stream.py

.ONESHELL:
SHELL = /bin/bash

# function to execute a list of integration test files
# exits on first failure
define run_integration_tests
	cd test ; \
	for t in $1 ; do \
		echo $$(date): Running test/$$t... ; \
		output=$$(time ./$$t $$VT_TEST_FLAGS 2>&1) ; \
		if [[ $$? != 0 ]]; then \
			echo "$$output" >&2 ; \
			exit 1 ; \
		fi ; \
		echo ; \
	done
endef

small_integration_test:
	$(call run_integration_tests, $(small_integration_test_files))

medium_integration_test:
	$(call run_integration_tests, $(medium_integration_test_files))

large_integration_test:
	$(call run_integration_tests, $(large_integration_test_files))

ci_skip_integration_test:
	$(call run_integration_tests, $(ci_skip_integration_test_files))

integration_test: small_integration_test medium_integration_test large_integration_test ci_skip_integration_test

site_integration_test:
	$(call run_integration_tests, $(site_integration_test_files))

# this rule only works if bootstrap.sh was successfully ran in ./java
java_test:
	cd java && mvn verify

java_vtgate_client_test:
	mvn -f java/gorpc/pom.xml clean install -DskipTests
	mvn -f java/vtgate-client/pom.xml clean verify

v3_test:
	cd test && ./vtgatev3_test.py

bson:
	go generate ./go/...
