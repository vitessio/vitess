# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build test clean unit_test unit_test_cover unit_test_race queryservice_test integration_test bson proto site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_vtgate_client_test

all: build test

# Values to be burned into the binary at build-time.
LDFLAGS = "\
	-X github.com/youtube/vitess/go/vt/servenv.buildHost   '$$(hostname)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildUser   '$$(whoami)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildGitRev '$$(git rev-parse HEAD)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildTime   '$$(LC_ALL=C date)'\
"

build:
	godep go install -ldflags ${LDFLAGS} ./go/...

# Set VT_TEST_FLAGS to pass flags to python tests.
# For example, verbose output: export VT_TEST_FLAGS=-v
test: unit_test queryservice_test integration_test java_vtgate_client_test
site_test: unit_test site_integration_test

clean:
	go clean -i ./go/...
	rm -rf java/vtocc-client/target java/vtocc-jdbc-driver/target third_party/acolyte

unit_test:
	godep go test ./go/...

# Run the code coverage tools, compute aggregate.
# If you want to improve in a directory, run:
#   go test -coverprofile=coverage.out && go tool cover -html=coverage.out
unit_test_cover:
	godep go test -cover ./go/... | misc/parse_cover.py

unit_test_race:
	godep go test -race ./go/...

# Run coverage and upload to coveralls.io.
# Requires the secret COVERALLS_TOKEN env variable to be set.
unit_test_goveralls:
	go list -f '{{if len .TestGoFiles}}godep go test -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}{{end}}' ./go/... | xargs -i sh -c {}
	gover ./go/
	# Travis doesn't set the token for forked pull requests, so skip
	# upload if COVERALLS_TOKEN is unset.
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
	vertical_split.py \
	vertical_split_vtgate.py \
	schema.py \
	keyspace_test.py \
	keyrange_test.py \
	mysqlctl.py \
	sharded.py \
	secure.py \
	binlog.py \
	clone.py \
	update_stream.py

medium_integration_test_files = \
	tabletmanager.py \
	reparent.py \
	vtdb_test.py \
	vtgate_utils_test.py \
	rowcache_invalidator.py \
	worker.py

large_integration_test_files = \
	vtgatev2_test.py \
	zkocc_test.py

# The following tests are considered too flaky to be included
# in the continous integration test suites
ci_skip_integration_test_files = \
	initial_sharding_bytes.py \
	initial_sharding.py \
	resharding_bytes.py \
	resharding.py

# Run the following tests after making worker changes
worker_integration_test_files = \
	binlog.py \
	resharding.py \
	resharding_bytes.py \
	vertical_split.py \
	vertical_split_vtgate.py \
	initial_sharding.py \
	initial_sharding_bytes.py \
	worker.py

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

worker_test:
	godep go test ./go/vt/worker/
	$(call run_integration_tests, $(worker_integration_test_files))

integration_test: small_integration_test medium_integration_test large_integration_test ci_skip_integration_test

site_integration_test:
	$(call run_integration_tests, $(site_integration_test_files))

# this rule only works if bootstrap.sh was successfully ran in ./java
java_test:
	cd java && mvn verify

java_vtgate_client_test:
	mvn -f java/vtgate-client/pom.xml clean verify

v3_test:
	cd test && ./vtgatev3_test.py

bson:
	go generate ./go/...

# This rule rebuilds all the go files from the proto definitions for gRPC
# FIXME(alainjobart) also add support for python gRPC stubs, right now
# it's only the proto files without gRPC
proto:
	cd go/vt/proto/queryservice && $$VTROOT/dist/protobuf/bin/protoc -I../../../../proto ../../../../proto/queryservice.proto --go_out=plugins=grpc:.
	cd go/vt/proto/vtctl && $$VTROOT/dist/protobuf/bin/protoc -I../../../../proto ../../../../proto/vtctl.proto --go_out=plugins=grpc:.
	cd go/vt/proto/tabletmanager && $$VTROOT/dist/protobuf/bin/protoc -I../../../../proto ../../../../proto/tabletmanager.proto --go_out=plugins=grpc:.
	cd go/vt/proto/automation && $$VTROOT/dist/protobuf/bin/protoc -I../../../../proto ../../../../proto/automation.proto --go_out=plugins=grpc:.
	find go/vt/proto -name "*.pb.go" | xargs sed --in-place -r -e 's,"([a-z0-9_]+).pb","github.com/youtube/vitess/go/vt/proto/\1",g'
	cd py/vtctl && $$VTROOT/dist/protobuf/bin/protoc -I../../proto ../../proto/vtctl.proto --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=$$VTROOT/dist/grpc/bin/grpc_python_plugin

# This rule builds a bootstrap image of the given flavor.
# Example: $ make docker_bootstrap flavor=mariadb
docker_bootstrap:
	docker/bootstrap/build.sh $(flavor)

# This rule loads the working copy of the code into a bootstrap image,
# and then runs the tests inside Docker.
# Example: $ make docker_test flavor=mariadb
docker_test:
	docker/test/run.sh $(flavor) 'make test'

docker_unit_test:
	docker/test/run.sh $(flavor) 'make unit_test'
