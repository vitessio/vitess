# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build test clean unit_test unit_test_cover unit_test_race queryservice_test integration_test bson proto site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_vtgate_client_test

all: build test

# Set a custom value for -p, the number of packages to be built/tested in parallel.
# This is currently only used by our Travis CI test configuration.
# (Also keep in mind that this value is independent of GOMAXPROCS.)
ifdef VT_GO_PARALLEL
VT_GO_PARALLEL := "-p" $(VT_GO_PARALLEL)
endif
# Link against the MySQL library in $VT_MYSQL_ROOT if it's specified.
ifdef VT_MYSQL_ROOT
# Clutter the env var only if it's a non-standard path.
  ifneq ($(VT_MYSQL_ROOT),/usr)
    CGO_LDFLAGS += -L$(VT_MYSQL_ROOT)/lib
  endif
endif

build:
	godep go install $(VT_GO_PARALLEL) -ldflags "$(tools/build_version_flags.sh)" ./go/...

# Set VT_TEST_FLAGS to pass flags to python tests.
# For example, verbose output: export VT_TEST_FLAGS=-v
test: unit_test queryservice_test integration_test java_vtgate_client_test
site_test: unit_test site_integration_test

clean:
	go clean -i ./go/...
	rm -rf third_party/acolyte

# This will remove object files for all Go projects in the same GOPATH.
# This is necessary, for example, to make sure dependencies are rebuilt
# when switching between different versions of Go.
clean_pkg:
	rm -rf ../../../../pkg Godeps/_workspace/pkg

unit_test:
	godep go test $(VT_GO_PARALLEL) ./go/...

# Run the code coverage tools, compute aggregate.
# If you want to improve in a directory, run:
#   go test -coverprofile=coverage.out && go tool cover -html=coverage.out
unit_test_cover:
	godep go test $(VT_GO_PARALLEL) -cover ./go/... | misc/parse_cover.py

unit_test_race:
	godep go test $(VT_GO_PARALLEL) -race ./go/...

# Run coverage and upload to coveralls.io.
# Requires the secret COVERALLS_TOKEN env variable to be set.
unit_test_goveralls:
	go list -f '{{if len .TestGoFiles}}godep go test $(VT_GO_PARALLEL) -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}{{end}}' ./go/... | xargs -i sh -c {} | tee unit_test_goveralls.txt
	gover ./go/
	# -shallow ensures that goveralls does not return with a failure \
	# if Coveralls returns a 500 http error or higher (e.g. when the site is in read-only mode). \
	goveralls -shallow -coverprofile=gover.coverprofile -service=travis-ci
	echo
	echo "Top 10 of Go packages with worst coverage:"
	sort -n -k 5 unit_test_goveralls.txt | head -n10
	[ -f unit_test_goveralls.txt ] && rm unit_test_goveralls.txt

ENABLE_MEMCACHED := $(shell test -x /usr/bin/memcached && echo "-m")
queryservice_test_files = \
	"queryservice_test.py $(ENABLE_MEMCACHED) -e vtocc" \
	"queryservice_test.py $(ENABLE_MEMCACHED) -e vttablet"

queryservice_test:
	$(call run_integration_tests, $(queryservice_test_files))

# These tests should be run by users to check that Vitess works in their environment.
site_integration_test_files = \
	keyrange_test.py \
	keyspace_test.py \
	mysqlctl.py \
	secure.py \
	tabletmanager.py \
	update_stream.py \
	vtdb_test.py \
	vtgatev2_test.py

# These tests should be run by developers after making code changes.
# Integration tests are grouped into 3 suites.
# - small: under 30 secs
# - medium: 30 secs - 1 min
# - large: over 1 min
small_integration_test_files = \
	tablet_test.py \
	sql_builder_test.py \
	vertical_split.py \
	schema.py \
	keyspace_test.py \
	keyrange_test.py \
	mysqlctl.py \
	python_client_test.py \
	sharded.py \
	secure.py \
	binlog.py \
	backup.py \
	update_stream.py \
	custom_sharding.py \
	initial_sharding_bytes.py \
	initial_sharding.py

medium_integration_test_files = \
	tabletmanager.py \
	reparent.py \
	vtdb_test.py \
	client_test.py \
	vtgate_utils_test.py \
	rowcache_invalidator.py \
	worker.py \
	automation_horizontal_resharding.py

large_integration_test_files = \
	vtgatev2_test.py

# The following tests are considered too flaky to be included
# in the continous integration test suites
ci_skip_integration_test_files = \
	resharding_bytes.py \
	resharding.py

# Run the following tests after making worker changes.
worker_integration_test_files = \
	binlog.py \
	resharding.py \
	resharding_bytes.py \
	vertical_split.py \
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
		output=$$(time timeout 5m ./$$t $$VT_TEST_FLAGS 2>&1) ; \
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

java_vtgate_client_test:
	mvn -f java/pom.xml clean verify

v3_test:
	cd test && ./vtgatev3_test.py

bson:
	go generate ./go/...

# This rule rebuilds all the go files from the proto definitions for gRPC.
# 1. list all proto files.
# 2. remove 'proto/' prefix and '.proto' suffix.
# 3. (go) run protoc for each proto and put in go/vt/proto/${proto_file_name}/
# 4. (python) run protoc for each proto and put in py/vtproto/
proto:
	find proto -name '*.proto' -print | sed 's/^proto\///' | sed 's/\.proto//' | xargs -I{} $$VTROOT/dist/protobuf/bin/protoc -Iproto proto/{}.proto --go_out=plugins=grpc:go/vt/proto/{}
	find go/vt/proto -name "*.pb.go" | xargs sed --in-place -r -e 's,import ([a-z0-9_]+) ".",import \1 "github.com/youtube/vitess/go/vt/proto/\1",g'
	find proto -name '*.proto' -print | sed 's/^proto\///' | sed 's/\.proto//' | xargs -I{} $$VTROOT/dist/protobuf/bin/protoc -Iproto proto/{}.proto --python_out=py/vtproto --grpc_out=py/vtproto --plugin=protoc-gen-grpc=$$VTROOT/dist/grpc/bin/grpc_python_plugin

# This rule builds the bootstrap images for all flavors.
docker_bootstrap:
	docker/bootstrap/build.sh common
	docker/bootstrap/build.sh mariadb
	docker/bootstrap/build.sh mysql56

# This rule loads the working copy of the code into a bootstrap image,
# and then runs the tests inside Docker.
# Example: $ make docker_test flavor=mariadb
docker_test:
	go run test.go -flavor $(flavor)

docker_unit_test:
	go run test.go -flavor $(flavor) unit
