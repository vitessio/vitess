# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build test clean unit_test unit_test_cover unit_test_race integration_test proto site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_test php_test reshard_tests

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
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	go install $(VT_GO_PARALLEL) -ldflags "$(tools/build_version_flags.sh)" ./go/...

# To pass extra flags, run test.go manually.
# For example: go run test.go -docker=false -- --extra-flag
# For more info see: go run test.go -help
test:
	go run test.go -docker=false

site_test: unit_test site_integration_test

clean:
	go clean -i ./go/...
	rm -rf third_party/acolyte

# This will remove object files for all Go projects in the same GOPATH.
# This is necessary, for example, to make sure dependencies are rebuilt
# when switching between different versions of Go.
clean_pkg:
	rm -rf ../../../../pkg Godeps/_workspace/pkg

unit_test: build
	echo $$(date): Running unit tests
	go test $(VT_GO_PARALLEL) ./go/...

# Run the code coverage tools, compute aggregate.
# If you want to improve in a directory, run:
#   go test -coverprofile=coverage.out && go tool cover -html=coverage.out
unit_test_cover: build
	go test $(VT_GO_PARALLEL) -cover ./go/... | misc/parse_cover.py

unit_test_race: build
	tools/unit_test_race.sh

# Run coverage and upload to coveralls.io.
# Requires the secret COVERALLS_TOKEN env variable to be set.
unit_test_goveralls: build
	travis/goveralls.sh

.ONESHELL:
SHELL = /bin/bash

# Run the following tests after making worker changes.
worker_test:
	go test ./go/vt/worker/
	go run test.go -docker=false -tag=worker_test

site_integration_test:
	go run test.go -docker=false -tag=site_test

java_test:
	go install ./go/cmd/vtgateclienttest ./go/cmd/vtcombo
	mvn -f java/pom.xml clean verify

php_test:
	go install ./go/cmd/vtgateclienttest
	phpunit php/tests

# TODO(mberlin): Remove the manual copy once govendor supports a way to
# install vendor'd programs: https://github.com/kardianos/govendor/issues/117
install_protoc-gen-go:
	mkdir -p $${GOPATH}/src/github.com/golang/
	cp -au vendor/github.com/golang/protobuf $${GOPATH}/src/github.com/golang/
	go install github.com/golang/protobuf/protoc-gen-go

# This rule rebuilds all the go files from the proto definitions for gRPC.
# 1. list all proto files.
# 2. remove 'proto/' prefix and '.proto' suffix.
# 3. (go) run protoc for each proto and put in go/vt/proto/${proto_file_name}/
# 4. (python) run protoc for each proto and put in py/vtproto/
proto: install_protoc-gen-go
	find proto -maxdepth 1 -name '*.proto' -print | sed 's/^proto\///' | sed 's/\.proto//' | xargs -I{} $$VTROOT/dist/grpc/usr/local/bin/protoc -Iproto proto/{}.proto --go_out=plugins=grpc:go/vt/proto/{}
	find go/vt/proto -name "*.pb.go" | xargs sed --in-place -r -e 's,import ([a-z0-9_]+) ".",import \1 "github.com/youtube/vitess/go/vt/proto/\1",g'
	find proto -maxdepth 1 -name '*.proto' -print | sed 's/^proto\///' | sed 's/\.proto//' | xargs -I{} $$VTROOT/dist/grpc/usr/local/bin/protoc -Iproto proto/{}.proto --python_out=py/vtproto --grpc_out=py/vtproto --plugin=protoc-gen-grpc=$$VTROOT/dist/grpc/usr/local/bin/grpc_python_plugin

# Generate the PHP proto files in a Docker container, and copy them back.
php_proto:
	docker run -ti --name=vitess_php-proto -v $$PWD/proto:/in vitess/bootstrap:common bash -c 'cd $$VTTOP && mkdir -p proto && cp -R /in/* proto/ && tools/proto-gen-php.sh'
	docker cp vitess_php-proto:/vt/src/github.com/youtube/vitess/php/src/descriptor.php php/src/
	docker cp vitess_php-proto:/vt/src/github.com/youtube/vitess/php/src/php.php php/src/
	rm -r php/src/Vitess/Proto/*
	docker cp vitess_php-proto:/vt/src/github.com/youtube/vitess/php/src/Vitess/Proto/. php/src/Vitess/Proto/
	docker rm vitess_php-proto

# This rule builds the bootstrap images for all flavors.
DOCKER_IMAGES_FOR_TEST = mariadb mysql56 mysql57 percona
DOCKER_IMAGES = common $(DOCKER_IMAGES_FOR_TEST)
docker_bootstrap:
	for i in $(DOCKER_IMAGES); do echo "image: $$i"; docker/bootstrap/build.sh $$i || exit 1; done

docker_bootstrap_test:
	flavors='$(DOCKER_IMAGES_FOR_TEST)' && ./test.go -pull=false -parallel=4 -flavor=$${flavors// /,}

docker_bootstrap_push:
	for i in $(DOCKER_IMAGES); do echo "image: $$i"; docker push vitess/bootstrap:$$i || exit 1; done

docker_base:
	# Fix permissions before copying files, to avoid AUFS bug.
	chmod -R o=g *
	docker build -t vitess/base .

docker_base_mysql56:
	chmod -R o=g *
	docker build -f Dockerfile.percona -t vitess/base:mysql56 .

docker_base_percona:
	chmod -R o=g *
	docker build -f Dockerfile.percona -t vitess/base:percona .

docker_base_mariadb:
	chmod -R o=g *
	docker build -f Dockerfile.mariadb -t vitess/base:mariadb .

docker_lite: docker_base
	cd docker/lite && ./build.sh

docker_lite_mysql56: docker_base_mysql56
	cd docker/lite && ./build.sh mysql56

docker_lite_mariadb: docker_base_mariadb
	cd docker/lite && ./build.sh mariadb

docker_lite_percona: docker_base_percona
	cd docker/lite && ./build.sh percona

docker_guestbook:
	cd examples/kubernetes/guestbook && ./build.sh

docker_etcd:
	cd docker/etcd-lite && ./build.sh

docker_publish_site:
	docker build -f docker/publish-site/Dockerfile -t vitess/publish-site .

# This rule loads the working copy of the code into a bootstrap image,
# and then runs the tests inside Docker.
# Example: $ make docker_test flavor=mariadb
docker_test:
	go run test.go -flavor $(flavor)

docker_unit_test:
	go run test.go -flavor $(flavor) unit

# This can be used to rebalance the total average runtime of each group of
# tests in Travis. The results are saved in test/config.json, which you can
# then commit and push.
rebalance_tests:
	go run test.go -rebalance 5 -remote-stats http://enisoc.com:15123/travis/stats
