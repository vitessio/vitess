# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build build_web test clean unit_test unit_test_cover unit_test_race integration_test proto proto_banner site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_test php_test reshard_tests

all: build test

# Set a custom value for -p, the number of packages to be built/tested in parallel.
# This is currently only used by our Travis CI test configuration.
# (Also keep in mind that this value is independent of GOMAXPROCS.)
ifdef VT_GO_PARALLEL_VALUE
export VT_GO_PARALLEL := -p $(VT_GO_PARALLEL_VALUE)
endif

# Link against the MySQL library in $VT_MYSQL_ROOT if it's specified.
ifdef VT_MYSQL_ROOT
# Clutter the env var only if it's a non-standard path.
  ifneq ($(VT_MYSQL_ROOT),/usr)
    CGO_LDFLAGS += -L$(VT_MYSQL_ROOT)/lib
  endif
endif

build_web:
	echo $$(date): Building web artifacts
	cd web/vtctld2 && ng build -prod
	cp -f web/vtctld2/src/{favicon.ico,plotly-latest.min.js,primeui-ng-all.min.css} web/vtctld2/dist/

build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	go install $(VT_GO_PARALLEL) -ldflags "$(tools/build_version_flags.sh)" ./go/...

parser:
	make -C go/vt/sqlparser

# To pass extra flags, run test.go manually.
# For example: go run test.go -docker=false -- --extra-flag
# For more info see: go run test.go -help
test:
	go run test.go -docker=false

site_test: unit_test site_integration_test

clean:
	go clean -i ./go/...
	rm -rf third_party/acolyte
	rm -rf go/vt/.proto.tmp

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
	cp -a vendor/github.com/golang/protobuf $${GOPATH}/src/github.com/golang/
	go install github.com/golang/protobuf/protoc-gen-go

PROTOC_DIR := $(VTROOT)/dist/grpc/usr/local/bin
PROTOC_EXISTS := $(shell type -p $(PROTOC_DIR)/protoc)
ifeq (,$(PROTOC_EXISTS))
  PROTOC_BINARY := $(shell which protoc)
  ifeq (,$(PROTOC_BINARY))
    $(error "Cannot find protoc binary. Did bootstrap.sh succeed, and did you execute 'source dev.env'?")
  endif
  PROTOC_DIR := $(dir $(PROTOC_BINARY))
endif

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_PY_OUTS = $(foreach name, $(PROTO_SRC_NAMES), py/vtproto/$(name)_pb2.py)
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)
PROTO_GO_TEMPS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/.proto.tmp/$(name).pb.go)

# This rule rebuilds all the go and python files from the proto definitions for gRPC.
proto: proto_banner $(PROTO_GO_OUTS) $(PROTO_PY_OUTS)

proto_banner:
ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

$(PROTO_PY_OUTS): py/vtproto/%_pb2.py: proto/%.proto
	$(PROTOC_DIR)/protoc -Iproto $< --python_out=py/vtproto --grpc_out=py/vtproto --plugin=protoc-gen-grpc=$(PROTOC_DIR)/grpc_python_plugin

$(PROTO_GO_OUTS): $(PROTO_GO_TEMPS)
	for name in $(PROTO_SRC_NAMES); do \
	  mkdir -p go/vt/proto/$${name}; \
	  cp -a go/vt/.proto.tmp/$${name}.pb.go go/vt/proto/$${name}/$${name}.pb.go; \
	done

$(PROTO_GO_TEMPS): install_protoc-gen-go

$(PROTO_GO_TEMPS): go/vt/.proto.tmp/%.pb.go: proto/%.proto
	mkdir -p go/vt/.proto.tmp
	$(PROTOC_DIR)/protoc -Iproto $< --go_out=plugins=grpc:go/vt/.proto.tmp
	sed -i -e 's,import \([a-z0-9_]*\) ".",import \1 "github.com/youtube/vitess/go/vt/proto/\1",g' $@

# Generate the PHP proto files in a Docker container, and copy them back.
php_proto:
	docker run -ti --name=vitess_php-proto -v $$PWD/proto:/in vitess/bootstrap:common bash -c 'cd $$VTTOP && mkdir -p proto && cp -R /in/* proto/ && tools/proto-gen-php.sh'
	docker cp vitess_php-proto:/vt/src/github.com/youtube/vitess/php/src/descriptor.php php/src/
	docker cp vitess_php-proto:/vt/src/github.com/youtube/vitess/php/src/php.php php/src/
	rm -r php/src/Vitess/Proto/*
	docker cp vitess_php-proto:/vt/src/github.com/youtube/vitess/php/src/Vitess/Proto/. php/src/Vitess/Proto/
	docker rm vitess_php-proto

# This rule builds the bootstrap images for all flavors.
DOCKER_IMAGES_FOR_TEST = mariadb mysql56 mysql57 percona percona57
DOCKER_IMAGES = common $(DOCKER_IMAGES_FOR_TEST)
docker_bootstrap:
	for i in $(DOCKER_IMAGES); do echo "building bootstrap image: $$i"; docker/bootstrap/build.sh $$i || exit 1; done

docker_bootstrap_test:
	flavors='$(DOCKER_IMAGES_FOR_TEST)' && ./test.go -pull=false -parallel=4 -flavor=$${flavors// /,}

docker_bootstrap_push:
	for i in $(DOCKER_IMAGES); do echo "pushing boostrap image: $$i"; docker push vitess/bootstrap:$$i || exit 1; done

# Use this target to update the local copy of your images with the one on Dockerhub.
docker_bootstrap_pull:
	for i in $(DOCKER_IMAGES); do echo "pulling bootstrap image: $$i"; docker pull vitess/bootstrap:$$i || exit 1; done

# Note: The default base and lite images (tag "latest") use MySQL 5.7.
# Images with other MySQL/MariaDB versions get their own tag e.g. "mariadb".
# We never push the non-"latest" tags though and only provide them for convenience for users who want to run something else than MySQL 5.7.
docker_base:
	# Fix permissions before copying files, to avoid AUFS bug.
	chmod -R o=g *
	docker build -t vitess/base .

docker_base_mysql56:
	chmod -R o=g *
	docker build -f Dockerfile.mysql56 -t vitess/base:mysql56 .

docker_base_mariadb:
	chmod -R o=g *
	docker build -f Dockerfile.mariadb -t vitess/base:mariadb .

docker_base_percona:
	chmod -R o=g *
	docker build -f Dockerfile.percona -t vitess/base:percona .

docker_base_percona57:
	chmod -R o=g *
	docker build -f Dockerfile.percona57 -t vitess/base:percona57 .

docker_lite: docker_base
	cd docker/lite && ./build.sh

docker_lite_mysql56: docker_base_mysql56
	cd docker/lite && ./build.sh mysql56

docker_lite_mariadb: docker_base_mariadb
	cd docker/lite && ./build.sh mariadb

docker_lite_percona: docker_base_percona
	cd docker/lite && ./build.sh percona

docker_lite_percona57: docker_base_percona57
	cd docker/lite && ./build.sh percona57

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
