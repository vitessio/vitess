# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MAKEFLAGS = -s

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build build_web test clean unit_test unit_test_cover unit_test_race integration_test proto proto_banner site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_test reshard_tests

all: build

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
	go install $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/...

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

# Remove everything including stuff pulled down by bootstrap.sh
cleanall:
	# symlinks
	for f in config data py-vtdb; do test -L ../../../../$$f && rm ../../../../$$f; done
	# directories created by bootstrap.sh
	# - exclude vtdataroot and vthook as they may have data we want
	rm -rf ../../../../bin ../../../../dist ../../../../lib ../../../../pkg
	# keep the vendor.json file but nothing else under the vendor directory as it's not actually part of the Vitess repo
	rm -rf vendor/cloud.google.com vendor/github.com vendor/golang.org vendor/google.golang.org vendor/gopkg.in
	# other stuff in the go hierarchy that is not under vendor/
	rm -rf ../../../golang.org ../../../honnef.co
	rm -rf ../../../github.com/golang ../../../github.com/kardianos ../../../github.com/kisielk
	# Remind people to run bootstrap.sh again
	echo "Please run bootstrap.sh again to setup your environment"

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

# TODO(mberlin): Remove the manual copy once govendor supports a way to
# install vendor'd programs: https://github.com/kardianos/govendor/issues/117
install_protoc-gen-go:
	mkdir -p $${GOPATH}/src/github.com/golang/
	cp -a vendor/github.com/golang/protobuf $${GOPATH}/src/github.com/golang/
	go install github.com/golang/protobuf/protoc-gen-go

# Find protoc compiler.
# NOTE: We are *not* using the "protoc" binary (as suggested by the grpc Go
#       quickstart for example). Instead, we run "protoc" via the Python
#       wrapper script which is provided by the "grpcio-tools" PyPi package.
#       (The package includes the compiler as library, but not as binary.
#       Therefore, we have to use the wrapper script they provide.)
ifneq ($(wildcard $(VTROOT)/dist/grpc/usr/local/lib/python2.7/site-packages/grpc_tools/protoc.py),)
# IMPORTANT: The next line must not be indented.
PROTOC_COMMAND := python -m grpc_tools.protoc
endif

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_PY_OUTS = $(foreach name, $(PROTO_SRC_NAMES), py/vtproto/$(name)_pb2.py)
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)

# This rule rebuilds all the go and python files from the proto definitions for gRPC.
proto: proto_banner $(PROTO_GO_OUTS) $(PROTO_PY_OUTS)

proto_banner:
ifeq (,$(PROTOC_COMMAND))
	$(error "Cannot find protoc compiler. Did bootstrap.sh succeed, and did you execute 'source dev.env'?")
endif

ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

$(PROTO_PY_OUTS): py/vtproto/%_pb2.py: proto/%.proto
	$(PROTOC_COMMAND) -Iproto $< --python_out=py/vtproto --grpc_python_out=py/vtproto

$(PROTO_GO_OUTS): install_protoc-gen-go proto/*.proto
	for name in $(PROTO_SRC_NAMES); do \
		cd $(VTROOT)/src && PATH=$(VTROOT)/bin:$(PATH) $(VTROOT)/bin/protoc --go_out=plugins=grpc:. -Ivitess.io/vitess/proto vitess.io/vitess/proto/$${name}.proto; \
	done

# Helper targets for building Docker images.
# Please read docker/README.md to understand the different available images.

# This rule builds the bootstrap images for all flavors.
DOCKER_IMAGES_FOR_TEST = mariadb mariadb103 mysql56 mysql57 mysql80 percona percona57 percona80
DOCKER_IMAGES = common $(DOCKER_IMAGES_FOR_TEST)
docker_bootstrap:
	for i in $(DOCKER_IMAGES); do echo "building bootstrap image: $$i"; docker/bootstrap/build.sh $$i || exit 1; done

docker_bootstrap_test:
	flavors='$(DOCKER_IMAGES_FOR_TEST)' && ./test.go -pull=false -parallel=2 -flavor=$${flavors// /,}

docker_bootstrap_push:
	for i in $(DOCKER_IMAGES); do echo "pushing boostrap image: $$i"; docker push vitess/bootstrap:$$i || exit 1; done

# Use this target to update the local copy of your images with the one on Dockerhub.
docker_bootstrap_pull:
	for i in $(DOCKER_IMAGES); do echo "pulling bootstrap image: $$i"; docker pull vitess/bootstrap:$$i || exit 1; done

docker_base:
	# Fix permissions before copying files, to avoid AUFS bug.
	chmod -R o=g *
	docker build -f docker/base/Dockerfile -t vitess/base .

docker_base_mysql56:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.mysql56 -t vitess/base:mysql56 .

docker_base_mysql80:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.mysql80 -t vitess/base:mysql80 .

docker_base_mariadb:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.mariadb -t vitess/base:mariadb .

docker_base_mariadb103:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.mariadb -t vitess/base:mariadb103 .

docker_base_percona:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.percona -t vitess/base:percona .

docker_base_percona57:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.percona57 -t vitess/base:percona57 .

docker_base_percona80:
	chmod -R o=g *
	docker build -f docker/base/Dockerfile.percona80 -t vitess/base:percona80 .

# Run "make docker_lite PROMPT_NOTICE=false" to avoid that the script
# prompts you to press ENTER and confirm that the vitess/base image is not
# rebuild by this target as well.
docker_lite:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE)

docker_lite_mysql56:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) mysql56

docker_lite_mysql57:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) mysql57

docker_lite_mysql80:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) mysql80

docker_lite_mariadb:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) mariadb

docker_lite_mariadb103:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) mariadb103

docker_lite_percona:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) percona

docker_lite_percona57:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) percona57

docker_lite_percona80:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) percona80

docker_lite_alpine:
	cd docker/lite && ./build.sh --prompt=$(PROMPT_NOTICE) alpine

docker_guestbook:
	cd examples/kubernetes/guestbook && ./build.sh

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
	go run test.go -rebalance 5

# Release a version.
# This will generate a tar.gz file into the releases folder with the current source
# as well as the vendored libs.
release: docker_base
	@if [ -z "$VERSION" ]; then \
	  echo "Set the env var VERSION with the release version"; exit 1;\
	fi
	mkdir -p releases
	docker build -f docker/Dockerfile.release -t vitess/release .
	docker run -v ${PWD}/releases:/vt/releases --env VERSION=$(VERSION) vitess/release
	git tag -m Version\ $(VERSION) v$(VERSION)
	echo "A git tag was created, you can push it with:"
	echo "git push origin v$(VERSION)"
	echo "Also, don't forget the upload releases/v$(VERSION).tar.gz file to GitHub releases"

packages: docker_base
	@if [ -z "$VERSION" ]; then \
	  echo "Set the env var VERSION with the release version"; exit 1;\
	fi
	mkdir -p releases
	docker build -f docker/packaging/Dockerfile -t vitess/packaging .
	docker run --rm -v ${PWD}/releases:/vt/releases --env VERSION=$(VERSION) vitess/packaging --package /vt/releases -t deb --deb-no-default-config-files
	docker run --rm -v ${PWD}/releases:/vt/releases --env VERSION=$(VERSION) vitess/packaging --package /vt/releases -t rpm
