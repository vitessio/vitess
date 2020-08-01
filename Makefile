# Copyright 2019 The Vitess Authors.
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

export GOBIN=$(PWD)/bin
export GO111MODULE=on
export REWRITER=go/vt/sqlparser/rewriter.go

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build install test clean unit_test unit_test_cover unit_test_race integration_test proto proto_banner site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_test reshard_tests e2e_test e2e_test_race minimaltools tools web_bootstrap web_build web_start

all: build

# Set a custom value for -p, the number of packages to be built/tested in parallel.
# This is currently only used by our Travis CI test configuration.
# (Also keep in mind that this value is independent of GOMAXPROCS.)
ifdef VT_GO_PARALLEL_VALUE
export VT_GO_PARALLEL := -p $(VT_GO_PARALLEL_VALUE)
endif

ifdef VT_EXTRA_BUILD_FLAGS
export EXTRA_BUILD_FLAGS := $(VT_EXTRA_BUILD_FLAGS)
endif

embed_config:
	cd go/vt/mysqlctl
	go run github.com/GeertJohan/go.rice/rice embed-go
	go build .

build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go install $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/...

debug:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go install $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" -gcflags -'N -l' ./go/...

# install copies the files needed to run Vitess into the given directory tree.
# Usage: make install PREFIX=/path/to/install/root
install: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctld,vtctld,vtctlclient,vtgate,vttablet,vtworker,vtbackup} "$${PREFIX}/bin/"

# install copies the files needed to run test Vitess using vtcombo into the given directory tree.
# Usage: make install PREFIX=/path/to/install/root
install-testing: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctld,mysqlctl,vtcombo,vttestserver} "$${PREFIX}/bin/"
	# config files
	cp -R config "$${PREFIX}/"
	# vtctld web UI files
	mkdir -p "$${PREFIX}/web/vtctld2"
	cp -R web/vtctld2/app "$${PREFIX}/web/vtctld2"

parser:
	make -C go/vt/sqlparser

visitor:
	go generate go/vt/sqlparser/rewriter.go

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
	rm -rf ./visitorgen

# Remove everything including stuff pulled down by bootstrap.sh
cleanall: clean
	# directories created by bootstrap.sh
	# - exclude vtdataroot and vthook as they may have data we want
	rm -rf bin dist lib pkg
	# Remind people to run bootstrap.sh again
	echo "Please run 'make tools' again to setup your environment"

unit_test: build dependency_check
	echo $$(date): Running unit tests
	tools/unit_test_runner.sh

e2e_test: build
	echo $$(date): Running endtoend tests
	go test $(VT_GO_PARALLEL) ./go/.../endtoend/...

# Run the code coverage tools, compute aggregate.
# If you want to improve in a directory, run:
#   go test -coverprofile=coverage.out && go tool cover -html=coverage.out
unit_test_cover: build
	go test $(VT_GO_PARALLEL) -cover ./go/... | misc/parse_cover.py

unit_test_race: build dependency_check
	tools/unit_test_race.sh

e2e_test_race: build
	tools/e2e_test_race.sh

e2e_test_cluster: build
	tools/e2e_test_cluster.sh

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
	VTROOT=${PWD} mvn -f java/pom.xml -B clean verify

install_protoc-gen-go:
	go install github.com/golang/protobuf/protoc-gen-go

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)

# This rule rebuilds all the go files from the proto definitions for gRPC.
proto: $(PROTO_GO_OUTS)

ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

# TODO(sougou): find a better way around this temp hack.
VTTOP=$(VTROOT)/../../..
$(PROTO_GO_OUTS): install_protoc-gen-go proto/*.proto
	for name in $(PROTO_SRC_NAMES); do \
		cd $(VTTOP)/src && \
		$(VTROOT)/bin/protoc --go_out=plugins=grpc:. -Ivitess.io/vitess/proto vitess.io/vitess/proto/$${name}.proto && \
		goimports -w $(VTROOT)/go/vt/proto/$${name}/$${name}.pb.go; \
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
	for i in $(DOCKER_IMAGES); do echo "pushing bootstrap image: $$i"; docker push vitess/bootstrap:$$i || exit 1; done

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

docker_lite:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile -t vitess/lite .

docker_lite_mysql56:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.mysql56 -t vitess/lite:mysql56 .

docker_lite_mysql57:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.mysql57 -t vitess/lite:mysql57 .

docker_lite_ubi7.mysql57:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.ubi7.mysql57 -t vitess/lite:ubi7.mysql57 .

docker_lite_mysql80:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.mysql80 -t vitess/lite:mysql80 .

docker_lite_ubi7.mysql80:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.ubi7.mysql80 -t vitess/lite:ubi7.mysql80 .

docker_lite_mariadb:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.mariadb -t vitess/lite:mariadb .

docker_lite_mariadb103:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.mariadb103 -t vitess/lite:mariadb103 .

docker_lite_percona:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.percona -t vitess/lite:percona .

docker_lite_percona57:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.percona57 -t vitess/lite:percona57 .

docker_lite_ubi7.percona57:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.ubi7.percona57 -t vitess/lite:ubi7.percona57 .

docker_lite_percona80:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.percona80 -t vitess/lite:percona80 .

docker_lite_ubi7.percona80:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.ubi7.percona80 -t vitess/lite:ubi7.percona80 .

docker_lite_alpine:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.alpine -t vitess/lite:alpine .

docker_lite_testing:
	chmod -R o=g *
	docker build -f docker/lite/Dockerfile.testing -t vitess/lite:testing .

docker_local:
	chmod -R o=g *
	docker build -f docker/local/Dockerfile -t vitess/local .

docker_mini:
	chmod -R o=g *
	docker build -f docker/mini/Dockerfile -t vitess/mini .

# This rule loads the working copy of the code into a bootstrap image,
# and then runs the tests inside Docker.
# Example: $ make docker_test flavor=mariadb
docker_test:
	go run test.go -flavor $(flavor)

docker_unit_test:
	go run test.go -flavor $(flavor) unit

# Release a version.
# This will generate a tar.gz file into the releases folder with the current source
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

tools:
	echo $$(date): Installing dependencies
	./bootstrap.sh

minimaltools:
	echo $$(date): Installing minimal dependencies
	BUILD_CHROME=0 BUILD_JAVA=0 BUILD_CONSUL=0 ./bootstrap.sh

dependency_check:
	./tools/dependency_check.sh

GEN_BASE_DIR ?= ./go/vt/topo/k8stopo

client_go_gen:
	echo $$(date): Regenerating client-go code
	# Delete and re-generate the deepcopy types
	find $(GEN_BASE_DIR)/apis/topo/v1beta1 -type f -name 'zz_generated*' -exec rm '{}' \;
	deepcopy-gen -i $(GEN_BASE_DIR)/apis/topo/v1beta1 -O zz_generated.deepcopy -o ./ --bounding-dirs $(GEN_BASE_DIR)/apis --go-header-file $(GEN_BASE_DIR)/boilerplate.go.txt

	# Delete, generate, and move the client libraries
	rm -rf go/vt/topo/k8stopo/client

	# There is no way to get client-gen to automatically put files in the right place and still have the right import path so we generate and move them

	# Generate client, informers, and listers
	client-gen -o ./ --input 'topo/v1beta1' --clientset-name versioned --input-base 'vitess.io/vitess/go/vt/topo/k8stopo/apis/' -i vitess.io/vitess --output-package vitess.io/vitess/go/vt/topo/k8stopo/client/clientset --go-header-file $(GEN_BASE_DIR)/boilerplate.go.txt
	lister-gen -o ./ --input-dirs  vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1 --output-package vitess.io/vitess/go/vt/topo/k8stopo/client/listers --go-header-file $(GEN_BASE_DIR)/boilerplate.go.txt
	informer-gen -o ./ --input-dirs  vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1 --versioned-clientset-package vitess.io/vitess/go/vt/topo/k8stopo/client/clientset/versioned --listers-package vitess.io/vitess/go/vt/topo/k8stopo/client/listers --output-package vitess.io/vitess/go/vt/topo/k8stopo/client/informers --go-header-file $(GEN_BASE_DIR)/boilerplate.go.txt

	# Move and cleanup
	mv vitess.io/vitess/go/vt/topo/k8stopo/client go/vt/topo/k8stopo/
	rmdir -p vitess.io/vitess/go/vt/topo/k8stopo/

# Check prerequisites and install dependencies
web_bootstrap:
	./tools/web_bootstrap.sh

# Do a production build of the vtctld UI.
# This target needs to be manually run every time any file within web/vtctld2/app 
# is modified to regenerate rice-box.go
web_build: web_bootstrap
	./tools/web_build.sh

# Start a front-end dev server with hot reloading on http://localhost:4200.
# This expects that you have a vtctld API server running on http://localhost:15000.
# Following the local Docker install guide is recommended: https://vitess.io/docs/get-started/local-docker/
web_start: web_bootstrap
	cd web/vtctld2 && npm run start
