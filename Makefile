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

.PHONY: all build install test clean unit_test unit_test_cover unit_test_race integration_test proto proto_banner site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_test reshard_tests e2e_test e2e_test_race minimaltools tools web_bootstrap web_build web_start generate_ci_workflows

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

ifndef VTROOT
export VTROOT=${PWD}
endif

# We now have CGO code in the build which throws warnings with newer gcc builds.
# See: https://github.com/mattn/go-sqlite3/issues/803
# Work around by dropping optimization level from default -O2.
# Safe, since this code isn't performance critical.
export CGO_CFLAGS := -O1

# regenerate rice-box.go when any of the .cnf files change
embed_config:
	cd go/vt/mysqlctl
	go run github.com/GeertJohan/go.rice/rice embed-go
	go build .

build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go install $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/... && \
		(cd go/cmd/vttablet && go run github.com/GeertJohan/go.rice/rice append --exec=../../../bin/vttablet)

debug:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go install $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" -gcflags -'N -l' ./go/...

# install copies the files needed to run Vitess into the given directory tree.
# This target is optimized for docker images. It only installs the files needed for running vitess in docker
# Usage: make install PREFIX=/path/to/install/root
install: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctl,mysqlctld,vtorc,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtworker,vtbackup} "$${PREFIX}/bin/"

# Install local install the binaries needed to run vitess locally
# Usage: make install-local PREFIX=/path/to/install/root
install-local: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctl,mysqlctld,vtorc,vtctl,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtworker,vtbackup} "$${PREFIX}/bin/"


# install copies the files needed to run test Vitess using vtcombo into the given directory tree.
# Usage: make install-testing PREFIX=/path/to/install/root
install-testing: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctld,mysqlctl,vtcombo,vttestserver} "$${PREFIX}/bin/"
	# config files
	cp -R config "$${PREFIX}/"
	# vtctld web UI files
	mkdir -p "$${PREFIX}/web/vtctld2"
	cp -R web/vtctld2/app "$${PREFIX}/web/vtctld2"

grpcvtctldclient: go/vt/proto/vtctlservice/vtctlservice.pb.go
	make -C go/vt/vtctl/grpcvtctldclient

parser:
	make -C go/vt/sqlparser

codegen: asthelpergen sizegen parser

visitor: asthelpergen
	echo "make visitor has been replaced by make asthelpergen"

asthelpergen:
	go run ./go/tools/asthelpergen/main -in ./go/vt/sqlparser -iface vitess.io/vitess/go/vt/sqlparser.SQLNode -except "*ColName"

sizegen:
	go run ./go/tools/sizegen/sizegen.go \
		-in ./go/vt/... \
	  	-gen vitess.io/vitess/go/vt/vtgate/engine.Plan \
	  	-gen vitess.io/vitess/go/vt/vttablet/tabletserver.TabletPlan

astfmtgen:
	go run ./go/tools/astfmtgen/main.go vitess.io/vitess/go/vt/sqlparser/...

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
	go install github.com/gogo/protobuf/protoc-gen-gofast

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)

# This rule rebuilds all the go files from the proto definitions for gRPC.
proto: $(PROTO_GO_OUTS)

ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

$(PROTO_GO_OUTS): minimaltools install_protoc-gen-go proto/*.proto
	for name in $(PROTO_SRC_NAMES); do \
		$(VTROOT)/bin/protoc --gofast_out=plugins=grpc:. --plugin protoc-gen-gofast="${GOBIN}/protoc-gen-gofast" \
		-I${PWD}/dist/vt-protoc-3.6.1/include:proto proto/$${name}.proto && \
		goimports -w vitess.io/vitess/go/vt/proto/$${name}/$${name}.pb.go; \
	done
	cp -Rf vitess.io/vitess/go/vt/proto/* go/vt/proto
	rm -rf vitess.io/vitess/go/vt/proto/

# Helper targets for building Docker images.
# Please read docker/README.md to understand the different available images.

# This rule builds the bootstrap images for all flavors.
DOCKER_IMAGES_FOR_TEST = mariadb mariadb103 mysql56 mysql57 mysql80 percona percona57 percona80
DOCKER_IMAGES = common $(DOCKER_IMAGES_FOR_TEST)
BOOTSTRAP_VERSION=1
ensure_bootstrap_version:
	find docker/ -type f -exec sed -i "s/^\(ARG bootstrap_version\)=.*/\1=${BOOTSTRAP_VERSION}/" {} \;
	sed -i 's/\(^.*flag.String(\"bootstrap-version\",\) *\"[^\"]\+\"/\1 \"${BOOTSTRAP_VERSION}\"/' test.go

docker_bootstrap:
	for i in $(DOCKER_IMAGES); do echo "building bootstrap image: $$i"; docker/bootstrap/build.sh $$i ${BOOTSTRAP_VERSION} || exit 1; done

docker_bootstrap_test:
	flavors='$(DOCKER_IMAGES_FOR_TEST)' && ./test.go -pull=false -parallel=2 -bootstrap-version=${BOOTSTRAP_VERSION} -flavor=$${flavors// /,}

docker_bootstrap_push:
	for i in $(DOCKER_IMAGES); do echo "pushing bootstrap image: ${BOOTSTRAP_VERSION}-$$i"; docker push vitess/bootstrap:${BOOTSTRAP_VERSION}-$$i || exit 1; done

# Use this target to update the local copy of your images with the one on Dockerhub.
docker_bootstrap_pull:
	for i in $(DOCKER_IMAGES); do echo "pulling bootstrap image: $$i"; docker pull vitess/bootstrap:${BOOTSTRAP_VERSION}-$$i || exit 1; done


define build_docker_image
	# Fix permissions before copying files, to avoid AUFS bug.
	${info Building ${2}}
	chmod -R o=g *;
	docker build -f ${1} -t ${2} --build-arg bootstrap_version=${BOOTSTRAP_VERSION} .;
endef

docker_base:
	${call build_docker_image,docker/base/Dockerfile,vitess/base}

DOCKER_BASE_SUFFIX = mysql56 mysql80 mariadb mariadb103 percona percona57 percona80
DOCKER_BASE_TARGETS = $(addprefix docker_base_, $(DOCKER_BASE_SUFFIX))
$(DOCKER_BASE_TARGETS): docker_base_%:
	${call build_docker_image,docker/base/Dockerfile.$*,vitess/base:$*}

docker_base_all: docker_base $(DOCKER_BASE_TARGETS)

docker_lite:
	${call build_docker_image,docker/lite/Dockerfile,vitess/lite}

DOCKER_LITE_SUFFIX = mysql56 mysql57 ubi7.mysql57 mysql80 ubi7.mysql80 mariadb mariadb103 percona percona57 ubi7.percona57 percona80 ubi7.percona80 alpine testing
DOCKER_LITE_TARGETS = $(addprefix docker_lite_,$(DOCKER_LITE_SUFFIX))
$(DOCKER_LITE_TARGETS): docker_lite_%:
	${call build_docker_image,docker/lite/Dockerfile.$*,vitess/lite:$*}

docker_lite_all: docker_lite $(DOCKER_LITE_TARGETS)

docker_local:
	${call build_docker_image,docker/local/Dockerfile,vitess/local}

docker_mini:
	${call build_docker_image,docker/mini/Dockerfile,vitess/mini}

DOCKER_VTTESTSERVER_SUFFIX = mysql57 mysql80
DOCKER_VTTESTSERVER_TARGETS = $(addprefix docker_vttestserver_,$(DOCKER_VTTESTSERVER_SUFFIX))
$(DOCKER_VTTESTSERVER_TARGETS): docker_vttestserver_%:
	${call build_docker_image,docker/vttestserver/Dockerfile.$*,vitess/vttestserver:$*}

docker_vttestserver: $(DOCKER_VTTESTSERVER_TARGETS)
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

tools:
	echo $$(date): Installing dependencies
	./bootstrap.sh

minimaltools:
	echo $$(date): Installing minimal dependencies
	BUILD_CHROME=0 BUILD_JAVA=0 BUILD_CONSUL=0 ./bootstrap.sh

dependency_check:
	./tools/dependency_check.sh

install_k8s-code-generator: tools.go go.mod
	go install k8s.io/code-generator/cmd/deepcopy-gen
	go install k8s.io/code-generator/cmd/client-gen
	go install k8s.io/code-generator/cmd/lister-gen
	go install k8s.io/code-generator/cmd/informer-gen

DEEPCOPY_GEN=$(GOBIN)/deepcopy-gen
CLIENT_GEN=$(GOBIN)/client-gen
LISTER_GEN=$(GOBIN)/lister-gen
INFORMER_GEN=$(GOBIN)/informer-gen

GEN_BASE_DIR ?= vitess.io/vitess/go/vt/topo/k8stopo

client_go_gen: install_k8s-code-generator
	echo $$(date): Regenerating client-go code
	# Delete and re-generate the deepcopy types
	find $(VTROOT)/go/vt/topo/k8stopo/apis/topo/v1beta1 -name "zz_generated.deepcopy.go" -delete

	# We output to ./ and then copy over the generated files to the appropriate path
	# This is done so we don't have rely on the repository being cloned to `$GOPATH/src/vitess.io/vitess`

	$(DEEPCOPY_GEN) -o ./ \
	--input-dirs $(GEN_BASE_DIR)/apis/topo/v1beta1 \
	-O zz_generated.deepcopy \
	--bounding-dirs $(GEN_BASE_DIR)/apis \
	--go-header-file ./go/vt/topo/k8stopo/boilerplate.go.txt

	# Delete existing code
	rm -rf go/vt/topo/k8stopo/client

	# Generate clientset
	$(CLIENT_GEN) -o ./ \
	--clientset-name versioned \
	--input-base $(GEN_BASE_DIR)/apis \
	--input 'topo/v1beta1' \
	--output-package $(GEN_BASE_DIR)/client/clientset \
	--fake-clientset=true \
	--go-header-file ./go/vt/topo/k8stopo/boilerplate.go.txt

	# Generate listers
	$(LISTER_GEN) -o ./ \
	--input-dirs $(GEN_BASE_DIR)/apis/topo/v1beta1 \
	--output-package $(GEN_BASE_DIR)/client/listers \
	--go-header-file ./go/vt/topo/k8stopo/boilerplate.go.txt

	# Generate informers
	$(INFORMER_GEN) -o ./ \
	--input-dirs $(GEN_BASE_DIR)/apis/topo/v1beta1 \
	--output-package $(GEN_BASE_DIR)/client/informers \
	--versioned-clientset-package $(GEN_BASE_DIR)/client/clientset/versioned \
	--listers-package $(GEN_BASE_DIR)/client/listers \
	--go-header-file ./go/vt/topo/k8stopo/boilerplate.go.txt

	# Move and cleanup
	mv vitess.io/vitess/go/vt/topo/k8stopo/client go/vt/topo/k8stopo/
	mv vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1/zz_generated.deepcopy.go go/vt/topo/k8stopo/apis/topo/v1beta1/zz_generated.deepcopy.go
	rm -rf vitess.io/vitess/go/vt/topo/k8stopo/

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

vtadmin_web_install:
	cd web/vtadmin && npm install

# Generate JavaScript/TypeScript bindings for vtadmin-web from the Vitess .proto files.
# Eventually, we'll want to call this target as part of the standard `make proto` target.
# While vtadmin-web is new and unstable, however, we can keep it out of the critical build path.
vtadmin_web_proto_types: vtadmin_web_install
	./web/vtadmin/bin/generate-proto-types.sh

# Generate github CI actions workflow files for unit tests and cluster endtoend tests based on templates in the test/templates directory
# Needs to be called if the templates change or if a new test "shard" is created. We do not need to rebuild tests if only the test/config.json
# is changed by adding a new test to an existing shard. Any new or modified files need to be committed into git
generate_ci_workflows:
	cd test && go run ci_workflow_gen.go && cd ..
