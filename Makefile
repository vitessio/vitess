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
GIT_STATUS := $(shell git status --porcelain)

export GOBIN=$(PWD)/bin
export REWRITER=go/vt/sqlparser/rewriter.go

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all
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
.PHONY: embed_config
embed_config:
	cd go/vt/mysqlctl && go run github.com/GeertJohan/go.rice/rice embed-go
	cd go/vt/mysqlctl && go build .

# build the vitess binaries with dynamic dependency on libc
.PHONY: build-dyn
build-dyn:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go install -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/...
	(cd go/cmd/vttablet && go run github.com/GeertJohan/go.rice/rice append --exec=../../../bin/vttablet)

# build the vitess binaries statically
.PHONY: build
build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	# build all the binaries by default with CGO disabled
	CGO_ENABLED=0 go install -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/...
	# embed local resources in the vttablet executable
	(cd go/cmd/vttablet && go run github.com/GeertJohan/go.rice/rice append --exec=../../../bin/vttablet)
	# build vtorc with CGO, because it depends on sqlite
	CGO_ENABLED=1 go install -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/cmd/vtorc/...

# cross-build can be used to cross-compile Vitess client binaries
# Outside of select client binaries (namely vtctlclient & vtexplain), cross-compiled Vitess Binaries are not recommended for production deployments
# Usage: GOOS=darwin GOARCH=amd64 make cross-build
.PHONY: cross-build
cross-build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	# In order to cross-compile, go install requires GOBIN to be unset
	export GOBIN=""
	# For the specified GOOS + GOARCH, build all the binaries by default with CGO disabled
	CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go install -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" ./go/...
	# unset GOOS and embed local resources in the vttablet executable
	(cd go/cmd/vttablet && unset GOOS && unset GOARCH && go run github.com/GeertJohan/go.rice/rice --verbose append --exec=$${HOME}/go/bin/${GOOS}_${GOARCH}/vttablet)
	# Cross-compiling w/ cgo isn't trivial and we don't need vtorc, so we can skip building it

.PHONY: debug
debug:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go install -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) -ldflags "$(shell tools/build_version_flags.sh)" -gcflags -'N -l' ./go/...

# install copies the files needed to run Vitess into the given directory tree.
# This target is optimized for docker images. It only installs the files needed for running vitess in docker
# Usage: make install PREFIX=/path/to/install/root
.PHONY: install
install: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtworker,vtbackup} "$${PREFIX}/bin/"

# Install local install the binaries needed to run vitess locally
# Usage: make install-local PREFIX=/path/to/install/root
.PHONY: install-local
install-local: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctl,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtworker,vtbackup} "$${PREFIX}/bin/"


# install copies the files needed to run test Vitess using vtcombo into the given directory tree.
# Usage: make install-testing PREFIX=/path/to/install/root
.PHONY: install-testing
install-testing: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctld,mysqlctl,vtcombo,vttestserver} "$${PREFIX}/bin/"
	# config files
	cp -R config "$${PREFIX}/"
	# vtctld web UI files
	mkdir -p "$${PREFIX}/web/vtctld2"
	cp -R web/vtctld2/app "$${PREFIX}/web/vtctld2"

.PHONY: vtctldclient
vtctldclient: go/vt/proto/vtctlservice/vtctlservice.pb.go
	make -C go/vt/vtctl/vtctldclient

.PHONY: parser
parser:
	make -C go/vt/sqlparser

.PHONY: demo
demo:
	go install ./examples/demo/demo.go

.PHONY: codegen
codegen: asthelpergen sizegen parser astfmtgen

.PHONY: visitor
visitor: asthelpergen
	echo "make visitor has been replaced by make asthelpergen"

.PHONY: asthelpergen
asthelpergen:
	go run ./go/tools/asthelpergen/main -in ./go/vt/sqlparser -iface vitess.io/vitess/go/vt/sqlparser.SQLNode -except "*ColName"

.PHONY: sizegen
sizegen:
	go run ./go/tools/sizegen/sizegen.go \
		-in ./go/... \
	  	-gen vitess.io/vitess/go/vt/vtgate/engine.Plan \
	  	-gen vitess.io/vitess/go/vt/vttablet/tabletserver.TabletPlan \
	  	-gen vitess.io/vitess/go/sqltypes.Result

.PHONY: astfmtgen
astfmtgen:
	go run ./go/tools/astfmtgen/main.go vitess.io/vitess/go/vt/sqlparser/...

# To pass extra flags, run test.go manually.
# For example: go run test.go -docker=false -- --extra-flag
# For more info see: go run test.go -help
.PHONY: test
test:
	go run test.go -docker=false

.PHONY: site_test
site_test: unit_test site_integration_test

.PHONY: clean
clean:
	go clean -i ./go/...
	rm -rf third_party/acolyte
	rm -rf go/vt/.proto.tmp

# Remove everything including stuff pulled down by bootstrap.sh
.PHONY: cleanall
cleanall: clean
	# directories created by bootstrap.sh
	# - exclude vtdataroot and vthook as they may have data we want
	rm -rf bin dist lib pkg
	# Remind people to run bootstrap.sh again
	echo "Please run 'make tools' again to setup your environment"

.PHONY: unit_test
unit_test: build dependency_check demo
	echo $$(date): Running unit tests
	tools/unit_test_runner.sh

.PHONY: e2e_test
e2e_test: build
	echo $$(date): Running endtoend tests
	go test $(VT_GO_PARALLEL) ./go/.../endtoend/...

# Run the code coverage tools, compute aggregate.
# If you want to improve in a directory, run:
#   go test -coverprofile=coverage.out && go tool cover -html=coverage.out
.PHONY: unit_test_cover
unit_test_cover: build
	go test $(VT_GO_PARALLEL) -cover ./go/... | misc/parse_cover.py

.PHONY: unit_test_race
unit_test_race: build dependency_check
	tools/unit_test_race.sh

.PHONY: e2e_test_race
e2e_test_race: build
	tools/e2e_test_race.sh

.PHONY: e2e_test_cluster
e2e_test_cluster: build
	tools/e2e_test_cluster.sh

.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS = -ec

# Run the following tests after making worker changes.
.PHONY: worker_test
worker_test:
	go test ./go/vt/worker/
	go run test.go -docker=false -tag=worker_test

.PHONY: site_integration_test
site_integration_test:
	go run test.go -docker=false -tag=site_test

.PHONY: java_test
java_test:
	go install ./go/cmd/vtgateclienttest ./go/cmd/vtcombo
	VTROOT=${PWD} mvn -f java/pom.xml -B clean verify

.PHONY: install_protoc-gen-go
install_protoc-gen-go:
	go install google.golang.org/protobuf/cmd/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)

# This rule rebuilds all the go files from the proto definitions for gRPC.
.PHONY: proto
proto: $(PROTO_GO_OUTS)

ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

.PHONY: $(PROTO_GO_OUTS)
$(PROTO_GO_OUTS): minimaltools install_protoc-gen-go proto/*.proto
	$(VTROOT)/bin/protoc \
		--go_out=. --plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
		--go-grpc_out=. --plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
		--go-vtproto_out=. --plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+size+pool \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/query.Row \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/binlogdata.VStreamRowsResponse \
		-I${PWD}/dist/vt-protoc-3.19.4/include:proto $(PROTO_SRCS)
	cp -Rf vitess.io/vitess/go/vt/proto/* go/vt/proto
	rm -rf vitess.io/vitess/go/vt/proto/

# Helper targets for building Docker images.
# Please read docker/README.md to understand the different available images.

# This rule builds the bootstrap images for all flavors.
DOCKER_IMAGES_FOR_TEST = mariadb mariadb103 mysql56 mysql57 mysql80 percona percona57 percona80
DOCKER_IMAGES = common $(DOCKER_IMAGES_FOR_TEST)
BOOTSTRAP_VERSION=4
.PHONY: ensure_bootstrap_version
ensure_bootstrap_version:
	find docker/ -type f -exec sed -i "s/^\(ARG bootstrap_version\)=.*/\1=${BOOTSTRAP_VERSION}/" {} \;
	sed -i 's/\(^.*flag.String(\"bootstrap-version\",\) *\"[^\"]\+\"/\1 \"${BOOTSTRAP_VERSION}\"/' test.go

.PHONY: docker_bootstrap
docker_bootstrap:
	for i in $(DOCKER_IMAGES); do echo "building bootstrap image: $$i"; docker/bootstrap/build.sh $$i ${BOOTSTRAP_VERSION} || exit 1; done

.PHONY: docker_bootstrap_test
docker_bootstrap_test:
	flavors='$(DOCKER_IMAGES_FOR_TEST)' && ./test.go -pull=false -parallel=2 -bootstrap-version=${BOOTSTRAP_VERSION} -flavor=$${flavors// /,}

.PHONY: docker_bootstrap_push
docker_bootstrap_push:
	for i in $(DOCKER_IMAGES); do echo "pushing bootstrap image: ${BOOTSTRAP_VERSION}-$$i"; docker push vitess/bootstrap:${BOOTSTRAP_VERSION}-$$i || exit 1; done

# Use this target to update the local copy of your images with the one on Dockerhub.
.PHONY: docker_bootstrap_pull
docker_bootstrap_pull:
	for i in $(DOCKER_IMAGES); do echo "pulling bootstrap image: $$i"; docker pull vitess/bootstrap:${BOOTSTRAP_VERSION}-$$i || exit 1; done

define build_docker_image
	${info Building ${2}}
	# Fix permissions before copying files, to avoid AUFS bug other must have read/access permissions
	chmod -R o=rx *;
	docker build -f ${1} -t ${2} --build-arg bootstrap_version=${BOOTSTRAP_VERSION} .;
endef

.PHONY: docker_base
docker_base:
	${call build_docker_image,docker/base/Dockerfile,vitess/base}

DOCKER_BASE_SUFFIX = mysql56 mysql80 mariadb mariadb103 percona percona57 percona80
DOCKER_BASE_TARGETS = $(addprefix docker_base_, $(DOCKER_BASE_SUFFIX))
.PHONY: $(DOCKER_BASE_TARGETS)
$(DOCKER_BASE_TARGETS): docker_base_%:
	${call build_docker_image,docker/base/Dockerfile.$*,vitess/base:$*}

.PHONY: docker_base_all
docker_base_all: docker_base $(DOCKER_BASE_TARGETS)

.PHONY: docker_lite
docker_lite:
	${call build_docker_image,docker/lite/Dockerfile,vitess/lite}

DOCKER_LITE_SUFFIX = mysql56 mysql57 ubi7.mysql57 mysql80 ubi7.mysql80 mariadb mariadb103 percona percona57 ubi7.percona57 percona80 ubi7.percona80 alpine testing
DOCKER_LITE_TARGETS = $(addprefix docker_lite_,$(DOCKER_LITE_SUFFIX))

.PHONY: $(DOCKER_LITE_TARGETS)
$(DOCKER_LITE_TARGETS): docker_lite_%:
	${call build_docker_image,docker/lite/Dockerfile.$*,vitess/lite:$*}

.PHONY: docker_lite_all
docker_lite_all: docker_lite $(DOCKER_LITE_TARGETS)

.PHONY: docker_local
docker_local:
	${call build_docker_image,docker/local/Dockerfile,vitess/local}

.PHONY: docker_run_local
docker_run_local:
	./docker/local/run.sh

.PHONY: docker_mini
docker_mini:
	${call build_docker_image,docker/mini/Dockerfile,vitess/mini}

DOCKER_VTTESTSERVER_SUFFIX = mysql57 mysql80
DOCKER_VTTESTSERVER_TARGETS = $(addprefix docker_vttestserver_,$(DOCKER_VTTESTSERVER_SUFFIX))
.PHONY: $(DOCKER_VTTESTSERVER_TARGETS)
$(DOCKER_VTTESTSERVER_TARGETS): docker_vttestserver_%:
	${call build_docker_image,docker/vttestserver/Dockerfile.$*,vitess/vttestserver:$*}

.PHONY: docker_vttestserver
docker_vttestserver: $(DOCKER_VTTESTSERVER_TARGETS)
# This rule loads the working copy of the code into a bootstrap image,
# and then runs the tests inside Docker.
# Example: $ make docker_test flavor=mariadb
.PHONY: docker_test
docker_test:
	go run test.go -flavor $(flavor)

.PHONY: docker_unit_test
docker_unit_test:
	go run test.go -flavor $(flavor) unit

# Release a version.
# This will generate a tar.gz file into the releases folder with the current source
.PHONY: release
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

.PHONY: do_release
do_release:
	./tools/do_release.sh

.PHONY: tools
tools:
	echo $$(date): Installing dependencies
	./bootstrap.sh

.PHONY: minimaltools
minimaltools:
	echo $$(date): Installing minimal dependencies
	BUILD_CHROME=0 BUILD_JAVA=0 BUILD_CONSUL=0 ./bootstrap.sh

.PHONY: dependency_check
dependency_check:
	./tools/dependency_check.sh

.PHONY: install_k8s-code-generator
install_k8s-code-generator: tools/tools.go go.mod
	go install k8s.io/code-generator/cmd/deepcopy-gen
	go install k8s.io/code-generator/cmd/client-gen
	go install k8s.io/code-generator/cmd/lister-gen
	go install k8s.io/code-generator/cmd/informer-gen

DEEPCOPY_GEN=$(GOBIN)/deepcopy-gen
CLIENT_GEN=$(GOBIN)/client-gen
LISTER_GEN=$(GOBIN)/lister-gen
INFORMER_GEN=$(GOBIN)/informer-gen

GEN_BASE_DIR ?= vitess.io/vitess/go/vt/topo/k8stopo

.PHONY: client_go_gen
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
.PHONY: web_bootstrap
web_bootstrap:
	./tools/web_bootstrap.sh

# Do a production build of the vtctld UI.
# This target needs to be manually run every time any file within web/vtctld2/app
# is modified to regenerate rice-box.go
.PHONY: web_build
web_build: web_bootstrap
	./tools/web_build.sh

# Start a front-end dev server with hot reloading on http://localhost:4200.
# This expects that you have a vtctld API server running on http://localhost:15000.
# Following the local Docker install guide is recommended: https://vitess.io/docs/get-started/local-docker/
.PHONY: web_start
web_start: web_bootstrap
	cd web/vtctld2 && npm run start

.PHONY: vtadmin_web_install
vtadmin_web_install:
	cd web/vtadmin && npm install

# Generate JavaScript/TypeScript bindings for vtadmin-web from the Vitess .proto files.
# Eventually, we'll want to call this target as part of the standard `make proto` target.
# While vtadmin-web is new and unstable, however, we can keep it out of the critical build path.
.PHONY: vtadmin_web_proto_types
vtadmin_web_proto_types: vtadmin_web_install
	./web/vtadmin/bin/generate-proto-types.sh

# Generate github CI actions workflow files for unit tests and cluster endtoend tests based on templates in the test/templates directory
# Needs to be called if the templates change or if a new test "shard" is created. We do not need to rebuild tests if only the test/config.json
# is changed by adding a new test to an existing shard. Any new or modified files need to be committed into git
.PHONY: generate_ci_workflows
generate_ci_workflows:
	cd test && go run ci_workflow_gen.go && cd ..

.PHONY: release-notes
release-notes:
	go run ./go/tools/release-notes -from "$(FROM)" -to "$(TO)" -version "$(VERSION)" -summary "$(SUMMARY)"
