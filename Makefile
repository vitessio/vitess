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

ifndef GOARCH
export GOARCH=$(go env GOARCH)
endif

# This is where Go installs binaries when you run `go install`. By default this
# is $GOPATH/bin. It is better to try to avoid setting this globally, because
# Go will complain if you try to cross-install while this is set.
#
# GOBIN=

ifndef GOOS
export GOOS=$(go env GOOS)
endif

# GOPATH is the root of the Golang installation. `bin` is nested under here. In
# development environments, this is usually $HOME/go. In production and Docker
# environments, this is usually /go.
ifndef GOPATH
export GOPATH=$(go env GOROOT)
endif

# This governs where Vitess binaries are installed during `make install` and
# `make cross-install`. Typically for production builds we set this to /vt.
# PREFIX=

export REWRITER=go/vt/sqlparser/rewriter.go

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build install test clean unit_test unit_test_cover unit_test_race integration_test proto proto_banner site_test site_integration_test docker_bootstrap docker_test docker_unit_test java_test reshard_tests e2e_test e2e_test_race minimaltools tools generate_ci_workflows generate-flag-testdata

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

ifdef VT_EXTRA_BUILD_LDFLAGS
export EXTRA_BUILD_LDFLAGS := $(VT_EXTRA_BUILD_LDFLAGS)
endif

# This should be the root of the vitess Git directory.
ifndef VTROOT
export VTROOT=${PWD}
endif

# This is where Go will install binaries in response to `go build`.
export VTROOTBIN=${VTROOT}/bin

# build the vitess binaries with dynamic dependency on libc
build-dyn:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go build -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		-ldflags "$(EXTRA_BUILD_LDFLAGS) $(shell tools/build_version_flags.sh)"  \
		-o ${VTROOTBIN} ./go/...

# build the vitess binaries statically
build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	# build all the binaries by default with CGO disabled.
	# Binaries will be placed in ${VTROOTBIN}.
	CGO_ENABLED=0 go build \
		    -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		    -ldflags "$(EXTRA_BUILD_LDFLAGS) $(shell tools/build_version_flags.sh)" \
		    -o ${VTROOTBIN} ./go/...
ifndef NOVTADMINBUILD
	echo "Building VTAdmin Web, disable VTAdmin build by setting 'NOVTADMINBUILD'"
	PREFIX="" ./web/vtadmin/build.sh
endif

# cross-build can be used to cross-compile Vitess client binaries
# Outside of select client binaries (namely vtctlclient & vtexplain), cross-compiled Vitess Binaries are not recommended for production deployments
# Usage: GOOS=darwin GOARCH=amd64 make cross-build
cross-build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env

	# For the specified GOOS + GOARCH, build all the binaries by default
	# with CGO disabled. Binaries will be placed in
	# ${VTROOTBIN}/${GOOS}_${GOARG}.
	mkdir -p ${VTROOTBIN}/${GOOS}_${GOARCH}
	CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go build         \
		    -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		    -ldflags "$(EXTRA_BUILD_LDFLAGS) $(shell tools/build_version_flags.sh)" \
		    -o ${VTROOTBIN}/${GOOS}_${GOARCH} ./go/...

	@if [ ! -x "${VTROOTBIN}/${GOOS}_${GOARCH}/vttablet" ]; then \
		echo "Missing vttablet at: ${VTROOTBIN}/${GOOS}_${GOARCH}." && exit; \
	fi

debug:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go build -trimpath \
		$(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		-ldflags "$(EXTRA_BUILD_LDFLAGS) $(shell tools/build_version_flags.sh)"  \
		-gcflags -'N -l' \
		-o ${VTROOTBIN} ./go/...

# install copies the files needed to run Vitess into the given directory tree.
# This target is optimized for docker images. It only installs the files needed for running vitess in docker
# Usage: make install PREFIX=/path/to/install/root
install: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOTBIN}/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtbackup} "$${PREFIX}/bin/"

# Will only work inside the docker bootstrap for now
cross-install: cross-build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "${VTROOTBIN}/${GOOS}_${GOARCH}/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtbackup} "$${PREFIX}/bin/"

# Install local install the binaries needed to run vitess locally
# Usage: make install-local PREFIX=/path/to/install/root
install-local: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctl,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtbackup} "$${PREFIX}/bin/"


# install copies the files needed to run test Vitess using vtcombo into the given directory tree.
# Usage: make install-testing PREFIX=/path/to/install/root
install-testing: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctld,mysqlctl,vtcombo,vttestserver} "$${PREFIX}/bin/"
	# config files
	cp -R config "$${PREFIX}/"

vtctldclient: go/vt/proto/vtctlservice/vtctlservice.pb.go
	make -C go/vt/vtctl/vtctldclient

sqlparser:
	go generate ./go/vt/sqlparser/...

codegen: sqlparser sizegen

demo:
	go install ./examples/demo/demo.go

sizegen:
	go run ./go/tools/sizegen/sizegen.go \
		--in ./go/... \
		--gen vitess.io/vitess/go/pools/smartconnpool.Setting \
		--gen vitess.io/vitess/go/vt/schema.DDLStrategySetting \
		--gen vitess.io/vitess/go/vt/vtgate/engine.Plan \
		--gen vitess.io/vitess/go/vt/vttablet/tabletserver.TabletPlan \
		--gen vitess.io/vitess/go/sqltypes.Result

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

unit_test: build dependency_check demo
	echo $$(date): Running unit tests
	tools/unit_test_runner.sh

e2e_test: build
	echo $$(date): Running endtoend tests
	go test $(VT_GO_PARALLEL) ./go/.../endtoend/...

# Run the code coverage tools, compute aggregate.
unit_test_cover: build dependency_check demo
	source build.env
	go test $(VT_GO_PARALLEL) -count=1 -failfast -covermode=atomic -coverpkg=vitess.io/vitess/go/... -coverprofile=coverage.out ./go/...
	# Handle go tool cover failures due to not handling `//line` directives, which
	# the goyacc compiler adds to the generated parser in sql.go. See:
	# https://github.com/golang/go/issues/41222
	sed -i'' -e '/^vitess.io\/vitess\/go\/vt\/sqlparser\/yaccpar/d' coverage.out
	go tool $(VT_GO_PARALLEL) cover -html=coverage.out

unit_test_race: build dependency_check
	tools/unit_test_race.sh

e2e_test_race: build
	tools/e2e_test_race.sh

e2e_test_cluster: build
	tools/e2e_test_cluster.sh

.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS = -ec

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
	GOBIN=$(VTROOTBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(shell go list -m -f '{{ .Version }}' google.golang.org/protobuf)
	GOBIN=$(VTROOTBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0 # the GRPC compiler its own pinned version
	GOBIN=$(VTROOTBIN) go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@$(shell go list -m -f '{{ .Version }}' github.com/planetscale/vtprotobuf)

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)
# This rule rebuilds all the go files from the proto definitions for gRPC.
proto: $(PROTO_GO_OUTS) vtadmin_web_proto_types

ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

$(PROTO_GO_OUTS): minimaltools install_protoc-gen-go proto/*.proto
	$(VTROOT)/bin/protoc \
		--go_out=. --plugin protoc-gen-go="${VTROOTBIN}/protoc-gen-go" \
		--go-grpc_out=. --plugin protoc-gen-go-grpc="${VTROOTBIN}/protoc-gen-go-grpc" \
		--go-vtproto_out=. --plugin protoc-gen-go-vtproto="${VTROOTBIN}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+size+pool+clone \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/query.Row \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/binlogdata.VStreamRowsResponse \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/binlogdata.VStreamTablesResponse \
		-I${PWD}/dist/vt-protoc-21.3/include:proto $(PROTO_SRCS)
	cp -Rf vitess.io/vitess/go/vt/proto/* go/vt/proto
	rm -rf vitess.io/vitess/go/vt/proto/

# Helper targets for building Docker images.
# Please read docker/README.md to understand the different available images.

# This rule builds the bootstrap images for all flavors.
DOCKER_IMAGES_FOR_TEST = mysql57 mysql80 percona57 percona80
DOCKER_IMAGES = common $(DOCKER_IMAGES_FOR_TEST)
BOOTSTRAP_VERSION=28
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
	${info Building ${2}}
	# Fix permissions before copying files, to avoid AUFS bug other must have read/access permissions
	chmod -R o=rx *;

	if grep -q arm64 <<< ${2}; then \
		echo "Building docker using arm64 buildx"; \
		docker buildx build --platform linux/arm64 -f ${1} -t ${2} --build-arg bootstrap_version=${BOOTSTRAP_VERSION} .; \
	elif [ $$(go env GOOS) != $$(go env GOHOSTOS) ] || [ $$(go env GOARCH) != $$(go env GOHOSTARCH) ]; then \
		echo "Building docker using buildx --platform=$$(go env GOOS)/$$(go env GOARCH)"; \
		docker buildx build --platform "$$(go env GOOS)/$$(go env GOARCH)" -f ${1} -t ${2} --build-arg bootstrap_version=${BOOTSTRAP_VERSION} .; \
	else \
		echo "Building docker using straight docker build"; \
		docker build -f ${1} -t ${2} --build-arg bootstrap_version=${BOOTSTRAP_VERSION} .; \
	fi
endef

docker_base:
	${call build_docker_image,docker/base/Dockerfile,vitess/base}

DOCKER_BASE_SUFFIX = mysql80 percona57 percona80
DOCKER_BASE_TARGETS = $(addprefix docker_base_, $(DOCKER_BASE_SUFFIX))
$(DOCKER_BASE_TARGETS): docker_base_%:
	${call build_docker_image,docker/base/Dockerfile.$*,vitess/base:$*}

docker_base_all: docker_base $(DOCKER_BASE_TARGETS)

docker_lite:
	${call build_docker_image,docker/lite/Dockerfile,vitess/lite}

DOCKER_LITE_SUFFIX = mysql57 ubi7.mysql57 mysql80 ubi7.mysql80 percona57 ubi7.percona57 percona80 ubi7.percona80 testing ubi8.mysql80 ubi8.arm64.mysql80
DOCKER_LITE_TARGETS = $(addprefix docker_lite_,$(DOCKER_LITE_SUFFIX))
$(DOCKER_LITE_TARGETS): docker_lite_%:
	${call build_docker_image,docker/lite/Dockerfile.$*,vitess/lite:$*}

docker_lite_push:
	for i in $(DOCKER_LITE_SUFFIX); do echo "pushing lite image: $$i"; docker push vitess/lite:$$i || exit 1; done

docker_lite_all: docker_lite $(DOCKER_LITE_TARGETS)

docker_local:
	${call build_docker_image,docker/local/Dockerfile,vitess/local}

docker_run_local:
	./docker/local/run.sh

docker_mini:
	${call build_docker_image,docker/mini/Dockerfile,vitess/mini}

DOCKER_VTTESTSERVER_SUFFIX = mysql57 mysql80
DOCKER_VTTESTSERVER_TARGETS = $(addprefix docker_vttestserver_,$(DOCKER_VTTESTSERVER_SUFFIX))
$(DOCKER_VTTESTSERVER_TARGETS): docker_vttestserver_%:
	${call build_docker_image,docker/vttestserver/Dockerfile.$*,vitess/vttestserver:$*}

docker_vttestserver: $(DOCKER_VTTESTSERVER_TARGETS)
# This rule loads the working copy of the code into a bootstrap image,
# and then runs the tests inside Docker.
# Example: $ make docker_test flavor=mysql80
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

create_release:
	./tools/create_release.sh

back_to_dev_mode:
	./tools/back_to_dev_mode.sh

tools:
	echo $$(date): Installing dependencies
	./bootstrap.sh

clean_tools:
	./tools/remove_dependencies.sh

minimaltools:
	echo $$(date): Installing minimal dependencies
	BUILD_JAVA=0 BUILD_CONSUL=0 ./bootstrap.sh

dependency_check:
	./tools/dependency_check.sh

vtadmin_web_install:
	cd web/vtadmin && npm install

# Generate JavaScript/TypeScript bindings for vtadmin-web from the Vitess .proto files.
# Eventually, we'll want to call this target as part of the standard `make proto` target.
# While vtadmin-web is new and unstable, however, we can keep it out of the critical build path.
vtadmin_web_proto_types: vtadmin_web_install
	./web/vtadmin/bin/generate-proto-types.sh

vtadmin_authz_testgen:
	go generate ./go/vt/vtadmin/
	go fmt ./go/vt/vtadmin/

# Generate github CI actions workflow files for unit tests and cluster endtoend tests based on templates in the test/templates directory
# Needs to be called if the templates change or if a new test "shard" is created. We do not need to rebuild tests if only the test/config.json
# is changed by adding a new test to an existing shard. Any new or modified files need to be committed into git
generate_ci_workflows:
	cd test && go run ci_workflow_gen.go && cd ..

generate-flag-testdata:
	./tools/generate_flag_testdata.sh

install_kubectl_kind:
	./tools/get_kubectl_kind.sh
