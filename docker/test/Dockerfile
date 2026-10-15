# Copyright 2026 The Vitess Authors.
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

# This file builds the image that "test.go -docker -docker-image" runs the tests
# in: a bootstrap image plus the source tree and its build. Every test then
# starts from the built tree instead of running "make build" itself. The image
# is built for one commit and is not meant to be published.
#
# Build it from the root of the source tree:
#
#   docker build -f docker/test/Dockerfile \
#     --build-arg bootstrap_image=vitess/bootstrap:ci-mysql84 -t vitess-test .

ARG bootstrap_image=vitess/bootstrap:ci-mysql84

# Go module and build caches to start from. These stages are empty; pass
# directories with --build-context gomodcache=<dir> and gocache=<dir> to use
# them instead, e.g. "go env GOMODCACHE" and "go env GOCACHE" of the same Go
# version.
FROM scratch AS gomodcache
FROM scratch AS gocache

FROM ${bootstrap_image}

COPY --from=gomodcache --chown=vitess:vitess . /go/pkg/mod/
COPY --from=gocache --chown=vitess:vitess . /home/vitess/.cache/go-build/
COPY --chown=vitess:vitess . /vt/src/vitess.io/vitess

# Without a cache mount, the Go module and build caches stay in the image. A
# test compiled with the same CGO_ENABLED=0 and -trimpath as "make build" then
# only compiles its own packages.
RUN NOVTADMINBUILD=1 make build
