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

# The vitesst e2e test image: a MySQL flavor installed by the repo's standard
# install script, plus Vitess binaries compiled in the builder stage. The
# binaries are copied last so the MySQL layers cache across source changes.
#
# Build from the repo root, e.g.:
#   docker buildx bake --load -f go/vitesst/docker-bake.hcl

ARG BASE_IMAGE=debian:trixie-slim

FROM golang:1.26-alpine AS builder

WORKDIR /vt/src/vitess.io/vitess

COPY go.mod go.sum ./
RUN go mod download

COPY config config
COPY go go

# Extra Go build tags. The debug2pc bake target sets debug2PC for the
# transaction/twopc fault-injection suites.
ARG EXTRA_BUILD_TAGS=""

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -tags "${EXTRA_BUILD_TAGS}" -o /vt/bin/ \
    ./go/cmd/mysqlctl ./go/cmd/mysqlctld ./go/cmd/vtorc ./go/cmd/vtadmin \
    ./go/cmd/vtctld ./go/cmd/vtctlclient ./go/cmd/vtctldclient \
    ./go/cmd/vtgate ./go/cmd/vttablet ./go/cmd/vtbackup

FROM ${BASE_IMAGE}

ARG FLAVOR=mysql84

# MySQL version passed to install_dependencies.sh. Empty uses the script's
# default; the bake file pins versions that exist on cdn.mysql.com, since the
# arm64 tarball for the script's default can rotate off the CDN.
ARG MYSQL_VERSION=""

# Pin the vitess uid/gid to 999 before any package install can claim them:
# the framework mounts tmpfs data directories with uid=999,gid=999.
RUN groupadd -r -g 999 vitess && useradd -r -u 999 -g vitess vitess

# Locale required for mysql-shell; tar and xz for the arm64 MySQL tarball
# install path; libncurses6 for the tarball's mysql client, which apt-based
# installs pull in as a dependency but the tarball does not.
RUN apt-get update && apt-get install -y --no-install-recommends locales tar xz-utils libncurses6 \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

COPY docker/utils/install_dependencies.sh /vt/dist/install_dependencies.sh
RUN /vt/dist/install_dependencies.sh ${FLAVOR} ${MYSQL_VERSION}

COPY --chown=vitess:vitess config/init_db.sql /vt/config/init_db.sql
COPY --chown=vitess:vitess config/mycnf /vt/config/mycnf

ENV VTROOT=/vt
ENV VTDATAROOT=/vt/vtdataroot
ENV PATH=/vt/bin:$PATH

# /vt/files is the drop zone for files the framework and tests copy into
# containers (init_db.sql, supervisor script, certs, ...). /vt/backups is the
# mount point for the shared backup volume: it exists in the image, owned by
# vitess, so a fresh volume inherits that ownership on first mount.
RUN mkdir -p /vt/vtdataroot /vt/files /vt/backups && chown vitess:vitess /vt/vtdataroot /vt/files /vt/backups

# Keep this last: it is the only layer that changes when sources change.
COPY --from=builder --chown=vitess:vitess /vt/bin /vt/bin

USER vitess
