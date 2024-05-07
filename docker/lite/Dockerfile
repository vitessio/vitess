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

# NOTE: We have to build the Vitess binaries from scratch instead of sharing
#       a base image because Docker Hub dropped the feature we relied upon to
#       ensure images contain the right binaries.

# Use a temporary layer for the build stage.
ARG bootstrap_version=31
ARG image="vitess/bootstrap:${bootstrap_version}-common"

FROM "${image}" AS builder

# Allows docker builds to set the BUILD_NUMBER
ARG BUILD_NUMBER

# Install mysql
USER root
RUN apt-get update && apt-get install -y xz-utils
RUN wget https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.37-linux-glibc2.12-x86_64.tar.xz
RUN tar -xf mysql-8.0.37-linux-glibc2.12-x86_64.tar.xz
RUN mv mysql-8.0.37-linux-glibc2.12-x86_64 mysql-generic-binaries

# Re-copy sources from working tree.
COPY --chown=vitess:vitess . /vt/src/vitess.io/vitess

# Build and install Vitess in a temporary output directory.
USER vitess

RUN make install PREFIX=/vt/install

# Start over and build the final image.
FROM --platform=linux/amd64 debian:bullseye-slim

# Install xtrabackup
RUN apt-get update && apt-get -y install gnupg
RUN apt-key adv --no-tty --keyserver keyserver.ubuntu.com --recv-keys 9334A25F8507EFA5
RUN echo 'deb http://repo.percona.com/apt bullseye main' > /etc/apt/sources.list.d/percona.list
RUN apt-get update -y
RUN apt-get install -y percona-xtrabackup-80

# Set up Vitess user and directory tree.
RUN groupadd -r vitess && useradd -r -g vitess vitess
RUN mkdir -p /vt/vtdataroot && chown -R vitess:vitess /vt

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt
ENV VTDATAROOT $VTROOT/vtdataroot
ENV PATH $VTROOT/bin:$PATH

# Copy artifacts from builder layer.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chown=vitess:vitess /vt/install $VTROOT
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/web/vtadmin $VTROOT/web/vtadmin
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/config/init_db.sql $VTROOT/config/
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/config/mycnf $VTROOT/config/
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/mysql-generic-binaries/bin/mysqlbinlog $VTROOT/bin/mysqlbinlog
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/mysql-generic-binaries/lib/private/libcrypto.so.3 $VTROOT/lib/private/libcrypto.so.3
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/mysql-generic-binaries/lib/private/libssl.so.3 $VTROOT/lib/private/libssl.so.3

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot
USER vitess
