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

# This file builds every bootstrap image. The common stage holds the dependencies
# that all flavors share, and each flavor stage adds a database on top of it.
# Build one image with --target, e.g. --target mysql84.

# The image the flavor stages build on. By default it is the common stage below;
# set it to build a flavor on a common image built separately.
ARG base_image=common

FROM --platform=linux/amd64 golang:1.25.13-bookworm@sha256:40dfc169bd5ad8a8617e49c8ead7fe16c6873e79d6937539e9c2e5947b7984ef AS common

# Install Vitess build dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    # TODO(mberlin): Group these to make it easier to understand which library actually requires them.
    ant \
    ca-certificates \
    chromium \
    curl \
    default-jdk \
    etcd-client \
    etcd-server \
    g++ \
    git \
    gnupg \
    make \
    maven \
    unzip \
    zip \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (equivalent to '. dev.env')
ENV VTROOT /vt/src/vitess.io/vitess
ENV VTDATAROOT /vt/vtdataroot
ENV VTPORTSTART 15000
ENV PATH $VTROOT/bin:$VTROOT/dist/maven/bin:$PATH
ENV USER vitess

# Copy files needed for bootstrap
COPY bootstrap.sh dev.env build.env go.mod go.sum /vt/src/vitess.io/vitess/
COPY config /vt/src/vitess.io/vitess/config
COPY tools /vt/src/vitess.io/vitess/tools

# Create vitess user
RUN groupadd -r vitess && useradd -r -g vitess vitess && \
    mkdir -p /vt/vtdataroot /home/vitess && \
    chown -R vitess:vitess /vt /home/vitess

# Download vendored Go dependencies
RUN cd /vt/src/vitess.io/vitess && \
 su vitess -c "/usr/local/go/bin/go mod download"

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot

# The docker lite images copy from the builder in /vt/bin
# Add compatibility to the previous layout for now
RUN su vitess -c "mkdir -p /vt/src/vitess.io/vitess/bin && rm -rf /vt/bin && ln -s /vt/src/vitess.io/vitess/bin /vt/bin"

# Bootstrap Vitess
WORKDIR /vt/src/vitess.io/vitess

USER vitess
RUN ./bootstrap.sh

# If the user doesn't specify a command, load a shell.
CMD ["/bin/bash"]

FROM --platform=linux/amd64 ${base_image} AS mysql80
USER root

# Install MySQL 8.0
RUN for i in $(seq 1 10); do apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com 8C718D3B5072E1F5 && break; done && \
    for i in $(seq 1 10); do apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com A8D3785C && break; done && \
    echo 'deb http://repo.mysql.com/apt/debian/ bookworm mysql-8.0' > /etc/apt/sources.list.d/mysql.list && \
    for i in $(seq 1 10); do apt-key adv --no-tty --keyserver keyserver.ubuntu.com --recv-keys 9334A25F8507EFA5 && break; done && \
    echo 'deb http://repo.percona.com/apt bookworm main' > /etc/apt/sources.list.d/percona.list && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-8.0 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-8.0 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections && \
    apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server libmysqlclient-dev libdbd-mysql-perl rsync libev4 libcurl4-openssl-dev percona-xtrabackup-80 && \
    rm -rf /var/lib/apt/lists/*

USER vitess

FROM --platform=linux/amd64 ${base_image} AS mysql84
USER root

# Install MySQL 8.4
RUN for i in $(seq 1 10); do apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com 8C718D3B5072E1F5 && break; done && \
    for i in $(seq 1 10); do apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com A8D3785C && break; done && \
    echo 'deb http://repo.mysql.com/apt/debian/ bookworm mysql-8.4-lts' > /etc/apt/sources.list.d/mysql.list && \
    for i in $(seq 1 10); do apt-key adv --no-tty --keyserver keyserver.ubuntu.com --recv-keys 9334A25F8507EFA5 && break; done && \
    echo 'deb http://repo.percona.com/pxb-84-lts/apt bookworm main' > /etc/apt/sources.list.d/percona.list && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-8.4 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-8.4 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections && \
    apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server libmysqlclient-dev libdbd-mysql-perl rsync libev4 libcurl4-openssl-dev percona-xtrabackup-84 && \
    rm -rf /var/lib/apt/lists/*

USER vitess

FROM --platform=linux/amd64 ${base_image} AS percona80
USER root

# Install Percona 8.0
RUN for i in $(seq 1 10); do apt-key adv --no-tty --keyserver keyserver.ubuntu.com --recv-keys 9334A25F8507EFA5 && break; done \
    && echo 'deb http://repo.percona.com/ps-80/apt bookworm main' > /etc/apt/sources.list.d/percona.list && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-8.0 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-8.0 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        percona-server-server \
	libperconaserverclient21 \
	percona-server-rocksdb \
	bzip2 \
	libdbd-mysql-perl \
	rsync \
	libev4 \
#    && rm -f /etc/apt/sources.list.d/percona.list \
    && echo 'deb http://repo.percona.com/apt bookworm main' > /etc/apt/sources.list.d/percona.list \
#    { \
#        echo debconf debconf/frontend select Noninteractive; \
#        echo percona-server-server-8.0 percona-server-server/root_password password 'unused'; \
#        echo percona-server-server-8.0 percona-server-server/root_password_again password 'unused'; \
#    } | debconf-set-selections \
    && apt-get update -y \
    && apt-get install -y --no-install-recommends percona-xtrabackup-80 \
    && rm -rf /var/lib/apt/lists/*

USER vitess
