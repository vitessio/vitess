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

FROM vitess/base AS builder
FROM debian:stretch-slim AS staging

RUN mkdir -p /vt/vtdataroot/ \
    && mkdir -p /vt/bin \
    && mkdir -p /vt/src/vitess.io/vitess/web/vtctld2 \
    && groupadd -r vitess && useradd -r -g vitess vitess

COPY --from=builder /vt/src/vitess.io/vitess/web/vtctld /vt/src/vitess.io/vitess/web/vtctld
COPY --from=builder /vt/src/vitess.io/vitess/web/vtctld2/app /vt/src/vitess.io/vitess/web/vtctld2/app
COPY --from=builder /vt/src/vitess.io/vitess/config /vt/config
COPY --from=builder /vt/bin/mysqlctld /vt/bin/
COPY --from=builder /vt/bin/vtctld /vt/bin/
COPY --from=builder /vt/bin/vtctlclient /vt/bin/
COPY --from=builder /vt/bin/vtgate /vt/bin/
COPY --from=builder /vt/bin/vttablet /vt/bin/
COPY --from=builder /vt/bin/vtworker /vt/bin/
COPY --from=builder /vt/bin/vtbackup /vt/bin/

RUN chown -R vitess:vitess /vt

FROM debian:stretch-slim

# Install dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    gnupg dirmngr ca-certificates wget libdbd-mysql-perl rsync libaio1 libatomic1 libcurl3 libev4 \
 && for i in $(seq 1 10); do apt-key adv --no-tty --recv-keys --keyserver keyserver.ubuntu.com 8C718D3B5072E1F5 && break; done \
 && echo 'deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7' > /etc/apt/sources.list.d/mysql.list \
 && for i in $(seq 1 10); do apt-key adv --no-tty --keyserver keys.gnupg.net --recv-keys 9334A25F8507EFA5 && break; done \
 && echo 'deb http://repo.percona.com/apt stretch main' > /etc/apt/sources.list.d/percona.list && \
    { \
        echo debconf debconf/frontend select Noninteractive; \
        echo percona-server-server-5.7 percona-server-server/root_password password 'unused'; \
        echo percona-server-server-5.7 percona-server-server/root_password_again password 'unused'; \
    } | debconf-set-selections \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
      bzip2 \
      libmysqlclient20 \
      mysql-client \
      mysql-server \
      libjemalloc1 \
      libtcmalloc-minimal4 \
      percona-xtrabackup-24 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r vitess && useradd -r -g vitess vitess

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTTOP /vt/src/vitess.io/vitess
ENV VTROOT /vt
ENV GOTOP $VTTOP/go
ENV VTDATAROOT $VTROOT/vtdataroot
ENV GOBIN $VTROOT/bin
ENV GOPATH $VTROOT
ENV PATH $VTROOT/bin:$PATH
ENV VT_MYSQL_ROOT /usr
ENV PKG_CONFIG_PATH $VTROOT/lib
ENV MYSQL_FLAVOR MySQL56

# Copy binaries (placed by build.sh)
COPY --from=staging /vt/ /vt/

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot
USER vitess
