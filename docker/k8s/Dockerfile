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

ARG VT_BASE_VER=latest

FROM vitess/base:${VT_BASE_VER} AS base

FROM debian:buster-slim

# TODO: remove when https://github.com/vitessio/vitess/issues/3553 is fixed
RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install default-mysql-client -qq --no-install-recommends && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt/src/vitess.io/vitess
ENV VTDATAROOT /vtdataroot

# Prepare directory structure.
RUN mkdir -p /vt && \
    mkdir -p /vt/bin && \
    mkdir -p /vt/config && \
    mkdir -p /vt/web && \
    mkdir -p /vtdataroot/tabletdata

# Copy CA certs for https calls
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy binaries
COPY --from=base /vt/bin/mysqlctld /vt/bin/
COPY --from=base /vt/bin/mysqlctl /vt/bin/
COPY --from=base /vt/bin/vtctld /vt/bin/
COPY --from=base /vt/bin/vtctl /vt/bin/
COPY --from=base /vt/bin/vtctlclient /vt/bin/
COPY --from=base /vt/bin/vtgate /vt/bin/
COPY --from=base /vt/bin/vttablet /vt/bin/
COPY --from=base /vt/bin/vtworker /vt/bin/
COPY --from=base /vt/bin/vtbackup /vt/bin/

# copy web admin files
COPY --from=base $VTROOT/web /vt/web/

# copy vitess config
COPY --from=base $VTROOT/config/init_db.sql /vt/config/

# my.cnf include files
COPY --from=base $VTROOT/config/mycnf /vt/config/mycnf

# add vitess user and add permissions
RUN groupadd -r --gid 2000 vitess && useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt;
