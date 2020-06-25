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
FROM vitess/base AS base

FROM vitess/lite

USER root

RUN apt-get update
RUN apt-get install -y sudo curl vim python3 jq sqlite3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install minivitess dependencies
COPY docker/mini/install_mini_dependencies.sh /vt/dist/install_mini_dependencies.sh
RUN /vt/dist/install_mini_dependencies.sh

COPY docker/mini/orchestrator-vitess-mini.conf.json /etc/orchestrator.conf.json
RUN chown vitess:vitess /etc/orchestrator.conf.json

COPY docker/mini/docker-entry /vt/dist/docker/mini/docker-entry
COPY examples/local/scripts /vt/dist/scripts
COPY examples/local/env.sh /vt/dist/scripts/env.sh
COPY docker/mini/vtctld-mini-up.sh /vt/dist/scripts/vtctld-mini-up.sh
COPY docker/mini/vttablet-mini-up.sh /vt/dist/scripts/vttablet-mini-up.sh
COPY docker/mini/orchestrator-up.sh /vt/dist/scripts/orchestrator-up.sh
RUN echo "hostname=127.0.0.1" >> /vt/dist/scripts/env.sh
RUN cat /vt/dist/scripts/env.sh | egrep "^alias" >> /etc/bash.bashrc

COPY --from=base /vt/bin/vtctl /vt/bin/
COPY --from=base /vt/bin/mysqlctl /vt/bin/

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt/src/vitess.io/vitess
ENV VTDATAROOT /vt/vtdataroot
ENV PATH $VTROOT/bin:$PATH
ENV PATH="/vt/bin:${PATH}"
ENV PATH="/var/opt/etcd:${PATH}"
ENV TOPO="etcd"

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot
USER vitess
EXPOSE 15000-15200 16000-16200
CMD /vt/dist/docker/mini/docker-entry && /bin/bash
