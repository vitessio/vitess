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
FROM vitess/k8s:${VT_BASE_VER} AS k8s

FROM debian:buster-slim
ARG ORC_VER='3.2.3'

RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install wget ca-certificates jq -qq --no-install-recommends && \
   wget https://github.com/openark/orchestrator/releases/download/v${ORC_VER}/orchestrator_${ORC_VER}_amd64.deb && \
   dpkg -i orchestrator_${ORC_VER}_amd64.deb && \
   rm orchestrator_${ORC_VER}_amd64.deb && \
   apt-get purge wget -qq && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Copy vtctlclient to be used to notify
COPY --from=k8s /vt/bin/vtctlclient /usr/bin/

WORKDIR /usr/local/orchestrator
CMD ["./orchestrator", "--config=/conf/orchestrator.conf.json", "http"]
