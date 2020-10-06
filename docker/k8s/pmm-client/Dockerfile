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
ARG PMM_CLIENT_VER='1.17.4'

RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install procps wget ca-certificates -qq --no-install-recommends && \
   wget https://www.percona.com/redir/downloads/pmm-client/${PMM_CLIENT_VER}/binary/debian/buster/x86_64/pmm-client_${PMM_CLIENT_VER}-1.buster_amd64.deb && \
   dpkg -i pmm-client_${PMM_CLIENT_VER}-1.buster_amd64.deb && \
   rm pmm-client_${PMM_CLIENT_VER}-1.buster_amd64.deb && \
   apt-get purge wget ca-certificates -qq && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Copy CA certs for https calls
COPY --from=k8s /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
