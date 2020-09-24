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

FROM debian:buster-slim

ADD logrotate.conf /vt/logrotate.conf

ADD rotate.sh /vt/rotate.sh

RUN mkdir -p /vt && \
   apt-get update && \
   apt-get upgrade -qq && \
   apt-get install logrotate -qq --no-install-recommends && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/* && \
   groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   chmod +x /vt/rotate.sh

ENTRYPOINT [ "/vt/rotate.sh" ]
