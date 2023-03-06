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

ARG DEBIAN_VER=stable-slim

FROM debian:${DEBIAN_VER}

ARG GID=2000
ARG GROUP=vitess
ARG LOGROTATEROOT=/vt
ARG UID=1000
ARG USER=vitess

ENV GROUP $GROUP
ENV LOGROTATEROOT $LOGROTATEROOT
ENV USER $USER

COPY rotate.sh $LOGROTATEROOT/rotate.sh
COPY logrotate.conf.tpl $LOGROTATEROOT/logrotate.conf.tpl

RUN mkdir -p /vt && \
   apt-get update && \
   apt-get upgrade -qq && \
   apt-get install default-mysql-client gettext logrotate -qq --no-install-recommends && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/* && \
   groupadd -r --gid $GID $GROUP && \
   useradd -r -g $GROUP --uid $UID $USER && \
   chown -R $USER:$GROUP /vt && \
   chmod +x $LOGROTATEROOT/rotate.sh

USER $USER

# Use /bin/bash to invoke rotate.sh so we can interpolate $LOGROTATEROOT in the
# rotate.sh path.
#
# See https://stackoverflow.com/a/67206046/614660
ENTRYPOINT ["/bin/bash", "-c", "exec $LOGROTATEROOT/rotate.sh \"${@}\"", "--"]
