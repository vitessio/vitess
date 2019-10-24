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

FROM vitess/base

USER root

# Install gem and use gem to install fpm
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
   build-essential \
   ruby-dev \
   rubygems \
   rpm \
 && rm -rf /var/lib/apt/lists/* \
 && gem install --no-ri --no-rdoc fpm

RUN mkdir /vt/packaging

COPY docker/packaging/* /vt/packaging/

RUN chown -R vitess:vitess /vt/packaging

USER vitess

ENTRYPOINT ["/bin/bash", "/vt/packaging/package_vitess.sh"]
