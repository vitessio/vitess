# Copyright 2023 The Vitess Authors.
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

FROM debian:bullseye-slim

RUN mkdir -p /vt/dist

# Install dependencies
COPY docker/lite/install_dependencies.sh /vt/dist/install_dependencies.sh
RUN /vt/dist/install_dependencies.sh mysql80

# Set up Vitess user and directory tree.
RUN groupadd -r vitess && useradd -r -g vitess vitess
RUN mkdir -p /vt/vtdataroot && chown -R vitess:vitess /vt

ENV MYSQL_FLAVOR MySQL80

VOLUME /vt/vtdataroot
USER vitess