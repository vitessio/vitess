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

FROM --platform=linux/amd64 golang:1.23.8-bullseye AS builder

# Allows docker builds to set the BUILD_NUMBER
ARG BUILD_NUMBER

WORKDIR /vt/src/vitess.io/vitess

# Create vitess user
RUN groupadd -r vitess && useradd -r -g vitess vitess
RUN mkdir -p /vt/vtdataroot /home/vitess
RUN chown -R vitess:vitess /vt /home/vitess
USER vitess

# Re-copy sources from working tree.
COPY --chown=vitess:vitess . /vt/src/vitess.io/vitess

RUN make install PREFIX=/vt/install

# Start over and build the final image.
FROM --platform=linux/amd64 debian:bullseye-slim

# Install locale required for mysqlsh
RUN apt-get update && apt-get install -y locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen en_US.UTF-8

# Install dependencies
COPY docker/utils/install_dependencies.sh /vt/dist/install_dependencies.sh
RUN /vt/dist/install_dependencies.sh mysql80

# Set up Vitess user and directory tree.
RUN groupadd -r vitess && useradd -r -g vitess vitess
RUN mkdir -p /vt/vtdataroot /home/vitess && chown -R vitess:vitess /vt /home/vitess

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt
ENV VTDATAROOT /vt/vtdataroot
ENV PATH $VTROOT/bin:$PATH

# Copy artifacts from builder layer.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chown=vitess:vitess /vt/install /vt
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/web/vtadmin /vt/web/vtadmin
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/config/init_db.sql /vt/config/
COPY --from=builder --chown=vitess:vitess /vt/src/vitess.io/vitess/config/mycnf /vt/config/

# Create mount point for actual data (e.g. MySQL data dir)
VOLUME /vt/vtdataroot
USER vitess
