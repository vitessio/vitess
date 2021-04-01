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

# NOTE: This file is also symlinked as "Dockerfile" in the root of our
#       repository because the automated build feature on Docker Hub does not
#       allow to specify a different build context. It always assumes that the
#       build context is the same directory as the Dockerfile is in.
#       "make build" below must be called in our repository's root and
#       therefore we need to have the symlinked "Dockerfile" in there as well.
# TODO(mberlin): Remove the symlink and this note once
# https://github.com/docker/hub-feedback/issues/292 is fixed.

ARG bootstrap_version=1
ARG image="vitess/bootstrap:${bootstrap_version}-mysql57"

FROM "${image}"

# Allows some docker builds to disable CGO
ARG CGO_ENABLED=0

# Allows docker builds to set the BUILD_NUMBER
ARG BUILD_NUMBER

# Re-copy sources from working tree
USER root
COPY . /vt/src/vitess.io/vitess

# Build Vitess
RUN make build

# Fix permissions
RUN chown -R vitess:vitess /vt
USER vitess

