#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

set -euo pipefail

cell=${CELL:-'zone1'}

echo "Starting vtgate..."
# shellcheck disable=SC2086
exec vtgate \
  ${TOPOLOGY_FLAGS} \
  --port 15001 \
  --grpc-port 15991 \
  --mysql-server-port 15306 \
  --cell "${cell}" \
  --cells-to-watch "${cell}" \
  --tablet-types-to-wait PRIMARY,REPLICA \
  --service-map 'grpc-vtgateservice' \
  --enable-buffer \
  --mysql-auth-server-impl none \
  --pprof-http \
  --log-format text \
  --config-file-not-found-handling=ignore
