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

echo "Adding CellInfo for ${cell}..."
set +e
# shellcheck disable=SC2086
vtctldclient --server internal ${TOPOLOGY_FLAGS} AddCellInfo \
  --root "/vitess/${cell}" \
  --server-address "etcd:2379" \
  "${cell}"
set -e

echo "Starting vtctld..."
# shellcheck disable=SC2086
exec vtctld \
  ${TOPOLOGY_FLAGS} \
  --cell "${cell}" \
  --service-map 'grpc-vtctl,grpc-vtctld' \
  --backup-storage-implementation file \
  --file-backup-storage-root /vt/vtdataroot/backups \
  --port 15000 \
  --grpc-port 15999 \
  --pprof-http \
  --log-format text \
  --config-file-not-found-handling=ignore
