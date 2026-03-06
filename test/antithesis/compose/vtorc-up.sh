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

SOURCE_DB=${SOURCE_DB:-0}
WEB_PORT=${WEB_PORT:-"8080"}
VTORC_CONFIG=${VTORC_CONFIG:-/vt/vtorc/config.json}
TOPOLOGY_FLAGS=${TOPOLOGY_FLAGS:-""}

source_db=$SOURCE_DB
web_port=$WEB_PORT
config=$VTORC_CONFIG
# Copy config directory
cp -R /script/vtorc /vt
# Update credentials
if [ "$source_db" = 1 ] ; then
    # Terrible substitution but we don't have jq in this image
    # This can be overridden by passing VTORC_CONFIG env variable
    echo "Updating $config..."
    cp /vt/vtorc/default.json /vt/vtorc/tmp.json
    cat /vt/vtorc/tmp.json
    cp /vt/vtorc/tmp.json /vt/vtorc/config.json
else
    cp /vt/vtorc/default.json /vt/vtorc/config.json
fi

echo "Starting vtorc..."
read -r -a topo_args <<< "$TOPOLOGY_FLAGS"

exec /vt/bin/vtorc \
"${topo_args[@]}" \
--logtostderr=true \
--port "$web_port" \
--config-file "$config"
