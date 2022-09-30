#!/bin/bash

# Copyright 2020 The Vitess Authors.
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

set -u

external=${EXTERNAL_DB:-0}
web_port=${WEB_PORT:-'8080'}
config=${VTORC_CONFIG:-/vt/vtorc/config.json}
# Copy config directory
cp -R /script/vtorc /vt
# Update credentials
if [ $external = 1 ] ; then
    # Terrible substitution but we don't have jq in this image
    # This can be overridden by passing VTORC_CONFIG env variable
    echo "Updating $config..."
    cp /vt/vtorc/default.json /vt/vtorc/tmp.json
    sed  -i '/MySQLTopologyUser/c\  \"MySQLTopologyUser\" : \"'"$DB_USER"'\",' /vt/vtorc/tmp.json
    sed  -i '/MySQLTopologyPassword/c\  \"MySQLTopologyPassword\" : \"'"$DB_PASS"'\",' /vt/vtorc/tmp.json
    sed  -i '/MySQLReplicaUser/c\  \"MySQLReplicaUser\" : \"'"$DB_USER"'\",' /vt/vtorc/tmp.json
    sed  -i '/MySQLReplicaPassword/c\  \"MySQLReplicaPassword\" : \"'"$DB_PASS"'\",' /vt/vtorc/tmp.json
    cat /vt/vtorc/tmp.json
    cp /vt/vtorc/tmp.json /vt/vtorc/config.json
else
    cp /vt/vtorc/default.json /vt/vtorc/config.json
fi

echo "Starting vtorc..."
exec /vt/bin/vtorc \
$TOPOLOGY_FLAGS \
--logtostderr=true \
--port $web_port \
--config $config
