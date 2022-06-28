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
config=${VTORC_CONFIG:-/vt/orchestrator/config.json}
keyspaces=${KEYSPACES:-'test_keyspace lookup_keyspace'}
sleeptime=${SLEEPTIME:-'10'}

# Copy config directory
cp -R /script/orchestrator /vt
# Update credentials
if [ $external = 1 ] ; then
    # Terrible substitution but we don't have jq in this image
    # This can be overridden by passing VTORC_CONFIG env variable
    echo "Updating $config..."
    cp /vt/orchestrator/default.json /vt/orchestrator/tmp.json
    sed  -i '/MySQLTopologyUser/c\  \"MySQLTopologyUser\" : \"'"$DB_USER"'\",' /vt/orchestrator/tmp.json
    sed  -i '/MySQLTopologyPassword/c\  \"MySQLTopologyPassword\" : \"'"$DB_PASS"'\",' /vt/orchestrator/tmp.json
    sed  -i '/MySQLReplicaUser/c\  \"MySQLReplicaUser\" : \"'"$DB_USER"'\",' /vt/orchestrator/tmp.json
    sed  -i '/MySQLReplicaPassword/c\  \"MySQLReplicaPassword\" : \"'"$DB_PASS"'\",' /vt/orchestrator/tmp.json
    cat /vt/orchestrator/tmp.json
    cp /vt/orchestrator/tmp.json /vt/orchestrator/config.json
else
    cp /vt/orchestrator/default.json /vt/orchestrator/config.json
fi

sleep $sleeptime

# set the correct durability policy for the keyspace
for keyspace in $keyspaces; do
  echo "Set Keyspace Durability Policy ${keyspace} to none"
  /vt/bin/vtctldclient --server vtctld:$GRPC_PORT SetKeyspaceDurabilityPolicy --durability-policy=none $keyspace
done

echo "Starting vtorc..."
exec /vt/bin/vtorc \
$TOPOLOGY_FLAGS \
-logtostderr=true \
-orc_web_dir=/vt/web/orchestrator \
-config $config
