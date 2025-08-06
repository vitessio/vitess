#!/bin/bash

# Copyright 2025 The Vitess Authors.
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

echo "======================================"
echo "🌍 MULTICOL REGION SHARDING CLUSTER"  
echo "======================================"
echo ""
echo "Setting up Vitess cluster with 4 shards using country-based multicol sharding:"
echo "  Americas region: country_id 0-63   → -40 shard    → 0x00-0x3F keyspace"
echo "  Europe region:   country_id 64-127 → 40-80 shard  → 0x40-0x7F keyspace" 
echo "  APAC region:     country_id 128-191 → 80-C0 shard  → 0x80-0xBF keyspace"
echo "  MEA region:      country_id 192-255 → C0- shard    → 0xC0-0xFF keyspace"
echo ""
echo "Each region gets a full range of 64 country_id values for maximum flexibility"
echo ""

source ../common/env.sh

# Clean up any existing setup - be more thorough
echo "🧹 Cleaning up any existing processes and data..."
pkill -9 -f vtgate 2>/dev/null || true
pkill -9 -f vttablet 2>/dev/null || true
pkill -9 -f vtctld 2>/dev/null || true
pkill -9 -f etcd 2>/dev/null || true
pkill -9 -f mysqld 2>/dev/null || true

# Wait a moment for processes to fully terminate
sleep 2

# Clean up directories completely
if [ -d "${VTDATAROOT}" ]; then
    echo "Removing existing VTDATAROOT: ${VTDATAROOT}"
    rm -rf "${VTDATAROOT}"
fi

# Create fresh VTDATAROOT
mkdir -p "${VTDATAROOT}"

echo "📍 Step 1: Starting etcd..."
CELL=zone1 ../common/scripts/etcd-up.sh

echo "📍 Step 2: Starting vtctld..."
../common/scripts/vtctld-up.sh

echo "📍 Step 3: Creating regional cells..."
# Create cells for each region
vtctldclient AddCellInfo --root /vitess/americas --server-address localhost:2379 americas || echo "Cell americas already exists"
vtctldclient AddCellInfo --root /vitess/europe --server-address localhost:2379 europe || echo "Cell europe already exists"  
vtctldclient AddCellInfo --root /vitess/apac --server-address localhost:2379 apac || echo "Cell apac already exists"
vtctldclient AddCellInfo --root /vitess/mea --server-address localhost:2379 mea || echo "Cell mea already exists"

echo "📍 Step 4: Creating keyspace and applying multicol vschema..."
vtctldclient CreateKeyspace main || fail "Failed to create keyspace"
vtctldclient ApplyVSchema --vschema-file main_vschema_multicol.json main || fail "Failed to apply multicol vschema"

echo "📍 Step 5: Starting tablets for each shard range..."

# Americas shard (-40)
echo "  🇺🇸 Starting Americas shard (-40)..."
CELL=americas TABLET_UID=100 ../common/scripts/mysqlctl-up.sh
SHARD=-40 CELL=americas KEYSPACE=main TABLET_UID=100 ../common/scripts/vttablet-up.sh

# Europe shard (40-80) 
echo "  🇪🇺 Starting Europe shard (40-80)..."
CELL=europe TABLET_UID=200 ../common/scripts/mysqlctl-up.sh
SHARD=40-80 CELL=europe KEYSPACE=main TABLET_UID=200 ../common/scripts/vttablet-up.sh

# APAC shard (80-c0)
echo "  🌏 Starting APAC shard (80-c0)..."
CELL=apac TABLET_UID=300 ../common/scripts/mysqlctl-up.sh
SHARD=80-c0 CELL=apac KEYSPACE=main TABLET_UID=300 ../common/scripts/vttablet-up.sh

# MEA shard (c0-)
echo "  🌍 Starting MEA shard (c0-)..."
CELL=mea TABLET_UID=400 ../common/scripts/mysqlctl-up.sh
SHARD=c0- CELL=mea KEYSPACE=main TABLET_UID=400 ../common/scripts/vttablet-up.sh

echo "📍 Step 6: Initializing tablet primaries..."
# Initialize each tablet as primary for its shard using PlannedReparentShard
vtctldclient PlannedReparentShard --new-primary=americas-0000000100 main/-40
vtctldclient PlannedReparentShard --new-primary=europe-0000000200 main/40-80  
vtctldclient PlannedReparentShard --new-primary=apac-0000000300 main/80-c0
vtctldclient PlannedReparentShard --new-primary=mea-0000000400 main/c0-

echo "📍 Step 7: Waiting for tablets to be healthy..."
for shard in "-40" "40-80" "80-c0" "c0-"; do
    echo "  Waiting for ${shard} shard..."
    wait_for_healthy_shard main "${shard}" 1 || exit 1
done

echo "📍 Step 8: Creating schema on all shards..."
vtctldclient ApplySchema --sql-file create_main_schema.sql main || fail "Failed to create schema"

echo "📍 Step 9: Starting VTGate to watch all regional cells..."
# Start VTGate with custom configuration to watch all cells
web_port=15001
grpc_port=15991
mysql_server_port=15306
mysql_server_socket_path="/tmp/mysql.sock"

vtgate \
  $TOPOLOGY_FLAGS \
  --log_dir $VTDATAROOT/tmp \
  --log-queries-to-file $VTDATAROOT/tmp/vtgate_querylog.txt \
  --port $web_port \
  --grpc-port $grpc_port \
  --mysql-server-port $mysql_server_port \
  --mysql-server-socket-path $mysql_server_socket_path \
  --cell apac \
  --cells_to_watch americas,europe,apac,mea \
  --tablet-types-to-wait PRIMARY \
  --service-map 'grpc-vtgateservice' \
  --pid-file $VTDATAROOT/tmp/vtgate.pid \
  --enable_buffer \
  --mysql-auth-server-impl none \
  --pprof-http \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

# Wait for VTGate to be ready
echo "Waiting for VTGate to start..."
while true; do
  curl -I "http://$hostname:$web_port/debug/status" >/dev/null 2>&1 && break
  sleep 0.5
done
echo "VTGate is ready and watching all regional cells!"

echo "🎉 CLUSTER READY!"
echo "======================================"
echo ""
echo "💡 Your multicol region sharding cluster is running with:"
echo "   - 4 shards: -40, 40-80, 80-c0, c0-"  
echo "   - 1 primary vttablet per shard"
echo "   - Multicol vindex for region/country sharding"
echo ""
echo "🔍 Key URLs:"
echo "   - VTGate: http://localhost:15001/debug/status"
echo "   - VTCtld: http://localhost:15000"
echo "   - MySQL: mysql -h 127.0.0.1 -P 15306"
echo ""
echo "📊 Ready for demo:"
echo "   mysql < insert_customers.sql     # Insert demo data"
echo "   mysql < demo_queries.sql         # Run demo queries"
echo ""
echo "🧹 To cleanup: ./teardown.sh"