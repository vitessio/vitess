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
echo "🧹 TEARING DOWN MULTICOL CLUSTER"
echo "======================================"

source ../common/env.sh

echo "Stopping all Vitess processes..."
pkill -9 -f vtgate 2>/dev/null || true
pkill -9 -f vttablet 2>/dev/null || true  
pkill -9 -f vtctld 2>/dev/null || true
pkill -9 -f etcd 2>/dev/null || true

echo "Stopping MySQL instances..."
pkill -9 -f mysqld 2>/dev/null || true

# Wait for processes to fully terminate
sleep 3

echo "Cleaning up data directories..."
if [ -d "${VTDATAROOT}" ]; then
    echo "Removing VTDATAROOT: ${VTDATAROOT}"
    rm -rf "${VTDATAROOT}"
fi

echo "✅ Cluster teardown complete!"