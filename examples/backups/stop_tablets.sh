#!/bin/bash

# Copyright 2022 The Vitess Authors.
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

# this script brings up new tablets for the two new shards that we will
# be creating in the customer keyspace and copies the schema

source ../common/env.sh

for tablet in 100 200 300; do
  if vtctldclient --action_timeout 1s --server localhost:15999 GetTablet zone1-$tablet >/dev/null 2>&1; then
    # The zero tablet is up. Try to shutdown 0-2 tablet + mysqlctl
    for i in 0 1 2; do
      uid=$(($tablet + $i))
      echo "Shutting down tablet zone1-$uid"
      CELL=zone1 TABLET_UID=$uid ../common/scripts/vttablet-down.sh
      echo "Shutting down mysql zone1-$uid"
      CELL=zone1 TABLET_UID=$uid ../common/scripts/mysqlctl-down.sh
      echo "Removing tablet directory zone1-$uid"
      vtctldclient DeleteTablets --allow-primary zone1-$uid

      for ((i=0; i<30; i++)); do
          # Redirect stderr to a temporary file
          temp_file=$(mktemp)
          rm -Rf $VTDATAROOT/vt_0000000$uid 2>"$temp_file"

          if grep -q 'Directory not empty' "$temp_file"; then
              echo "Directory not empty, retrying..."
          elif [ ! -s "$temp_file" ]; then
              echo "Deletion succeeded."
              rm -f "$temp_file"
              break
          else
              echo "An error occurred."
              cat "$temp_file"
          fi
          rm -f "$temp_file"
          sleep 1
      done
    done
  fi
done
