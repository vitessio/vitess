#!/bin/bash

# Copyright 2023 The Vitess Authors.
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

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

function stop_vtadmin() {
  local name="$1"
  local file="$2"

  if [[ -z ${1} || -z ${2} ]]; then
    fail "A binary name and PID file must be specified when attempting to shutdown vtadmin"
  fi
  if [[ -e "$file" ]]; then
    echo "Stopping $name..."
    local pid=$(cat "$file")
    kill $pid
    # Wait for the process to terminate
    while ps -p $pid > /dev/null; do
      sleep 1
    done
  else
    echo "Skipping stopping $name because no pid file."
  fi
}

stop_vtadmin "vtadmin-web" "$VTDATAROOT/tmp/vtadmin-web.pid"
stop_vtadmin "vtadmin-api" "$VTDATAROOT/tmp/vtadmin-api.pid"
