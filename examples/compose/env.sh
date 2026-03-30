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

# Utility functions for compose example scripts.

# Enable alias expansion in non-interactive shells (must be before alias definitions).
if [[ -n ${BASH} ]]; then
  shopt -s expand_aliases
fi

function fail() {
  echo "ERROR: ${1}"
  exit 1
}

alias vtctldclient="vtctldclient --server localhost:15999 --log-format text"

function wait_for_healthy_shard() {
  if [[ -z ${1} || -z ${2} ]]; then
    fail "A keyspace and shard must be specified"
  fi
  local keyspace=${1}
  local shard=${2}
  local num_tablets=${3:-3}
  local wait_secs=180

  echo "Waiting for ${num_tablets} tablets in ${keyspace}/${shard}..."
  for _ in $(seq 1 ${wait_secs}); do
    cur_tablets=$(vtctldclient GetTablets --keyspace "${keyspace}" --shard "${shard}" 2>/dev/null | wc -l)
    if [[ ${cur_tablets} -ge ${num_tablets} ]]; then
      break
    fi
    sleep 1
  done

  echo "Waiting for healthy primary in ${keyspace}/${shard}..."
  local unhealthy_indicator='"primary_alias": null'
  for _ in $(seq 1 ${wait_secs}); do
    if ! vtctldclient GetShard "${keyspace}/${shard}" 2>/dev/null | grep -qi "${unhealthy_indicator}"; then
      break
    fi
    sleep 1
  done

  if vtctldclient GetShard "${keyspace}/${shard}" 2>/dev/null | grep -qi "${unhealthy_indicator}"; then
    fail "Timed out waiting for healthy primary in ${keyspace}/${shard}"
  fi

  echo "Waiting for writable primary in ${keyspace}/${shard}..."
  local primary_tablet
  primary_tablet="$(vtctldclient GetTablets --keyspace "$keyspace" --shard "$shard" 2>/dev/null | grep -w "primary" | awk '{print $1}')"
  if [ -n "$primary_tablet" ]; then
    for _ in $(seq 1 30); do
      if vtctldclient GetFullStatus "$primary_tablet" 2>/dev/null | grep "super_read_only" | grep --quiet "false"; then
        break
      fi
      sleep 1
    done
    if vtctldclient GetFullStatus "$primary_tablet" 2>/dev/null | grep "super_read_only" | grep --quiet "true"; then
      fail "Timed out waiting for writable primary in ${keyspace}/${shard}"
    fi
  fi

  echo "Waiting for VReplication engine in ${keyspace}/${shard}..."
  for _ in $(seq 1 90); do
    if vtctldclient workflow --keyspace "${keyspace}" list &>/dev/null; then
      break
    fi
    sleep 1
  done

  if ! vtctldclient workflow --keyspace "${keyspace}" list &>/dev/null; then
    fail "Timed out waiting for VReplication engine in ${keyspace}/${shard}"
  fi

  echo "${keyspace}/${shard} is healthy!"
}

# Set aliases for direct use
alias mysql="mysql -h 127.0.0.1 -P 15306 --binary-as-hex=false"
