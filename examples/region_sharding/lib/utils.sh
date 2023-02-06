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

# This file contains utility functions that can be used throughout the
# various examples.

# Wait for the given number of tablets to show up in the topology server
# for the keyspace/shard. Example (wait for 2 tablets in commerce/0):
#   wait_for_shard_tablets commerce 0 2
function wait_for_shard_tablets() {
	if [[ -z ${1} || -z ${2} || -z ${3} ]]; then
		fail "A keyspace, shard, and number of tablets must be specified when waiting for tablets to come up"
	fi
	keyspace=${1}
	shard=${2}
	num_tablets=${3}
	wait_secs=180

	for _ in $(seq 1 ${wait_secs}); do
		cur_tablets=$(vtctldclient GetTablets --keyspace "${keyspace}" --shard "${shard}" | wc -l)
		if [[ ${cur_tablets} -eq ${num_tablets} ]]; then
			break
		fi
		sleep 1
	done;

	cur_tablets=$(vtctldclient GetTablets --keyspace "${keyspace}" --shard "${shard}" | wc -l)
        if [[ ${cur_tablets} -lt ${num_tablets} ]]; then
		fail "Timed out after ${wait_secs} seconds waiting for tablets to come up in ${keyspace}/${shard}"
        fi
}

# Wait for a primary tablet to be elected and become healthy and serving
# in the given keyspace/shard. Example:
#  wait_for_healthy_shard commerce 0
function wait_for_healthy_shard_primary() {
	if [[ -z ${1} || -z ${2} ]]; then
		fail "A keyspace and shard must be specified when waiting for the shard's primary to be healthy"
	fi
	keyspace=${1}
	shard=${2}
	healthy_indicator="PRIMARY: Serving"
	wait_secs=180

	for _ in $(seq 1 ${wait_secs}); do
		if curl -s "http://$(vtctldclient GetTablets --keyspace "${keyspace}" --shard "${shard}" | grep -i 'primary' | awk '{print $5}')/debug/status_details" | grep -qi "${healthy_indicator}"; then
			break
		fi
		sleep 1
	done;

        curl -s "http://$(vtctldclient GetTablets --keyspace "${keyspace}" --shard "${shard}" | grep -i 'primary' | awk '{print $5}')/debug/status_details" | grep -qi "${healthy_indicator}" \
		|| fail "Timed out after ${wait_secs} seconds waiting for a primary tablet to be elected and become healthy in ${keyspace}/${shard}"
}

# Wait for a specified number of the keyspace/shard's tablets to show up
# in the topology server (3 is the default if no value is specified) and
# then wait for one of the tablets to be promoted to primary and become
# healthy and serving. Example:
#  wait_for_healthy_shard commerce 0
function wait_for_healthy_shard() {
	if [[ -z ${1} || -z ${2} ]]; then
		fail "A keyspace and shard must be specified when waiting for tablets to come up"
	fi
	keyspace=${1}
	shard=${2}
	num_tablets=${3:-3}

	wait_for_shard_tablets "${keyspace}" "${shard}" "${num_tablets}"
	wait_for_healthy_shard_primary "${keyspace}" "${shard}"
}

# Print error message and exit with error code.
function fail() {
	echo "ERROR: ${1}"
	exit 1
}
