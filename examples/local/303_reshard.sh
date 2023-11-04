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

# this script copies the data from customer/0 to customer/-80 and customer/80-
# each row will be copied to exactly one shard based on the vindex value

source ../common/env.sh

vtctldclient Reshard --workflow cust2cust --target-keyspace customer create --source-shards '0' --target-shards '-80,80-'

# Wait for the workflow to reach the running state.
wait_for_workflow_running customer cust2cust

