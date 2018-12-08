#!/bin/bash

# Copyright 2018 The Vitess Authors.
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

# this script copies over all the data from commerce keyspace to
# customer keyspace for the customer and corder tables

set -e

# shellcheck disable=SC2128
script_root=$(dirname "${BASH_SOURCE}")

# shellcheck source=./env.sh
# shellcheck disable=SC1091
source "$script_root/env.sh"

# shellcheck disable=SC2086
"$VTROOT"/bin/vtworker \
    $TOPOLOGY_FLAGS \
    -cell zone1 \
    -log_dir "$VTDATAROOT"/tmp \
    -alsologtostderr \
    -use_v3_resharding_mode \
    VerticalSplitClone -min_healthy_rdonly_tablets=1 -tables=customer,corder customer/0

disown -a
