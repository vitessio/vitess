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

# this script brings down zookeeper and all the vitess components
# we brought up in the example
# optionally, you may want to delete everything that was created
# by the example from $VTDATAROOT

set -e

# shellcheck disable=SC2128
script_root=$(dirname "${BASH_SOURCE}")

./vtgate-down.sh
CELL=zone1 UID_BASE=100 "$script_root/vttablet-down.sh"
CELL=zone1 UID_BASE=300 "$script_root/vttablet-down.sh"
CELL=zone1 UID_BASE=400 "$script_root/vttablet-down.sh"
./vtctld-down.sh

if [ "${TOPO}" = "zk2" ]; then
    CELL=zone1 "$script_root/zk-down.sh"
else
    CELL=zone1 "$script_root/etcd-down.sh"
fi

rm -r $VTDATAROOT/*

disown -a
