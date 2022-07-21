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

# this script migrates traffic for the primary tablet

source ./env.sh

vtctlclient Reshard -- --tablet_types=primary SwitchTraffic main.main2regions

# to go back to unsharded
# call SwitchReads and SwitchWrites with workflow main.main2regions_reverse
# delete vreplication rows from sharded tablets
# drop all the tables
# change vschema back to unsharded
# drop lookup table

