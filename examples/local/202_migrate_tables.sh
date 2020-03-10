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

# this script copies over all the data from commerce keyspace to
# customer keyspace for the customer and corder tables

source ./env.sh

vtctlclient \
    -server localhost:15999 \
    -log_dir "$VTDATAROOT"/tmp \
    -alsologtostderr \
    Migrate \
    -workflow=commerce2customer \
    commerce customer customer,corder

sleep 2
