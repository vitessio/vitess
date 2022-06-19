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

# Create initial cluster, add a large column to increase row size of events being streamed
source build.env

set -xe

cd "$VTROOT/examples/local"
unset VTROOT # ensure that the examples can run without VTROOT now.

source ./env.sh # Required so that "mysql" works from alias

./101_initial_cluster.sh
sleep 10 # Give vtgate time to really start.
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

#vtctlclient -server localhost:15999 ApplySchema -sql "alter table customer add inv1 int invisible default 10" commerce

#vtctlclient -server localhost:15999 ApplySchema -sql "ALTER TABLE customer ADD md5 CHAR(32) COLLATE UTF8MB4_BIN NOT NULL DEFAULT ''" commerce

vtctlclient --server localhost:15999 ApplySchema --sql "ALTER TABLE customer ADD lt longtext " commerce
mysql -h 127.0.0.1 -P 15306 commerce -e "update customer set lt = repeat('a', 10000000)"

#curl -s http://rohit-ubuntu:15001/debug/pprof/heap > ./heap.out && go tool pprof -http=:8080 ./heap.out
