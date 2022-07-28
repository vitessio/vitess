#!/bin/bash

# Copyright 2019 The Vitess Authors.
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

usage () {
  echo "Starts a session on a sideloaded vttablet."
  echo "Note that this is a direct MySQL connection; if you actually want to work with Vitess, connect via the vtgate with:"
  echo "  mysql --port=15306 --host=127.0.0.1"
  echo
  echo "Usage: $0 <tablet alias> [<keyspace>]"
  echo "  Don't forget the 'vt_' before the keyspace!"
}

if [ $# -lt 1 ]; then
  usage
  exit -1
fi

keyspace=${2:-vt_test_keyspace}
long_alias=`printf "%010d" $1`
docker-compose exec vttablet$1 mysql -uvt_dba -S /vt/vtdataroot/vt_${long_alias}/mysql.sock $keyspace
