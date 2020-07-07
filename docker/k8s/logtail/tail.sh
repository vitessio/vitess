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

set -ex

# SIGTERM-handler
term_handler() {
  # block shutting down log tailers until mysql shuts down first
  until [ "$MYSQL_GONE" ]; do
    sleep 5

    # poll every 5 seconds to see if mysql is still running
    if ! mysqladmin ping -uroot --socket=/vtdataroot/tabletdata/mysql.sock; then
      MYSQL_GONE=true
    fi
  done
  
  exit;
}

# setup handlers
# on callback, kill the last background process and execute the specified handler
trap 'kill ${!}; term_handler' SIGINT SIGTERM SIGHUP

# wait forever
while true
do
  tail -n+1 -F "$TAIL_FILEPATH" & wait ${!}
done