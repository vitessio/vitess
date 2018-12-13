#!/bin/bash

# Copyright 2017 Google Inc.
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

# This is an example script that stops the mysqld and vttablet instances
# created by vttablet-up.sh

cell=${CELL:-'test'}
uid_base=${UID_BASE:-'100'}

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Stop 3 vttablets by default.
# Pass a list of UID indices on the command line to override.
uids=${@:-'0 1 2'}

wait_pids=''

for uid_index in $uids; do
  uid=$[$uid_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  printf -v tablet_dir 'vt_%010d' $uid

  echo "Stopping vttablet for $alias..."
  pid=`cat $VTDATAROOT/$tablet_dir/vttablet.pid`
  kill $pid
  wait_pids="$wait_pids $pid"

  echo "Stopping MySQL for tablet $alias..."
  $VTROOT/bin/mysqlctl \
    -tablet_uid $uid \
    shutdown &
done

# Wait for vttablets to die.
while ps -p $wait_pids > /dev/null; do sleep 1; done

# Wait for 'mysqlctl shutdown' commands to finish.
wait

