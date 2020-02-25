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

# This is an example script that creates a single shard vttablet deployment.

source ./env.sh

cell=${CELL:-'test'}
uid=$TABLET_UID
mysql_port=$[17000 + $uid]
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid

mkdir -p $VTDATAROOT/backups

echo "Starting MySQL for tablet $alias..."
action="init"

if [ -d $VTDATAROOT/$tablet_dir ]; then
 echo "Resuming from existing vttablet dir:"
 echo "    $VTDATAROOT/$tablet_dir"
 action='start'
fi

mysqlctl \
 -log_dir $VTDATAROOT/tmp \
 -tablet_uid $uid \
 -mysql_port $mysql_port \
 $action
