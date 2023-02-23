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

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"
cell=${CELL:-'test'}
uid=$TABLET_UID
mysql_port=$[17000 + $uid]
hostname="172.17.0."$uid
cur_path=$(dirname $(readlink -f "$0"))
port=$[17000 + $uid]
idx=$[$uid - 1]
printf -v alias '%s-%010d' $cell $uidls
printf -v tablet_dir 'vt_%010d' $uid

mkdir -p $VTDATAROOT/backups

echo "Starting MySQL for tablet $alias..."
action="init"

if [ -d $VTDATAROOT/$tablet_dir ]; then
 echo "Resuming from existing vttablet dir:"
 echo "    $VTDATAROOT/$tablet_dir"
 action='start'
fi

# mysqlctl \
#  --log_dir $VTDATAROOT/tmp \
#  --tablet_uid $uid \
#  --mysql_port $mysql_port \

# mysqld \
#      --enable-consensus=ON \
#      --server-id=$uid \
#      --defaults-file=$VTDATAROOT/$tablet_dir/my.cnf \
#      --basedir=/opt/homebrew \
#      --datadir=$VTDATAROOT/$tablet_dir/data \
#      --plugin-dir=/opt/homebrew/lib/plugin \
#      --log-error=$VTDATAROOT/$tablet_dir/error.log \
#      --pid-file=$VTDATAROOT/$tablet_dir/mysql.pid \
#      --socket=$VTDATAROOT/$tablet_dir/mysql.sock \
#      --port=$mysql_port  \
#      --cluster-info='$hostname:13306;$hostname:13307;$hostname:13308@$idx'
# 
docker run -itd  \
    --name mysql-server$idx \
    --ip $hostname \
    -p $port:3306     \
    -v ${cur_path}/../../../config/apecloud_mycnf:/etc/mysql/conf.d \
    -v ${cur_path}/../../../config/apecloud_scripts:/docker-entrypoint-initdb.d/    \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
    -e MYSQL_INIT_CONSENSUS_PORT=13306 \
    -v $VTDATAROOT/$tablet_dir:/mysql \
    -e CLUSTER_ID=1 \
    -e CLUSTER_INFO='172.17.0.2:13306;172.17.0.3:13306;172.17.0.4:13306@'$idx \
    apecloud/apecloud-mysql-server:8.0.30-5.alpha2.20230105.gd6b8719.2
