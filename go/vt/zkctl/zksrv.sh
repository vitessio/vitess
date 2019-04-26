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

# simple wrapper for starting up zookeeper so it detaches from the parent
# process and ingnores signals

logdir="$1"
config="$2"
pidfile="$3"
zk_java_opts=${ZK_JAVA_OPTS:-}
zk_ver=${ZK_VERSION:-3.4.14}
classpath="$VTROOT/dist/vt-zookeeper-$zk_ver/lib/zookeeper-$zk_ver-fatjar.jar:/usr/local/lib/zookeeper-$zk_ver-fatjar.jar:/usr/share/java/zookeeper-$zk_ver.jar"

mkdir -p "$logdir"
touch "$logdir/zksrv.log"

log() {
  now=`/bin/date`
  echo "$now $*" >> "$logdir/zksrv.log"
  return 0
}

for java in /usr/local/bin/java /usr/bin/java $JAVA_HOME/bin/java; do
  if [ -x "$java" ]; then
    break
  fi
done

if [ ! -x "$java" ]; then
  log "ERROR no java binary found"
  exit 1
fi

if [ "$VTDEV" ]; then
  # use less memory
  java="$java -client -Xincgc -Xms1m -Xmx32m"
else
  # enable hotspot
  java="$java -server"
fi


cmd="$java -DZOO_LOG_DIR=$logdir $zk_java_opts -cp $classpath org.apache.zookeeper.server.quorum.QuorumPeerMain $config"

log "INFO starting $cmd"
$cmd < /dev/null &> /dev/null &
pid=$!

log "INFO pid: $pid pidfile: $pidfile"
if [ "$pidfile" ]; then
  if [ -f "$pidfile" ]; then
    rm "$pidfile"
  fi
  echo "$pid" > "$pidfile"
fi

wait $pid
log "INFO exit status $pid: $exit_status"

