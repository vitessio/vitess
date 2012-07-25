#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# simple wrapper for starting up zookeeper so it detaches from the parent
# process and ingnores signals

logdir="$1"
config="$2"
pidfile="$3"

zkroot="$VTROOT/dist/vt-zookeeper-3.3.2"
classpath="$VTROOT/dist/vt-zookeeper-3.3.2/lib/zookeeper-3.3.2-fatjar.jar:$VTTOP/dist/vt-zookeeper-3.3.2/lib/"

mkdir -p "$logdir"
touch "$logdir/zksrv.log"

log() {
  now=`/bin/date`
  echo "$now $*" >> "$logdir/zksrv.log"
  return 0
}

for java in /usr/local/bin/java /usr/bin/java; do
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


cmd="$java -DZOO_LOG_DIR=$logdir -cp $classpath org.apache.zookeeper.server.quorum.QuorumPeerMain $config"

start=`/bin/date +%s`
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

