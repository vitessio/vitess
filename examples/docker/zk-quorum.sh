#!/bin/bash

# This example script creates a quorum of ZooKeeper servers on a single host
# with the vitess/zkctl Docker image. In a real deployment, these would be
# on different hosts. See the ZooKeeper guide for details:
# http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html

# List of all servers in the quorum.
zkcfg=(\
    "1@$HOSTNAME:28881:38881:21811" \
    "2@$HOSTNAME:28882:38882:21812" \
    "3@$HOSTNAME:28883:38883:21813" \
    )
printf -v zkcfg ",%s" "${zkcfg[@]}"
zkcfg=${zkcfg:1}

for myid in 1 2 3; do
  # Port mappings host:container. We append myid only because we're launching
  # multiple servers on the same host as an example.
  ports=(\
      # leader port
      2888$myid:2888$myid \
      # election port
      3888$myid:3888$myid \
      # client port
      2181$myid:2181$myid \
      )
  printf -v ports -- "-p %s " "${ports[@]}"

  # Docker volumes
  vols=(\
      # Redirect syslog to host machine.
      /dev/log:/dev/log \
      # Export vtdataroot so it can be examined from outside.
      /vt/vtdataroot \
      )
  printf -v vols -- "-v %s " "${vols[@]}"

  echo -n "zk$myid: "
  docker run -d --name zk$myid $vols $ports vitess/zkctl -zk.myid $myid -zk.cfg $zkcfg init
done
