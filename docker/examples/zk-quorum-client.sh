#!/bin/bash

# This example script shows how to use the zk command to inspect and modify
# the contents of the ZooKeeper quorum started by zk-quorum.sh.

# Run zk-quorum-client.sh with no arguments to see a list of commands available.
# For example, to create a path: zk-quorum-client.sh touch -p /zk/some/path

zk -zk.addrs "$HOSTNAME:21811,$HOSTNAME:21812,$HOSTNAME:21813" $*
