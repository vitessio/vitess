package main

// This plugin imports etcd2topo to register the etcd2 implementation of TopoServer.

import (
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
)
