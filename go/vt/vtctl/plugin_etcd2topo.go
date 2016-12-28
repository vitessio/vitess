package vtctl

import (
	// Imports etcd2topo to register the etcd2 implementation of
	// TopoServer.
	_ "github.com/youtube/vitess/go/vt/topo/etcd2topo"
)
