package vtctl

import (
	// This plugin imports zk2topo to register the zk2 implementation of TopoServer.
	_ "github.com/gitql/vitess/go/vt/topo/zk2topo"
)
