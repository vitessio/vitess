package vtctl

import (
	// Imports k8stopo to register the kubernetes implementation of
	// TopoServer.
	_ "vitess.io/vitess/go/vt/topo/k8stopo"
)
