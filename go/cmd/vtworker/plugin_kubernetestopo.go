package main

// This plugin imports k8stopo to register the kubernetes implementation of TopoServer.

import (
	_ "vitess.io/vitess/go/vt/topo/k8stopo"
)
