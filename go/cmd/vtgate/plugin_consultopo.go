package main

// This plugin imports consultopo to register the consul implementation of TopoServer.

import (
	_ "vitess.io/vitess/go/vt/topo/consultopo"
)
