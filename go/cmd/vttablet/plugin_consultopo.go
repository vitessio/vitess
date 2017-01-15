package main

// This plugin imports consultopo to register the consul implementation of TopoServer.

import (
	_ "github.com/gitql/vitess/go/vt/topo/consultopo"
)
