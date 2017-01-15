package main

// Imports and register the 'zk2' topo.Server and its Explorer.

import (
	"github.com/gitql/vitess/go/vt/servenv"
	"github.com/gitql/vitess/go/vt/topo/zk2topo"
	"github.com/gitql/vitess/go/vt/vtctld"
)

func init() {
	// Wait until flags are parsed, so we can check which topo server is in use.
	servenv.OnRun(func() {
		if s, ok := ts.Impl.(*zk2topo.Server); ok {
			vtctld.HandleExplorer("zk2", vtctld.NewBackendExplorer(s))
		}
	})
}
