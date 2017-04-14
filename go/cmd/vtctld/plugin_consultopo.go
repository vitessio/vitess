package main

// Imports and register the 'consul' topo.Server and its Explorer.

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo/consultopo"
	"github.com/youtube/vitess/go/vt/vtctld"
)

func init() {
	// Wait until flags are parsed, so we can check which topo server is in use.
	servenv.OnRun(func() {
		if s, ok := ts.Impl.(*consultopo.Server); ok {
			vtctld.HandleExplorer("consul", vtctld.NewBackendExplorer(s))
		}
	})
}
