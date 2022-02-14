package main

import (
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctld"
)

func init() {
	servenv.RegisterDefaultFlags()
}

// used at runtime by plug-ins
var (
	ts *topo.Server
)

func main() {
	servenv.ParseFlags("vtctld")
	servenv.Init()
	defer servenv.Close()

	ts = topo.Open()
	defer ts.Close()

	// Init the vtctld core
	err := vtctld.InitVtctld(ts)
	if err != nil {
		exit.Return(1)
	}

	// Register http debug/health
	vtctld.RegisterDebugHealthHandler(ts)

	// Start schema manager service.
	initSchema()

	// And run the server.
	servenv.RunDefault()
}
