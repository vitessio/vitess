package main

import (
	"flag"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
)

func init() {
	servenv.RegisterDefaultFlags()
}

// used at runtime by plug-ins
var (
	ts topo.Server
)

func main() {
	flag.Parse()
	servenv.Init()
	defer servenv.Close()

	ts = topo.GetServer()
	defer topo.CloseServers()

	// Init the vtctld core
	InitVtctld(ts)

	// Start schema manager service.
	initSchema()

	// And run the server.
	servenv.RunDefault()
}
