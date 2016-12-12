package main

import (
	"flag"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctld"
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

	ts = topo.Open()
	defer ts.Close()

	// Init the vtctld core
	vtctld.InitVtctld(ts)

	// Start schema manager service.
	initSchema()

	// And run the server.
	servenv.RunDefault()
}
