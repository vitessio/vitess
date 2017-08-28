/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"os"

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

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	ts = topo.Open()
	defer ts.Close()

	// Init the vtctld core
	vtctld.InitVtctld(ts)

	// Register http debug/health
	vtctld.RegisterDebugHealthHandler(ts)

	// Start schema manager service.
	initSchema()

	// And run the server.
	servenv.RunDefault()
}
