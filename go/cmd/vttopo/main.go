// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
  toporeader is a plain non-caching interface to the topo.Server API,
  to discover the serving graph. It only supports the topo.TopoReader API.
*/
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
)

var usage = `Allows an RPC access to the topo.Server for read-only access to the topology.`

var (
	port = flag.Int("port", 14850, "port for the server")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
}

func main() {
	flag.Parse()
	servenv.Init()

	topoServer := topo.GetServer()
	defer topo.CloseServers()

	topo.RegisterTopoReader(&TopoReader{ts: topoServer})
	servenv.Run(*port)
}
