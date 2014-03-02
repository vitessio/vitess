// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"time"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	_ "github.com/youtube/vitess/go/vt/zktopo"
)

var (
	port       = flag.Int("port", 8085, "serving port")
	cell       = flag.String("cell", "test_nj", "cell to use")
	retryDelay = flag.Duration("retry-delay", 200*time.Millisecond, "retry delay")
	retryCount = flag.Int("retry-count", 10, "retry count")
	timeout    = flag.Duration("timeout", 5*time.Second, "connection and call timeout")

	securePort = flag.Int("secure-port", 0, "port for the secure server")
	cert       = flag.String("cert", "", "cert file")
	key        = flag.String("key", "", "key file")
	caCert     = flag.String("ca-cert", "", "ca-cert file")
)

var topoReader *TopoReader

func main() {
	flag.Parse()
	servenv.Init()

	// For the initial phase vtgate is exposing
	// topoReader api. This will be subsumed by
	// vtgate once vtgate's client functions become active.
	ts := topo.GetServer()
	defer topo.CloseServers()

	rts := vtgate.NewResilientSrvTopoServer(ts)

	topoReader = NewTopoReader(rts)
	topo.RegisterTopoReader(topoReader)

	vtgate.Init(rts, *cell, *retryDelay, *retryCount, *timeout)
	servenv.RunSecure(*port, *securePort, *cert, *key, *caCert)
}
