// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Fake ZK based TopologyServer from a config file

import (
	"flag"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk/fakezk"
)

var fakezkConfig = flag.String("fakezk-config", "", "If set, will read the json config file, use it to seed a fakezk and register it as 'fakezk' topology implementation")

func init() {
	if *fakezkConfig != "" {
		topo.RegisterServer("fakezk", zktopo.NewServer(fakezk.NewConnFromFile(*fakezkConfig)))
	}
}
