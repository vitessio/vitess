// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtctl/grpcvtctlserver"
)

func init() {
	servenv.RegisterGRPCFlags()
	servenv.OnRun(func() {
		if servenv.GRPCCheckServiceMap("vtctl") {
			grpcvtctlserver.StartServer(servenv.GRPCServer, ts)
		}
	})
}
