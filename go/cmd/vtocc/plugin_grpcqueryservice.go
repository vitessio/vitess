// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the gRPC queryservice server

import (
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
)

func init() {
	servenv.RegisterGRPCFlags()
}
