// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"time"

	"github.com/youtube/vitess/go/vt/barnacle"
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/zktopo"
)

var (
	port           = flag.Int("port", 8085, "serving port")
	cell           = flag.String("cell", "test_nj", "cell to use")
	portName       = flag.String("port-name", "vt", "vt port name")
	retryDelay     = flag.Duration("retry-delay", 200*time.Millisecond, "retry delay")
	retryCount     = flag.Int("retry-count", 10, "retry count")
	tabletProtocol = flag.String("tablet-protocol", "bson", "how to talk to the vttablets")
)

func main() {
	flag.Parse()
	servenv.Init()
	barnacle.Init(*cell, *tabletProtocol, *portName, *retryDelay, *retryCount)
	servenv.Run(*port)
}
