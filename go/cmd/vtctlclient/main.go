// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we do't want to depend on them at all.
var (
	actionTimeout = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	dialTimeout   = flag.Duration("dial_timeout", 30*time.Second, "time to wait for the dial phase")
	server        = flag.String("server", "", "server to use for connection")
)

func main() {
	defer exit.Recover()

	flag.Parse()

	logger := logutil.NewConsoleLogger()

	err := vtctlclient.RunCommandAndWait(
		context.Background(), *server, flag.Args(),
		*dialTimeout, *actionTimeout,
		func(e *logutilpb.Event) {
			logutil.LogEvent(logger, e)
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
