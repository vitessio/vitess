// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we don't want to depend on them at all.
var (
	actionTimeout = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	server        = flag.String("server", "", "server to use for connection")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *actionTimeout)
	defer cancel()

	logger := logutil.NewConsoleLogger()

	err := vtworkerclient.RunCommandAndWait(
		ctx, *server, flag.Args(),
		func(e *logutilpb.Event) {
			logutil.LogEvent(logger, e)
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
