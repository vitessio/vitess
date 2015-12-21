// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
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

	err := vtworkerclient.RunCommandAndWait(
		ctx, *server, flag.Args(),
		func(e *logutilpb.Event) {
			switch e.Level {
			case logutilpb.Level_INFO:
				log.Info(logutil.EventString(e))
			case logutilpb.Level_WARNING:
				log.Warning(logutil.EventString(e))
			case logutilpb.Level_ERROR:
				log.Error(logutil.EventString(e))
			case logutilpb.Level_CONSOLE:
				fmt.Print(logutil.EventString(e))
			}
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
