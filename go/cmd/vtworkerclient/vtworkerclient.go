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
		func(e *logutil.LoggerEvent) {
			switch e.Level {
			case logutil.LOGGER_INFO:
				log.Info(e.String())
			case logutil.LOGGER_WARNING:
				log.Warning(e.String())
			case logutil.LOGGER_ERROR:
				log.Error(e.String())
			case logutil.LOGGER_CONSOLE:
				fmt.Print(e.String())
			}
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
