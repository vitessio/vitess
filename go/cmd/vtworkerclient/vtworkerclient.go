// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we do't want to depend on them at all.
var (
	actionTimeout   = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	dialTimeout     = flag.Duration("dial_timeout", 30*time.Second, "time to wait for the dial phase")
	lockWaitTimeout = flag.Duration("lock_wait_timeout", 10*time.Second, "time to wait for a topology server lock")
	server          = flag.String("server", "", "server to use for connection")
)

func main() {
	defer exit.Recover()

	flag.Parse()

	err := vtctlclient.RunCommandAndWait(
		context.Background(), *server, flag.Args(),
		*dialTimeout, *actionTimeout, *lockWaitTimeout,
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
