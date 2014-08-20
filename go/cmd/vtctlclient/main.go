// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
)

var (
	actionTimeout   = flag.Duration("action_timeout", 0, "timeout for the total command")
	dialTimeout     = flag.Duration("dial_timeout", 0, "time to wait for the dial phase")
	lockWaitTimeout = flag.Duration("lock_wait_timeout", 0, "time to wait for a lock before starting an action")
	server          = flag.String("server", "", "server to use for connection")
)

func main() {
	flag.Parse()

	// create the client
	client, err := vtctlclient.New(*server, *dialTimeout)
	if err != nil {
		log.Fatalf("Cannot dial to server %v: %v", *server, err)
	}

	// run the command
	c, errFunc := client.ExecuteVtctlCommand(flag.Args(), *actionTimeout, *lockWaitTimeout)
	if err = errFunc(); err != nil {
		log.Fatalf("Cannot execute remote command: %v", err)
	}

	// stream the result
	for e := range c {
		switch e.Level {
		case logutil.LOGGER_INFO:
			log.Info(e.String())
		case logutil.LOGGER_WARNING:
			log.Warning(e.String())
		case logutil.LOGGER_ERROR:
			log.Error(e.String())
		case logutil.LOGGER_CONSOLE:
			fmt.Println(e.Value)
		}
	}

	// then display the overall error
	if err = errFunc(); err != nil {
		log.Fatalf("Remote error: %v", err)
	}
}
