// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
vtworker is the main program to run a worker job.

It has two modes: single command or interactive.
- in single command, it will start the job passed in from the command line,
  and exit.
- in interactive mode, use a web browser to start an action.
*/
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	cell = flag.String("cell", "", "cell to pick servers from")
	commandDisplayInterval = flag.Duration("command_display_interval", time.Second, "Interval between each status update when vtworker is executing a single command from the command line")
)

func init() {
	servenv.RegisterDefaultFlags()
}

var (
	wi *worker.WorkerInstance
)

func main() {
	defer exit.Recover()

	setUsage()
	flag.Parse()
	args := flag.Args()

	servenv.Init()
	defer servenv.Close()

	ts := topo.GetServer()
	defer topo.CloseServers()

	wi = worker.NewWorkerInstance(ts, *cell, 30*time.Second, *commandDisplayInterval)

	// The logger will be replaced when we start a job.
	wi.Wr = wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), wi.LockTimeout)
	if len(args) == 0 {
		// In interactive mode, initialize the web UI to choose a command.
		wi.InitInteractiveMode()
	} else {
		// In single command mode, just run it.
		if err := wi.RunCommand(args, wi.Wr); err != nil {
			log.Error(err)
			exit.Return(1)
		}
	}
	wi.InstallSignalHandlers()
	wi.InitStatusHandling()

	servenv.RunDefault()
}

func setUsage() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		for _, group := range worker.Commands {
			if group.Name == "Debugging" {
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: %v\n", group.Name, group.Description)
			for _, cmd := range group.Commands {
				fmt.Fprintf(os.Stderr, "  %v %v\n", cmd.Name, cmd.Params)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
}
