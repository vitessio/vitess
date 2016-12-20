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
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/worker"
	"golang.org/x/net/context"
)

var (
	cell                   = flag.String("cell", "", "cell to pick servers from")
	commandDisplayInterval = flag.Duration("command_display_interval", time.Second, "Interval between each status update when vtworker is executing a single command from the command line")
)

func init() {
	servenv.RegisterDefaultFlags()

	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	flag.Usage = func() {
		logger.Printf("Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		logger.Printf("\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		logger.Printf("\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		worker.PrintAllCommands(logger)
	}
}

var (
	wi *worker.Instance
)

func main() {
	defer exit.Recover()

	flag.Parse()
	args := flag.Args()

	servenv.Init()
	defer servenv.Close()

	ts := topo.Open()
	defer ts.Close()

	wi = worker.NewInstance(ts, *cell, *commandDisplayInterval)
	wi.InstallSignalHandlers()
	wi.InitStatusHandling()

	if len(args) == 0 {
		// In interactive mode, initialize the web UI to choose a command.
		wi.InitInteractiveMode()
	} else {
		// In single command mode, just run it.
		worker, done, err := wi.RunCommand(context.Background(), args, nil /*custom wrangler*/, true /*runFromCli*/)
		if err != nil {
			log.Error(err)
			exit.Return(1)
		}
		// Run the subsequent, blocking wait asynchronously.
		go func() {
			if err := wi.WaitForCommand(worker, done); err != nil {
				log.Error(err)
				logutil.Flush()
				// We cannot use exit.Return() here because we are in a different go routine now.
				os.Exit(1)
			}
			logutil.Flush()
			os.Exit(0)
		}()
	}

	servenv.RunDefault()
}
