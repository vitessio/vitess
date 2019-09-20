/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	"golang.org/x/net/context"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/worker"
)

var (
	cell                   = flag.String("cell", "", "cell to pick servers from")
	commandDisplayInterval = flag.Duration("command_display_interval", time.Second, "Interval between each status update when vtworker is executing a single command from the command line")
	username               = flag.String("username", "", "If set, value is set as immediate caller id in the request and used by vttablet for TableACL check")
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

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

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
		ctx := context.Background()

		if *username != "" {
			ctx = callerid.NewContext(ctx,
				callerid.NewEffectiveCallerID("vtworker", "" /* component */, "" /* subComponent */),
				callerid.NewImmediateCallerID(*username))
		}

		worker, done, err := wi.RunCommand(ctx, args, nil /*custom wrangler*/, true /*runFromCli*/)
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
