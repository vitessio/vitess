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

package main

import (
	"context"
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/cmd"
	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	waitTime     = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
	detachedMode = flag.Bool("detach", false, "detached mode - run vtcl detached from the terminal")
)

func init() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	flag.Usage = func() {
		logger.Printf("Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		logger.Printf("\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		logger.Printf("\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		vtctl.PrintAllCommands(logger)
	}
}

// signal handling, centralized here
func installSignalHandlers(cancel func()) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		// we got a signal, cancel the current ctx
		cancel()
	}()
}

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	if *detachedMode {
		// this method will call os.Exit and kill this process
		cmd.DetachFromTerminalAndExit()
	}

	args := servenv.ParseFlagsWithArgs("vtctl")
	action := args[0]

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if syslogger, err := syslog.New(syslog.LOG_INFO, "vtctl "); err == nil {
		syslogger.Info(startMsg) // nolint:errcheck
	} else {
		log.Warningf("cannot connect to syslog: %v", err)
	}

	closer := trace.StartTracing("vtctl")
	defer trace.LogErrorsWhenClosing(closer)

	servenv.FireRunHooks()

	ts := topo.Open()
	defer ts.Close()

	vtctl.WorkflowManager = workflow.NewManager(ts)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	installSignalHandlers(cancel)

	if strings.EqualFold(action, "LegacyVtctlCommand") {
		args = args[1:]
		if args[0] == "--" {
			args = args[1:]
		}

		action = args[0]
		wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
		err := vtctl.RunCommand(ctx, wr, args)
		cancel()
		switch err {
		case vtctl.ErrUnknownCommand:
			flag.Usage()
			exit.Return(1)
		case nil:
			// keep going
		default:
			log.Errorf("action failed: %v %v", action, err)
			exit.Return(255)
		}
	}

	vtctld := grpcvtctldserver.NewVtctldServer(ts)
	localvtctldclient.SetServer(vtctld)
	command.VtctldClientProtocol = "local"
	for i, arg := range args {
		if strings.HasPrefix(arg, "-") {
			if len(arg) == 2 {
				continue
			}

			if strings.HasPrefix(arg, "--") {
				continue
			}

			log.Warningf("Long arg %s begins with a single dash; this will not work in future versions.", arg)
			dashed := strings.ReplaceAll(arg, "_", "-")
			if arg != dashed {
				log.Warningf("Long arg %s is using underscore word separators, switching to dashes. This will not work in future versions.", arg)
				arg = dashed
			}

			arg = "-" + arg
			args[i] = arg
		}
	}

	os.Args = append([]string{"vtctldclient"}, args...)
	if err := command.Root.ExecuteContext(ctx); err != nil {
		log.Errorf("action failed: %v %v", action, err)
		exit.Return(255)
	}
}
