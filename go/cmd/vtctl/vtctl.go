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
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	waitTime         = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
	detachedMode     = flag.Bool("detach", false, "detached mode - run vtcl detached from the terminal")
	durabilityPolicy = flag.String("durability_policy", "none", "type of durability to enforce. Default is none. Other values are dictated by registered plugins")
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

	log.Warningf("WARNING: vtctl should only be used for VDiff workflows. Consider using vtctldclient for all other commands.")

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if syslogger, err := syslog.New(syslog.LOG_INFO, "vtctl "); err == nil {
		syslogger.Info(startMsg) // nolint:errcheck
	} else {
		log.Warningf("cannot connect to syslog: %v", err)
	}

	if err := reparentutil.SetDurabilityPolicy(*durabilityPolicy, nil); err != nil {
		log.Errorf("error in setting durability policy: %v", err)
		exit.Return(1)
	}

	closer := trace.StartTracing("vtctl")
	defer trace.LogErrorsWhenClosing(closer)

	servenv.FireRunHooks()

	ts := topo.Open()
	defer ts.Close()

	vtctl.WorkflowManager = workflow.NewManager(ts)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	installSignalHandlers(cancel)

	// (TODO:ajm188) <Begin backwards compatibility support>.
	//
	// For v12, we are going to support new commands by prefixing as:
	//		vtctl VtctldCommand <command> <args...>
	//
	// Existing scripts will continue to use the legacy commands. This is the
	// default case below.
	//
	// We will also support legacy commands by prefixing as:
	//		vtctl LegacyVtctlCommand <command> <args...>
	// This is the fallthrough to the default case.
	//
	// In v13, we will make the default behavior to use the new commands and
	// drop support for the `vtctl VtctldCommand ...` prefix, and legacy
	// commands will only by runnable with the `vtctl LegacyVtctlCommand ...`
	// prefix.
	//
	// In v14, we will drop support for all legacy commands, only running new
	// commands, without any prefixing required or supported.
	switch {
	case strings.EqualFold(action, "VtctldCommand"):
		// New behavior. Strip off the prefix, and set things up to run through
		// the vtctldclient command tree, using the localvtctldclient (in-process)
		// client.
		vtctld := grpcvtctldserver.NewVtctldServer(ts)
		localvtctldclient.SetServer(vtctld)
		command.VtctldClientProtocol = "local"

		os.Args = append([]string{"vtctldclient"}, args[1:]...)
		if err := command.Root.ExecuteContext(ctx); err != nil {
			log.Errorf("action failed: %v %v", action, err)
			exit.Return(255)
		}
	case strings.EqualFold(action, "LegacyVtctlCommand"):
		// Strip off the prefix (being used for compatibility) and fallthrough
		// to the legacy behavior.
		args = args[1:]
		fallthrough
	default:
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
}
