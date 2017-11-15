/*
Copyright 2017 Google Inc.

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
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

var (
	waitTime = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
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

	flag.Parse()
	args := flag.Args()

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	action := args[0]

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if syslogger, err := syslog.New(syslog.LOG_INFO, "vtctl "); err == nil {
		syslogger.Info(startMsg)
	} else {
		log.Warningf("cannot connect to syslog: %v", err)
	}

	servenv.FireRunHooks()

	ts := topo.Open()
	defer ts.Close()

	vtctl.WorkflowManager = workflow.NewManager(ts)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	installSignalHandlers(cancel)

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
