// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
