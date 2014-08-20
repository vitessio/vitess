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
	"sort"
	"strings"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/initiator"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	noWaitForAction = flag.Bool("no-wait", false, "don't wait for action completion, detach")
	waitTime        = flag.Duration("wait-time", 24*time.Hour, "time to wait on an action")
	lockWaitTimeout = flag.Duration("lock-wait-timeout", 0, "time to wait for a lock before starting an action")
)

func init() {
	// FIXME(msolomon) need to send all of this to stdout
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		vtctl.PrintHelp()
	}
}

// signal handling, centralized here
func installSignalHandlers() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		// we got a signal, notify our modules:
		// - initiator will interrupt anything waiting on a tablet action
		// - wrangler will interrupt anything waiting on a shard or
		//   keyspace lock
		initiator.SignalInterrupt()
		wrangler.SignalInterrupt()
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
	installSignalHandlers()

	startMsg := fmt.Sprintf("USER=%v SUDO_USER=%v %v", os.Getenv("USER"), os.Getenv("SUDO_USER"), strings.Join(os.Args, " "))

	if syslogger, err := syslog.New(syslog.LOG_INFO, "vtctl "); err == nil {
		syslogger.Info(startMsg)
	} else {
		log.Warningf("cannot connect to syslog: %v", err)
	}

	topoServer := topo.GetServer()
	defer topo.CloseServers()

	wr := wrangler.New(logutil.NewConsoleLogger(), topoServer, *waitTime, *lockWaitTimeout)

	actionPath, err := vtctl.RunCommand(wr, args)
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

	if actionPath != "" {
		if *noWaitForAction {
			fmt.Println(actionPath)
		} else {
			err := wr.ActionInitiator().WaitForCompletion(actionPath, *waitTime)
			if err != nil {
				log.Error(err.Error())
				exit.Return(255)
			} else {
				log.Infof("action completed: %v", actionPath)
			}
		}
	}
}

type rTablet struct {
	*topo.TabletInfo
	*myproto.ReplicationPosition
}

type rTablets []*rTablet

func (rts rTablets) Len() int { return len(rts) }

func (rts rTablets) Swap(i, j int) { rts[i], rts[j] = rts[j], rts[i] }

// Sort for tablet replication.
// master first, then i/o position, then sql position
func (rts rTablets) Less(i, j int) bool {
	// NOTE: Swap order of unpack to reverse sort
	l, r := rts[j], rts[i]
	// l or r ReplicationPosition would be nil if we failed to get
	// the position (put them at the beginning of the list)
	if l.ReplicationPosition == nil {
		return r.ReplicationPosition != nil
	}
	if r.ReplicationPosition == nil {
		return false
	}
	var lTypeMaster, rTypeMaster int
	if l.Type == topo.TYPE_MASTER {
		lTypeMaster = 1
	}
	if r.Type == topo.TYPE_MASTER {
		rTypeMaster = 1
	}
	if lTypeMaster < rTypeMaster {
		return true
	}
	if lTypeMaster == rTypeMaster {
		if l.MapKeyIo() < r.MapKeyIo() {
			return true
		}
		if l.MapKeyIo() == r.MapKeyIo() {
			if l.MapKey() < r.MapKey() {
				return true
			}
		}
	}
	return false
}

func sortReplicatingTablets(tablets []*topo.TabletInfo, positions []*myproto.ReplicationPosition) []*rTablet {
	rtablets := make([]*rTablet, len(tablets))
	for i, pos := range positions {
		rtablets[i] = &rTablet{tablets[i], pos}
	}
	sort.Sort(rTablets(rtablets))
	return rtablets
}
