// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package servenv contains functionality that is common for all
// Vitess server programs.  It defines and initializes command line
// flags that control the runtime environment.
//
// After a server program has called flag.Parse, it needs to call
// env.Init to make env use the command line variables to initialize
// the environment. It also needs to call env.Close before exiting.
//
// Note: If you need to plug in any custom initialization/cleanup for
// a vitess distribution, register them using onInit and onClose. A
// clean way of achieving that is adding to this package a file with
// an init() function that registers the hooks.
package servenv

import (
	"flag"
	"net/url"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	// register the HTTP handlers for profiling
	_ "net/http/pprof"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/stats"

	// register the proper init and shutdown hooks for logging
	_ "github.com/youtube/vitess/go/vt/logutil"
)

var (
	// Port is part of the flags used when calling RegisterDefaultFlags.
	Port *int

	// Flags to alter the behavior of the library.
	lameduckPeriod = flag.Duration("lameduck-period", 50*time.Millisecond, "keep running at least this long after SIGTERM before stopping")
	onTermTimeout  = flag.Duration("onterm_timeout", 10*time.Second, "wait no more than this for OnTermSync handlers before stopping")
	memProfileRate = flag.Int("mem-profile-rate", 512*1024, "profile every n bytes allocated")

	// mutex used to protect the Init function
	mu sync.Mutex

	onInitHooks     event.Hooks
	onTermHooks     event.Hooks
	onTermSyncHooks event.Hooks
	onRunHooks      event.Hooks
	inited          bool

	// ListeningURL is filled in when calling Run, contains the server URL.
	ListeningURL url.URL
)

// Init is the first phase of the server startup.
func Init() {
	mu.Lock()
	defer mu.Unlock()
	if inited {
		log.Fatal("servenv.Init called second time")
	}
	inited = true

	// Once you run as root, you pretty much destroy the chances of a
	// non-privileged user starting the program correctly.
	if uid := os.Getuid(); uid == 0 {
		log.Fatalf("servenv.Init: running this as root makes no sense")
	}

	runtime.MemProfileRate = *memProfileRate

	// We used to set this limit directly, but you pretty much have to
	// use a root account to allow increasing a limit reliably. Dropping
	// privileges is also tricky. The best strategy is to make a shell
	// script set up the limits as root and switch users before starting
	// the server.
	fdLimit := &syscall.Rlimit{}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		log.Errorf("max-open-fds failed: %v", err)
	}
	fdl := stats.NewInt("MaxFds")
	fdl.Set(int64(fdLimit.Cur))

	onInitHooks.Fire()
}

func populateListeningURL() {
	host, err := netutil.FullyQualifiedHostname()
	if err != nil {
		host, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() failed: %v", err)
		}
	}
	ListeningURL = url.URL{
		Scheme: "http",
		Host:   netutil.JoinHostPort(host, int32(*Port)),
		Path:   "/",
	}
}

// onInit registers f to be run at the beginning of the app
// lifecycle. It should be called in an init() function.
func onInit(f func()) {
	onInitHooks.Add(f)
}

// OnTerm registers a function to be run when the process receives a SIGTERM.
// This allows the program to change its behavior during the lameduck period.
//
// All hooks are run in parallel, and there is no guarantee that the process
// will wait for them to finish before dying when the lameduck period expires.
//
// See also: OnTermSync
func OnTerm(f func()) {
	onTermHooks.Add(f)
}

// OnTermSync registers a function to be run when the process receives SIGTERM.
// This allows the program to change its behavior during the lameduck period.
//
// All hooks are run in parallel, and the process will do its best to wait
// (up to -onterm_timeout) for all of them to finish before dying.
//
// See also: OnTerm
func OnTermSync(f func()) {
	onTermSyncHooks.Add(f)
}

// fireOnTermSyncHooks returns true iff all the hooks finish before the timeout.
func fireOnTermSyncHooks(timeout time.Duration) bool {
	log.Infof("Firing synchronous OnTermSync hooks and waiting up to %v for them", timeout)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	go func() {
		onTermSyncHooks.Fire()
		close(done)
	}()

	select {
	case <-done:
		log.Infof("OnTermSync hooks finished")
		return true
	case <-timer.C:
		log.Infof("OnTermSync hooks timed out")
		return false
	}
}

// OnRun registers f to be run right at the beginning of Run. All
// hooks are run in parallel.
func OnRun(f func()) {
	onRunHooks.Add(f)
}

// FireRunHooks fires the hooks registered by OnHook.
// Use this in a non-server to run the hooks registered
// by servenv.OnRun().
func FireRunHooks() {
	onRunHooks.Fire()
}

// RegisterDefaultFlags registers the default flags for
// listening to a given port for standard connections.
// If calling this, then call RunDefault()
func RegisterDefaultFlags() {
	Port = flag.Int("port", 0, "port for the server")
}

// RunDefault calls Run() with the parameters from the flags.
func RunDefault() {
	Run(*Port)
}
