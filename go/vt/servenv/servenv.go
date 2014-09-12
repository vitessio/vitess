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
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/stats"
	_ "github.com/youtube/vitess/go/vt/logutil"
)

var (
	// The flags used when calling RegisterDefaultFlags.
	Port *int

	// Flags to alter the behavior of the library.
	lameduckPeriod = flag.Duration("lameduck-period", 50*time.Millisecond, "how long to keep the server running on SIGTERM before stopping")
	memProfileRate = flag.Int("mem-profile-rate", 512*1024, "profile every n bytes allocated")

	// mutex used to protect the Init function
	mu sync.Mutex

	onInitHooks event.Hooks
	onTermHooks event.Hooks
	onRunHooks  event.Hooks
	inited      bool

	// filled in when calling Run
	ListeningURL url.URL
)

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
	gomaxprocs := os.Getenv("GOMAXPROCS")
	if gomaxprocs == "" {
		gomaxprocs = "1"
	}

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

	if err := exportBinaryVersion(); err != nil {
		log.Fatalf("servenv.Init: exportBinaryVersion: %v", err)
	}

	onInitHooks.Fire()
}

func exportBinaryVersion() error {
	hasher := md5.New()
	exeFile, err := os.Open("/proc/self/exe")
	if err != nil {
		return err
	}
	if _, err = io.Copy(hasher, exeFile); err != nil {
		return err
	}
	md5sum := hex.EncodeToString(hasher.Sum(nil))
	fileInfo, err := exeFile.Stat()
	if err != nil {
		return err
	}
	mtime := fileInfo.ModTime().Format(time.RFC3339)
	version := mtime + " " + md5sum
	stats.NewString("BinaryVersion").Set(version)
	return nil
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
		Host:   fmt.Sprintf("%v:%v", host, *Port),
		Path:   "/",
	}
}

// onInit registers f to be run at the beginning of the app
// lifecycle. It should be called in an init() function.
func onInit(f func()) {
	onInitHooks.Add(f)
}

// OnTerm registers f to be run when the process receives a SIGTERM.
// All hooks are run in parallel.
// This allows the program to change its behavior during the lameduck period.
func OnTerm(f func()) {
	onTermHooks.Add(f)
}

// OnRun registers f to be run right at the beginning of Run. All
// hooks are run in parallel.
func OnRun(f func()) {
	onRunHooks.Add(f)
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
