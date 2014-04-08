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
	"io"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	_ "github.com/youtube/vitess/go/vt/logutil"
	_ "net/http/pprof"
)

var (
	memProfileRate = flag.Int("mem-profile-rate", 512*1024, "profile every n bytes allocated")
	mu             sync.Mutex

	onInitHooks hooks
	onTermHooks hooks
	onRunHooks  hooks
	inited      bool
)

type hooks struct {
	funcs []func()
	mu    sync.Mutex
}

func (h *hooks) Add(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.funcs = append(h.funcs, f)
}

func (h *hooks) Fire() {
	wg := sync.WaitGroup{}

	for _, f := range h.funcs {
		wg.Add(1)
		go func(f func()) {
			f()
			wg.Done()
		}(f)
	}
	wg.Wait()
}

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

// onInit registers f to be run at the beginning of the app
// lifecycle. It should be called in an init() function.
func onInit(f func()) {
	onInitHooks.Add(f)
}

// OnRun registers f to be run right at the beginning of Run. All
// hooks are run in parallel.
func OnRun(f func()) {
	onRunHooks.Add(f)
}
