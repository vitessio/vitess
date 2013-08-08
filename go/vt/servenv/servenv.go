// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package env defines and initializes command line flags that control
// the runtime environment.
//
// After a server program has called flag.Parse, it needs to call
// env.Init to make env use the command line variables to initialize
// the environment. It also needs to call env.Close when exiting.
//
// Note: If you need to plug in any custom initialization/cleanup for
// a vitess distribution, register them using onInit and onClose. A
// clean way of achieving that is adding to this package a file with
// an init() function that registers the hooks.

package servenv

import (
	"crypto/md5"
	"encoding/hex"
	"expvar"
	"flag"
	"io"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	log "github.com/golang/glog"
	_ "github.com/youtube/vitess/go/vt/logutil"
)

var (
	memProfileRate = flag.Int("mem-profile-rate", 512*1024, "profile every n bytes allocated")
	mu             sync.Mutex
	onCloseHooks   []func()
	onInitHooks    []func()
	inited         bool
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
	// FIXME(msolomon) Can't hijack with a logfile because the file descriptor
	// changes after every rotation. Might need to make the logfile more posix
	// friendly.
	//relog.HijackStdio(f, f)
	runtime.MemProfileRate = *memProfileRate
	gomaxprocs := os.Getenv("GOMAXPROCS")
	if gomaxprocs == "" {
		gomaxprocs = "1"
	}
	// Could report this in an expvar instead.
	log.Infof("GOMAXPROCS = %v", gomaxprocs)

	// We used to set this limit directly, but you pretty much have to
	// use a root account to allow increasing a limit reliably. Dropping
	// privileges is also tricky. The best strategy is to make a shell
	// script set up the limits as root and switch users before starting
	// the server.
	fdLimit := &syscall.Rlimit{}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		log.Errorf("max-open-fds failed: %v", err)
	} else {
		// Could report this in an expvar instead.
		log.Infof("max-open-fds: %v", fdLimit.Cur)
	}

	if err := exportBinaryVersion(); err != nil {
		log.Fatalf("servenv.Init: exportBinaryVersion: %v", err)
	}

	for _, f := range onInitHooks {
		f()
	}
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
	expvar.NewString("binary-version").Set(version)
	// rexport this value for varz scraper
	expvar.NewString("Version").Set(version)
	return nil
}

// Close runs any registered exit hooks and exits the program.
func Close() {
	mu.Lock()
	defer mu.Unlock()

	for _, f := range onCloseHooks {
		f()
	}
	log.Fatal("done")
}

// onInit registers f to be run at the beginning of the app
// lifecycle. It should be called in an init() function.
func onInit(f func()) {
	mu.Lock()
	defer mu.Unlock()
	onInitHooks = append(onInitHooks, f)
}

// onClose registers f to be run at the end of the app lifecycle. It
// should be called in an init() function.
func onClose(f func()) {
	mu.Lock()
	defer mu.Unlock()
	onCloseHooks = append(onCloseHooks, f)
}
