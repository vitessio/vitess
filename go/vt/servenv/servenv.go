// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	Package env defines and initializes command line flags that control
	the runtime environment.
	After the main program has called flag.Parse, it needs to call env.Init
	to make env use the command line variables to initialize the environment.
*/
package env

import (
	"code.google.com/p/vitess/go/logfile"
	"code.google.com/p/vitess/go/relog"
	"crypto/md5"
	"encoding/hex"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"syscall"
	"time"
)

var (
	logfileName  = flag.String("logfile", "/dev/stderr", "base log file name")
	logFrequency = flag.Int64("logfile.frequency", 0,
		"rotation frequency in seconds")
	logMaxSize  = flag.Int64("logfile.maxsize", 0, "max file size in bytes")
	logMaxFiles = flag.Int64("logfile.maxfiles", 0, "max number of log files")

	memProfileRate = flag.Int("mem-profile-rate", 512*1024,
		"profile every n bytes allocated")
	maxOpenFds = flag.Uint64("max-open-fds", 32768, "max open file descriptors")
	gomaxprocs = flag.Int("gomaxprocs", 0, "Sets GOMAXPROCS")
)

func Init(logPrefix string) {
	if logPrefix != "" {
		logPrefix += " "
	}
	logPrefix += fmt.Sprintf("[%v]", os.Getpid())
	f, err := logfile.Open(*logfileName, *logFrequency, *logMaxSize, *logMaxFiles)
	if err != nil {
		panic(fmt.Sprintf("unable to open logfile %s: %v", *logfileName, err))
	}
	logger := relog.New(f, logPrefix + " ",
		log.Ldate|log.Lmicroseconds|log.Lshortfile, relog.DEBUG)
	relog.SetLogger(logger)

	runtime.MemProfileRate = *memProfileRate
	if *gomaxprocs != 0 {
		runtime.GOMAXPROCS(*gomaxprocs)
		relog.Info("set GOMAXPROCS = %v", *gomaxprocs)
	}

	fdLimit := &syscall.Rlimit{*maxOpenFds, *maxOpenFds}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		relog.Fatal("can't Setrlimit %#v: err %v", *fdLimit, err)
	} else {
		relog.Info("set max-open-fds = %v", *maxOpenFds)
	}

	exportBinaryVersion()
}

func exportBinaryVersion() {
	hasher := md5.New()
	exeFile, err := os.Open("/proc/self/exe")
	if err != nil {
		panic(err)
	}
	if _, err = io.Copy(hasher, exeFile); err != nil {
		panic(err)
	}
	md5sum := hex.EncodeToString(hasher.Sum(nil))
	fileInfo, err := exeFile.Stat()
	if err != nil {
		panic(err)
	}
	mtime := fileInfo.ModTime().Format(time.RFC3339)
	version := mtime + " " + md5sum
	expvar.NewString("binary-version").Set(version)
	// rexport this value for varz scraper
	expvar.NewString("Version").Set(version)
}
