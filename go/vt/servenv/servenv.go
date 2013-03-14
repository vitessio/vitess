// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package env defines and initializes command line flags that control
// the runtime environment.
//
// After the main program has called flag.Parse, it needs to call
// env.Init to make env use the command line variables to initialize
// the environment.

package servenv

import (
	"crypto/md5"
	"encoding/hex"
	"expvar"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/logfile"
	"code.google.com/p/vitess/go/relog"
)

var (
	logfileName  = flag.String("logfile", "/dev/stderr", "base log file name")
	logFrequency = flag.Int64("logfile.frequency", 0,
		"rotation frequency in seconds")
	logMaxSize  = flag.Int64("logfile.maxsize", 0, "max file size in bytes")
	logMaxFiles = flag.Int64("logfile.maxfiles", 0, "max number of log files")
	logLevel    = flag.String("log.level", "WARNING", "set log level")

	memProfileRate = flag.Int("mem-profile-rate", 512*1024,
		"profile every n bytes allocated")
)

func Init(logPrefix string) error {
	// Once you run as root, you pretty much destroy the chances of a
	// non-privileged user starting the program correctly.
	if uid := os.Getuid(); uid == 0 {
		return fmt.Errorf("running this as root makes no sense")
	}
	if logPrefix != "" {
		logPrefix += " "
	}
	logPrefix += fmt.Sprintf("[%v] ", os.Getpid())
	f, err := logfile.Open(*logfileName, *logFrequency, *logMaxSize, *logMaxFiles)
	if err != nil {
		return fmt.Errorf("unable to open logfile %s: %v", *logfileName, err)
	}
	relog.SetOutput(f)
	relog.SetPrefix(logPrefix)
	relog.SetLevel(relog.DEBUG)
	relog.HijackLog(nil)
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
	relog.Info("GOMAXPROCS = %v", gomaxprocs)

	// We used to set this limit directly, but you pretty much have to
	// use a root account to allow increasing a limit reliably. Dropping
	// privileges is also tricky. The best strategy is to make a shell
	// script set up the limits as root and switch users before starting
	// the server.
	fdLimit := &syscall.Rlimit{}
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		relog.Error("max-open-fds failed: %v", err)
	} else {
		// Could report this in an expvar instead.
		relog.Info("max-open-fds: %v", fdLimit.Cur)
	}

	return exportBinaryVersion()
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
