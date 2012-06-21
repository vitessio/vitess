// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"code.google.com/p/vitess/go/logfile"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	"code.google.com/p/vitess/go/sighandler"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/umgmt"
	ts "code.google.com/p/vitess/go/vt/tabletserver"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"runtime"
	"syscall"
	"time"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.0
)

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

func main() {
	port := flag.Int("port", 6510, "tcp port to serve on")
	memProfileRate := flag.Int("mem-profile-rate", 512*1024, "profile every n bytes allocated")
	maxOpenFds := flag.Uint64("max-open-fds", 32768, "max open file descriptors")
	lameDuckPeriod := flag.Float64("lame-duck-period", DefaultLameDuckPeriod,
		"how long to give in-flight transactions to finish")
	rebindDelay := flag.Float64("rebind-delay", DefaultRebindDelay,
		"artificial delay before rebinding a hijacked listener")
	logfileName := flag.String("logfile", "/dev/stderr", "base log file name")
	logFrequency := flag.Int64("logfile.frequency", 0, "rotation frequency in seconds")
	logMaxSize := flag.Int64("logfile.maxsize", 0, "max file size in bytes")
	logMaxFiles := flag.Int64("logfile.maxfiles", 0, "max number of log files")
	flag.Parse()

	exportBinaryVersion()

	runtime.MemProfileRate = *memProfileRate

	f, err := logfile.Open(*logfileName, *logFrequency, *logMaxSize, *logMaxFiles)
	if err != nil {
		panic(fmt.Sprintf("unable to open logfile %s", *logfileName))
	}
	logger := relog.New(f, "vtocc ",
		log.Ldate|log.Lmicroseconds|log.Lshortfile, relog.DEBUG)
	relog.SetLogger(logger)

	fdLimit := &syscall.Rlimit{*maxOpenFds, *maxOpenFds}
	if err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		relog.Fatal("can't Setrlimit %#v: err %v", *fdLimit, err)
	} else {
		relog.Info("set max-open-fds = %v", *maxOpenFds)
	}

	config, dbconfig := ts.Init()
	qm := &OccManager{config, dbconfig}
	rpc.Register(qm)
	ts.StartQueryService(config)
	ts.AllowQueries(dbconfig)
	rpc.HandleHTTP()
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()
	relog.Info("started vtocc %v", *port)

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.

	usefulLameDuckPeriod := float64(config.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		relog.Info("readjusted -lame-duck-period to %f", *lameDuckPeriod)
	}
	umgmt.SetLameDuckPeriod(float32(*lameDuckPeriod))
	umgmt.SetRebindDelay(float32(*rebindDelay))
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})
	umgmt.AddStartupCallback(func() {
		sighandler.SetSignalHandler(syscall.SIGTERM,
			umgmt.SigTermHandler)
	})
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries()
	})

	umgmtSocket := fmt.Sprintf("/tmp/vtocc-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}

type OccManager struct {
	config   ts.Config
	dbconfig map[string]interface{}
}

func (self *OccManager) GetSessionId(dbname *string, sessionId *int64) error {
	if *dbname != self.dbconfig["dbname"].(string) {
		return errors.New(fmt.Sprintf("db name mismatch, expecting %v, received %v", self.dbconfig["dbname"].(string), *dbname))
	}
	*sessionId = ts.GetSessionId()
	return nil
}
