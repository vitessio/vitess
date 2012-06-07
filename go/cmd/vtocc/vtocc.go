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
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"runtime"
	"syscall"
	"time"
)

const (
	Version = "vtocc/1.0"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.0
)

const (
	RLIMIT_CPU        = 0
	RLIMIT_FSIZE      = 1
	RLIMIT_DATA       = 2
	RLIMIT_STACK      = 3
	RLIMIT_CORE       = 4
	RLIMIT_RSS        = 5
	RLIMIT_NPROC      = 6
	RLIMIT_NOFILE     = 7
	RLIMIT_MEMLOCK    = 8
	RLIMIT_AS         = 9
	RLIMIT_LOCKS      = 10
	RLIMIT_SIGPENDING = 11
	RLIMIT_MSGQUEUE   = 12
	RLIMIT_NICE       = 13
	RLIMIT_RTPRIO     = 14
)

type configType struct {
	Port               int
	UmgmtSocket        string
	CachePoolCap       int
	PoolSize           int
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	IdleTimeout        float64
}

var config configType = configType{
	6510,
	"/tmp/vtocc-%08x-umgmt.sock",
	1000,
	16,
	20,
	30,
	10000,
	5000,
	30 * 60,
	0,
	30 * 60,
}

var dbconfig map[string]interface{} = map[string]interface{}{
	"host":        "localhost",
	"port":        0,
	"unix_socket": "",
	"uname":       "vt_app",
	"pass":        "",
	"dbname":      "",
	"charset":     "utf8",
}

func HandleGracefulShutdown() {
	relog.Info("HandleGracefulShutdown")
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

func main() {
	memProfileRate := flag.Int("mem-profile-rate", 512*1024, "profile every n bytes allocated")
	maxOpenFds := flag.Uint64("max-open-fds", 32768, "max open file descriptors")
	configFile := flag.String("config", "", "config file name")
	dbConfigFile := flag.String("dbconfig", "", "db config file name")
	lameDuckPeriod := flag.Float64("lame-duck-period", DefaultLameDuckPeriod,
		"how long to give in-flight transactions to finish")
	rebindDelay := flag.Float64("rebind-delay", DefaultRebindDelay,
		"artificial delay before rebinding a hijacked listener")
	logfileName := flag.String("logfile", "/dev/stderr", "base log file name")
	logFrequency := flag.Int64("logfile.frequency", 0, "rotation frequency in seconds")
	logMaxSize := flag.Int64("logfile.maxsize", 0, "max file size in bytes")
	logMaxFiles := flag.Int64("logfile.maxfiles", 0, "max number of log files")
	queryLog := flag.String("querylog", "", "for testing: log all queries to this file")
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
	if *queryLog != "" {
		if f, err = os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		}
	}
	unmarshalFile(*configFile, &config)
	unmarshalFile(*dbConfigFile, &dbconfig)
	// work-around for jsonism
	if v, ok := dbconfig["port"].(float64); ok {
		dbconfig["port"] = int(v)
	}

	fdLimit := &syscall.Rlimit{*maxOpenFds, *maxOpenFds}
	if err = syscall.Setrlimit(RLIMIT_NOFILE, fdLimit); err != nil {
		relog.Fatal("can't Setrlimit %#v: err %v", *fdLimit, err)
	} else {
		relog.Info("set max-open-fds = %v", *maxOpenFds)
	}

	qm := &OccManager{config, dbconfig}
	rpc.Register(qm)

	ts.StartQueryService(
		config.CachePoolCap,
		config.PoolSize,
		config.TransactionCap,
		config.TransactionTimeout,
		config.MaxResultSize,
		config.QueryCacheSize,
		config.SchemaReloadTime,
		config.QueryTimeout,
		config.IdleTimeout,
	)
	ts.AllowQueries(dbconfig)

	rpc.HandleHTTP()
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	relog.Info("started vtocc %v", config.Port)

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
		umgmt.StartHttpServer(fmt.Sprintf(":%v", config.Port))
	})
	umgmt.AddStartupCallback(func() {
		sighandler.SetSignalHandler(syscall.SIGTERM,
			umgmt.SigTermHandler)
	})
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries()
	})
	umgmt.AddShutdownCallback(func() error {
		HandleGracefulShutdown()
		return nil
	})

	umgmtSocket := fmt.Sprintf(config.UmgmtSocket, config.Port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}

func unmarshalFile(name string, val interface{}) {
	if name != "" {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			relog.Fatal("could not read %v: %v", val, err)
		}
		if err = json.Unmarshal(data, val); err != nil {
			relog.Fatal("could not read %s: %v", val, err)
		}
	}
	data, _ := json.MarshalIndent(val, "", "  ")
	relog.Info("config: %s\n", data)
}

type OccManager struct {
	config   configType
	dbconfig map[string]interface{}
}

func (self *OccManager) GetSessionId(dbname *string, sessionId *int64) error {
	if *dbname != self.dbconfig["dbname"].(string) {
		return errors.New(fmt.Sprintf("db name mismatch, expecting %v, received %v", self.dbconfig["dbname"].(string), *dbname))
	}
	*sessionId = ts.GetSessionId()
	return nil
}
