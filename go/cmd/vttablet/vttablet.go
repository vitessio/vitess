// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"compress/gzip"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/cgzip"
	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/auth"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	vtenv "code.google.com/p/vitess/go/vt/env"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/servenv"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	ts "code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/zk"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.01
)

var (
	port           = flag.Int("port", 6509, "port for the server")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
	tabletPath     = flag.String("tablet-path", "", "path to zk node representing the tablet")
	qsConfigFile   = flag.String("queryserver-config-file", "", "config file name for the query service")
	mycnfFile      = flag.String("mycnf-file", "", "my.cnf file")
	authConfig     = flag.String("auth-credentials", "", "name of file containing auth credentials")
	queryLog       = flag.String("debug-querylog-file", "", "for testing: log all queries to this file")
)

// Default values for the config
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great.  (the overhead makes the final packets on the wire about
// twice bigger than this).
var qsConfig = ts.Config{
	CachePoolCap:       1000,
	PoolSize:           16,
	StreamPoolSize:     750,
	TransactionCap:     20,
	TransactionTimeout: 30,
	MaxResultSize:      10000,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
}

func main() {
	dbConfigsFile, dbCredentialsFile := dbconfigs.RegisterCommonFlags()
	flag.Parse()

	servenv.Init("vttablet")

	_, tabletidStr := path.Split(*tabletPath)
	tabletId, err := tm.ParseUid(tabletidStr)
	if err != nil {
		relog.Fatal("%s", err)
	}

	mycnf := readMycnf(tabletId)
	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, *dbConfigsFile, *dbCredentialsFile)
	if err != nil {
		relog.Warning("%s", err)
	}

	initQueryService(dbcfgs)
	initUpdateStreamService(mycnf)
	initAgent(dbcfgs, mycnf, *dbConfigsFile, *dbCredentialsFile) // depends on both query and updateStream

	rpc.HandleHTTP()

	// NOTE(szopa): Changing credentials requires a server
	// restart.
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			relog.Error("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		serveAuthRPC()
	}

	serveRPC()

	// make a list of paths we can serve HTTP traffic from.
	// we don't resolve them here to real paths, as they might not exits yet
	snapshotDir := mysqlctl.SnapshotDir(uint32(tabletId))
	allowedPaths := []string{
		path.Join(vtenv.VtDataRoot(), "data"),
		mysqlctl.TabletDir(uint32(tabletId)),
		snapshotDir,
		mycnf.DataDir,
		mycnf.InnodbDataHomeDir,
		mycnf.InnodbLogGroupHomeDir,
	}

	// NOTE: trailing slash in pattern means we handle all paths with this prefix
	http.Handle(mysqlctl.SnapshotURLPath+"/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleSnapshot(w, r, snapshotDir, allowedPaths)
	}))

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	umgmt.SetLameDuckPeriod(float32(*lameDuckPeriod))
	umgmt.SetRebindDelay(float32(*rebindDelay))
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})
	umgmt.AddStartupCallback(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		go func() {
			for sig := range c {
				umgmt.SigTermHandler(sig)
			}
		}()
	})

	relog.Info("started vttablet %v", *port)
	umgmtSocket := fmt.Sprintf("/tmp/vttablet-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}

func serveAuthRPC() {
	bsonrpc.ServeAuthRPC()
	jsonrpc.ServeAuthRPC()
}

func serveRPC() {
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()
}

func readMycnf(tabletId uint32) *mysqlctl.Mycnf {
	if *mycnfFile == "" {
		*mycnfFile = mysqlctl.MycnfFile(tabletId)
	}
	mycnf, mycnfErr := mysqlctl.ReadMycnf(*mycnfFile)
	if mycnfErr != nil {
		relog.Fatal("mycnf read failed: %v", mycnfErr)
	}
	return mycnf
}

func initAgent(dbcfgs dbconfigs.DBConfigs, mycnf *mysqlctl.Mycnf, dbConfigsFile, dbCredentialsFile string) {
	zconn := zk.NewMetaConn(false)
	expvar.Publish("ZkMetaConn", zconn)
	umgmt.AddCloseCallback(func() {
		zconn.Close()
	})

	bindAddr := fmt.Sprintf(":%v", *port)

	// Action agent listens to changes in zookeeper and makes
	// modifications to this tablet.
	agent := tm.NewActionAgent(zconn, *tabletPath, *mycnfFile, dbConfigsFile, dbCredentialsFile)
	agent.AddChangeCallback(func(oldTablet, newTablet tm.Tablet) {
		if newTablet.IsServingType() {
			if dbcfgs.App.Dbname == "" {
				dbcfgs.App.Dbname = newTablet.DbName()
			}
			// Transitioning from replica to master, first disconnect
			// existing connections. "false" indicateds that clients must
			// re-resolve their endpoint before reconnecting.
			if newTablet.Type == tm.TYPE_MASTER && oldTablet.Type != tm.TYPE_MASTER {
				ts.DisallowQueries(false)
			}
			ts.AllowQueries(dbcfgs.App)
			mysqlctl.EnableUpdateStreamService(string(newTablet.Type), dbcfgs)
		} else {
			ts.DisallowQueries(false)
			mysqlctl.DisableUpdateStreamService()
		}
	})
	agent.Start(bindAddr, mycnf.MysqlAddr())
	umgmt.AddCloseCallback(func() {
		agent.Stop()
	})

	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	// The TabletManager service exports read-only management related
	// data.
	tm := tm.NewTabletManager(bindAddr, nil, mysqld)
	rpc.Register(tm)
}

func initQueryService(dbcfgs dbconfigs.DBConfigs) {
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}

	ts.SqlQueryLogger.ServeLogs("/debug/vt/querylog")

	if err := jscfg.ReadJson(*qsConfigFile, &qsConfig); err != nil {
		relog.Warning("%s", err)
	}
	ts.RegisterQueryService(qsConfig)
	usefulLameDuckPeriod := float64(qsConfig.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		relog.Info("readjusted -lame-duck-period to %f", *lameDuckPeriod)
	}
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries(true)
	})
}

func handleSnapshot(rw http.ResponseWriter, req *http.Request, snapshotDir string, allowedPaths []string) {
	// /snapshot must be rewritten to the actual location of the snapshot.
	relative, err := filepath.Rel(mysqlctl.SnapshotURLPath, req.URL.Path)
	if err != nil {
		relog.Error("bad snapshot relative path %v %v", req.URL.Path, err)
		http.Error(rw, "400 bad request", http.StatusBadRequest)
		return
	}

	// Make sure that realPath is absolute and resolve any escaping from
	// snapshotDir through a symlink.
	realPath, err := filepath.Abs(path.Join(snapshotDir, relative))
	if err != nil {
		relog.Error("bad snapshot absolute path %v %v", req.URL.Path, err)
		http.Error(rw, "400 bad request", http.StatusBadRequest)
		return
	}

	realPath, err = filepath.EvalSymlinks(realPath)
	if err != nil {
		relog.Error("bad snapshot symlink eval %v %v", req.URL.Path, err)
		http.Error(rw, "400 bad request", http.StatusBadRequest)
		return
	}

	// Resolve all the possible roots and make sure we're serving
	// from one of them
	for _, allowedPath := range allowedPaths {
		// eval the symlinks of the allowed path
		allowedPath, err := filepath.EvalSymlinks(allowedPath)
		if err != nil {
			continue
		}
		if strings.HasPrefix(realPath, allowedPath) {
			sendFile(rw, req, realPath)
			return
		}
	}

	relog.Error("bad snapshot real path %v %v", req.URL.Path, realPath)
	http.Error(rw, "400 bad request", http.StatusBadRequest)
}

// custom function to serve files
func sendFile(rw http.ResponseWriter, req *http.Request, path string) {
	relog.Info("serve %v %v", req.URL.Path, path)
	file, err := os.Open(path)
	if err != nil {
		http.NotFound(rw, req)
		return
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		http.NotFound(rw, req)
		return
	}

	// for directories, or for files smaller than 1k, use library
	if fileinfo.Mode().IsDir() || fileinfo.Size() < 1024 {
		http.ServeFile(rw, req, path)
		return
	}

	// supports If-Modified-Since header
	if t, err := time.Parse(http.TimeFormat, req.Header.Get("If-Modified-Since")); err == nil && fileinfo.ModTime().Before(t.Add(1*time.Second)) {
		rw.WriteHeader(http.StatusNotModified)
		return
	}

	// support Accept-Encoding header
	var writer io.Writer = rw
	var reader io.Reader = file
	if !strings.HasSuffix(path, ".gz") {
		ae := req.Header.Get("Accept-Encoding")

		if strings.Contains(ae, "fgzip") {
			relog.Info("Forking gzip to serve %v", path)
			cmd := exec.Command("gzip", "--fast", "-c", path)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			defer stdout.Close()
			if err = cmd.Start(); err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}

			rw.Header().Set("Content-Encoding", "gzip")
			defer func() {
				// An early abort leaves a process dangling.
				cmd.Process.Kill()
				if err := cmd.Wait(); err != nil {
					relog.Warning("gzip err for %v: %v", path, err)
				} else {
					relog.Info("gzip done for %v", path)
				}
			}()

			reader = stdout

		} else if strings.Contains(ae, "cgzip") {
			gz, err := cgzip.NewWriterLevel(rw, cgzip.Z_BEST_SPEED)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			rw.Header().Set("Content-Encoding", "gzip")
			defer gz.Close()
			writer = gz

		} else if strings.Contains(ae, "gzip") {
			gz, err := gzip.NewWriterLevel(rw, gzip.BestSpeed)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			rw.Header().Set("Content-Encoding", "gzip")
			defer gz.Close()
			writer = gz
		}
	}

	// add content-length if we know it
	if writer == rw && reader == file {
		rw.Header().Set("Content-Length", fmt.Sprintf("%v", fileinfo.Size()))
	}

	// and just copy content out
	rw.Header().Set("Last-Modified", fileinfo.ModTime().UTC().Format(http.TimeFormat))
	rw.WriteHeader(http.StatusOK)
	if _, err := io.Copy(writer, reader); err != nil {
		relog.Warning("transfer failed %v: %v", path, err)
	}
}

func initUpdateStreamService(mycnf *mysqlctl.Mycnf) {
	mysqlctl.RegisterUpdateStreamService(mycnf)

	umgmt.AddCloseCallback(func() {
		mysqlctl.DisableUpdateStreamService()
	})
}
