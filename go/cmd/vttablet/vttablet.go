// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
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
	"code.google.com/p/vitess/go/vt/sqlparser"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	ts "code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/vt/topo"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.01
)

var (
	port           = flag.Int("port", 6509, "port for the server")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
	tabletPath     = flag.String("tablet-path", "", "tablet alias or path to zk node representing the tablet")
	qsConfigFile   = flag.String("queryserver-config-file", "", "config file name for the query service")
	mycnfFile      = flag.String("mycnf-file", "", "my.cnf file")
	rowcache       = flag.String("rowcache", "", "rowcache connection, host:port or /path/to/socket")
	authConfig     = flag.String("auth-credentials", "", "name of file containing auth credentials")
	queryLog       = flag.String("debug-querylog-file", "", "for testing: log all queries to this file")
	customrules    = flag.String("customrules", "", "custom query rules file")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")

	securePort = flag.Int("secure-port", 0, "port for the secure server")
	cert       = flag.String("cert", "", "cert file")
	key        = flag.String("key", "", "key file")
	caCert     = flag.String("ca-cert", "", "ca-cert file")
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
	CachePoolCap:       400,
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

var schemaOverrides []ts.SchemaOverride

// tabletParamToTabletAlias takes either an old style ZK tablet path or a
// new style tablet alias as a string, and returns a TabletAlias.
func tabletParamToTabletAlias(param string) topo.TabletAlias {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" || zkPathParts[4] != "tablets" {
			relog.Fatal("Invalid tablet path: %v", param)
		}
		param = zkPathParts[2] + "-" + zkPathParts[5]
	}
	result, err := topo.ParseTabletAliasString(param)
	if err != nil {
		relog.Fatal("Invalid tablet alias %v: %v", param, err)
	}
	return result
}

func main() {
	dbConfigsFile, dbCredentialsFile := dbconfigs.RegisterCommonFlags()
	flag.Parse()

	if err := servenv.Init("vttablet"); err != nil {
		relog.Fatal("Error in servenv.Init: %s", err)
	}

	tabletAlias := tabletParamToTabletAlias(*tabletPath)

	mycnf := readMycnf(tabletAlias.Uid)
	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, *dbConfigsFile, *dbCredentialsFile)
	if err != nil {
		relog.Warning("%s", err)
	}
	dbcfgs.App.Memcache = *rowcache

	if err := jscfg.ReadJson(*overridesFile, &schemaOverrides); err != nil {
		relog.Warning("%s", err)
	} else {
		data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
		relog.Info("schemaOverrides: %s\n", data)
	}

	initQueryService(dbcfgs)
	initUpdateStreamService(mycnf)
	ts.RegisterCacheInvalidator()                                                   // depends on both query and updateStream
	err = initAgent(tabletAlias, dbcfgs, mycnf, *dbConfigsFile, *dbCredentialsFile) // depends on both query and updateStream
	if err != nil {
		relog.Fatal("%s", err)
	}

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
	snapshotDir := mysqlctl.SnapshotDir(tabletAlias.Uid)
	allowedPaths := []string{
		path.Join(vtenv.VtDataRoot(), "data"),
		mysqlctl.TabletDir(tabletAlias.Uid),
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
		if *securePort != 0 {
			relog.Info("listening on secure port %v", *securePort)
			umgmt.StartHttpsServer(fmt.Sprintf(":%v", *securePort), *cert, *key, *caCert)
		}
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

func initAgent(tabletAlias topo.TabletAlias, dbcfgs dbconfigs.DBConfigs, mycnf *mysqlctl.Mycnf, dbConfigsFile, dbCredentialsFile string) error {
	topoServer := topo.GetServer()
	umgmt.AddCloseCallback(func() {
		topo.CloseServers()
	})

	bindAddr := fmt.Sprintf(":%v", *port)
	secureAddr := ""
	if *securePort != 0 {
		secureAddr = fmt.Sprintf(":%v", *securePort)
	}

	// Action agent listens to changes in zookeeper and makes
	// modifications to this tablet.
	agent, err := tm.NewActionAgent(topoServer, tabletAlias, *mycnfFile, dbConfigsFile, dbCredentialsFile)
	if err != nil {
		return err
	}
	agent.AddChangeCallback(func(oldTablet, newTablet topo.Tablet) {
		if newTablet.IsServingType() {
			if dbcfgs.App.Dbname == "" {
				dbcfgs.App.Dbname = newTablet.DbName()
			}
			dbcfgs.App.KeyRange = newTablet.KeyRange
			dbcfgs.App.Keyspace = newTablet.Keyspace
			dbcfgs.App.Shard = newTablet.Shard
			// Transitioning from replica to master, first disconnect
			// existing connections. "false" indicateds that clients must
			// re-resolve their endpoint before reconnecting.
			if newTablet.Type == topo.TYPE_MASTER && oldTablet.Type != topo.TYPE_MASTER {
				ts.DisallowQueries(false)
			}
			qrs := loadCustomRules()
			if dbcfgs.App.KeyRange.IsPartial() {
				qr := ts.NewQueryRule("enforce keyspace_id range", "keyspace_id_not_in_range", ts.QR_FAIL_QUERY)
				qr.AddPlanCond(sqlparser.PLAN_INSERT_PK)
				err = qr.AddBindVarCond("keyspace_id", true, true, ts.QR_NOTIN, dbcfgs.App.KeyRange)
				if err != nil {
					relog.Warning("Unable to add keyspace rule: %v", err)
				} else {
					qrs.Add(qr)
				}
			}
			ts.AllowQueries(dbcfgs.App, schemaOverrides, qrs)
			mysqlctl.EnableUpdateStreamService(string(newTablet.Type), dbcfgs)
			if newTablet.Type != topo.TYPE_MASTER {
				ts.StartRowCacheInvalidation()
			}
		} else {
			ts.DisallowQueries(false)
			mysqlctl.DisableUpdateStreamService()
			if newTablet.Type != topo.TYPE_MASTER {
				ts.StopRowCacheInvalidation()
			}
		}
	})

	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)
	if err := agent.Start(bindAddr, secureAddr, mysqld.Addr()); err != nil {
		return err
	}
	umgmt.AddCloseCallback(func() {
		agent.Stop()
	})

	// register the RPC services from the agent
	agent.RegisterQueryService(mysqld)

	return nil
}

func initQueryService(dbcfgs dbconfigs.DBConfigs) {
	ts.SqlQueryLogger.ServeLogs("/debug/querylog")
	ts.TxLogger.ServeLogs("/debug/txlog")

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
			ts.QueryLogger = relog.New(f, "", relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries(true)
	})
}

func loadCustomRules() (qrs *ts.QueryRules) {
	if *customrules == "" {
		return ts.NewQueryRules()
	}

	data, err := ioutil.ReadFile(*customrules)
	if err != nil {
		relog.Fatal("Error reading file %v: %v", *customrules, err)
	}

	qrs = ts.NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		relog.Fatal("Error unmarshaling query rules %v", err)
	}
	return qrs
}

func handleSnapshot(rw http.ResponseWriter, req *http.Request, snapshotDir string, allowedPaths []string) {
	// if we get any error, we'll try to write a server error
	// (it will fail if the header has already been written, but at least
	// we won't crash vttablet)
	defer func() {
		if x := recover(); x != nil {
			relog.Error("vttablet http server panic: %v", x)
			http.Error(rw, fmt.Sprintf("500 internal server error: %v", x), http.StatusInternalServerError)
		}
	}()

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

		if strings.Contains(ae, "gzip") {
			gz, err := cgzip.NewWriterLevel(rw, cgzip.Z_BEST_SPEED)
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
