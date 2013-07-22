// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/relog"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

type Config struct {
	PoolSize           int
	StreamPoolSize     int
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	StreamBufferSize   int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	IdleTimeout        float64
	RowCache           []string
}

var SqlQueryRpcService *SqlQuery

func RegisterQueryService(config Config) {
	if SqlQueryRpcService != nil {
		relog.Warning("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(config)
	proto.RegisterAuthenticated(SqlQueryRpcService)
	http.HandleFunc("/debug/health", healthCheck)
}

// AllowQueries can take an indefinite amount of time to return because
// it keeps retrying until it obtains a valid connection to the database.
func AllowQueries(dbconfig dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules) {
	defer logError()
	SqlQueryRpcService.allowQueries(dbconfig, schemaOverrides, qrs)
}

// DisallowQueries can take a long time to return (not indefinite) because
// it has to wait for queries & transactions to be completed or killed,
// and also for house keeping goroutines to be terminated.
func DisallowQueries(forRestart bool) {
	defer logError()
	SqlQueryRpcService.disallowQueries(forRestart)
}

// Reload the schema. If the query service is not running, nothing will happen
func ReloadSchema() {
	defer logError()
	SqlQueryRpcService.qe.schemaInfo.triggerReload()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}

func IsCachePoolAvailable() bool {
	return !SqlQueryRpcService.qe.cachePool.IsClosed()
}

func InvalidateForDml(cacheInvalidate *proto.CacheInvalidate) {
	SqlQueryRpcService.InvalidateForDml(cacheInvalidate)
}

func InvalidateForDDL(ddlInvalidate *proto.DDLInvalidate) {
	SqlQueryRpcService.InvalidateForDDL(ddlInvalidate)
}

func SetQueryRules(qrs *QueryRules) {
	SqlQueryRpcService.qe.schemaInfo.SetRules(qrs)
}

func GetQueryRules() (qrs *QueryRules) {
	return SqlQueryRpcService.qe.schemaInfo.GetRules()
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	err := SqlQueryRpcService.Execute(
		new(rpcproto.Context),
		&proto.Query{Sql: "select 1 from dual", SessionId: SqlQueryRpcService.sessionId},
		new(mproto.QueryResult),
	)
	if err != nil {
		w.Write([]byte("notok"))
	}
	w.Write([]byte("ok"))
}
