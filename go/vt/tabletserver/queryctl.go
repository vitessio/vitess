// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

type Config struct {
	CachePoolCap       int
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
}

var SqlQueryRpcService *SqlQuery

func RegisterQueryService(config Config) {
	if SqlQueryRpcService != nil {
		relog.Warning("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(config)
	proto.RegisterAuthenticated(SqlQueryRpcService)
}

// AllowQueries can take an indefinite amount of time to return because
// it keeps retrying until it obtains a valid connection to the database.
func AllowQueries(dbconfig dbconfigs.DBConfig) {
	defer logError()
	SqlQueryRpcService.allowQueries(dbconfig)
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
