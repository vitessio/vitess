// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap"
)

type Config struct {
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

var SqlQueryRpcService *SqlQuery

func StartQueryService(config Config) {
	if SqlQueryRpcService != nil {
		relog.Warning("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(config)
	rpcwrap.RegisterAuthenticated(SqlQueryRpcService)
}

func AllowQueries(dbconfig map[string]interface{}) {
	defer logError()
	SqlQueryRpcService.allowQueries(dbconfig)
}

func DisallowQueries() {
	defer logError()
	SqlQueryRpcService.disallowQueries()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}
