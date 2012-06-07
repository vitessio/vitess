// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/relog"
	"net/rpc"
)

var SqlQueryRpcService *SqlQuery

func StartQueryService(cachePoolCap, poolSize, transactionCap int, transactionTimeout float64, maxResultSize, queryCacheSize int, schemaReloadTime, queryTimeout, idleTimeout float64) {
	if SqlQueryRpcService != nil {
		relog.Warning("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(cachePoolCap, poolSize, transactionCap, transactionTimeout, maxResultSize, queryCacheSize, schemaReloadTime, queryTimeout, idleTimeout)
	rpc.Register(SqlQueryRpcService)
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
