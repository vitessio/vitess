/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package tabletserver

import (
	"net/rpc"
	"vitess/relog"
)

var SqlQueryRpcService *SqlQuery

func StartQueryService(poolSize, transactionCap int, transactionTimeout float64, maxResultSize, queryCacheSize int, schemaReloadTime, queryTimeout, idleTimeout float64) {
	if SqlQueryRpcService != nil {
		relog.Warning("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(poolSize, transactionCap, transactionTimeout, maxResultSize, queryCacheSize, schemaReloadTime, queryTimeout, idleTimeout)
	rpc.Register(SqlQueryRpcService)
}

func AllowQueries(ConnFactory CreateConnectionFunc, cachingInfo map[string]uint64) {
	defer logError()
	SqlQueryRpcService.allowQueries(ConnFactory, cachingInfo)
}

func DisallowQueries() {
	defer logError()
	SqlQueryRpcService.disallowQueries()
}

func ReloadSchema() {
	defer logError()
	SqlQueryRpcService.reloadSchema()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}

func CreateTable(tableName string, cacheSize uint64) (err error) {
	defer handleError(&err)
	SqlQueryRpcService.createTable(tableName, cacheSize)
	return nil
}

func DropTable(tableName string) (err error) {
	defer handleError(&err)
	SqlQueryRpcService.dropTable(tableName)
	return nil
}

func SetRowCache(tableName string, cacheSize uint64) (err error) {
	defer handleError(&err)
	SqlQueryRpcService.setRowCache(tableName, cacheSize)
	return nil
}
