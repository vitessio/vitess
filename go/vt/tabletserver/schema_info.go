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
	"code.google.com/p/vitess/go/cache"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/schema"
	"code.google.com/p/vitess/go/vt/sqlparser"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type SchemaInfo struct {
	mu             sync.Mutex
	tables         map[string]*TableInfo
	queryCacheSize int
	queries        *cache.LRUCache
	connFactory    CreateConnectionFunc
	cachePool      *CachePool
}

func NewSchemaInfo(queryCacheSize int) *SchemaInfo {
	self := &SchemaInfo{queryCacheSize: queryCacheSize}
	http.Handle("/debug/schema/", self)
	return self
}

func (self *SchemaInfo) Open(connFactory CreateConnectionFunc, cachePool *CachePool) {
	conn, err := connFactory()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get connection: %v", err))
	}
	defer conn.Close()

	self.cachePool = cachePool
	tables, err := conn.ExecuteFetch([]byte("show tables"), 10000)
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get table list: %v", err))
	}
	self.tables = make(map[string]*TableInfo, len(tables.Rows))
	self.tables["dual"] = NewTableInfo(conn, "dual", self.cachePool)
	for _, row := range tables.Rows {
		tableName := row[0].(string)
		tableInfo := NewTableInfo(conn, tableName, self.cachePool)
		if tableInfo == nil {
			continue
		}
		self.tables[tableName] = tableInfo
	}
	self.queries = cache.NewLRUCache(uint64(self.queryCacheSize))
	self.connFactory = connFactory
}

func (self *SchemaInfo) Close() {
	self.tables = nil
	self.queries = nil
	self.connFactory = nil
}

// ThrottleDDL prevents consecutive alters of a table
// within the second. This helps us ensure create_time
// always new. This is not foolproof. Parallel requests
// can still bypass this.
func (self *SchemaInfo) ThrottleDDL(tableName string) {
	for {
		self.mu.Lock()
		tableInfo, ok := self.tables[tableName]
		self.mu.Unlock()
		if !ok {
			return
		}
		if time.Now().Unix() > tableInfo.TimeCreated.Unix()+1 {
			return
		}
		time.Sleep(1e9)
	}
}

func (self *SchemaInfo) CreateTable(tableName string) {
	conn, err := self.connFactory()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get connection for create table %s: %v", tableName, err))
	}
	defer conn.Close()
	self.createTable(conn, tableName)
}

func (self *SchemaInfo) createTable(conn *DBConnection, tableName string) {
	tableInfo := NewTableInfo(conn, tableName, self.cachePool)
	if tableInfo == nil {
		panic(NewTabletError(FATAL, "Could not read table info: %s", tableName))
	}
	if tableInfo.CacheType != 0 {
		relog.Info("Initialized cached table: %s", tableInfo.Cache.prefix)
	} else {
		relog.Info("Initialized table: %s", tableName)
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if _, ok := self.tables[tableName]; ok {
		panic(NewTabletError(FAIL, "Table %s already exists", tableName))
	}
	self.tables[tableName] = tableInfo
}

func (self *SchemaInfo) DropTable(tableName string) {
	self.mu.Lock()
	defer self.mu.Unlock()
	delete(self.tables, tableName)
	self.queries.Clear()
	relog.Info("Table %s forgotten", tableName)
}

func (self *SchemaInfo) GetPlan(sql string, mustCache bool) (*sqlparser.ExecPlan, *TableInfo) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if plan := self.getQuery(sql); plan != nil {
		return plan, self.tables[plan.TableName]
	}

	GetTable := func(tableName string) (table *schema.Table, ok bool) {
		tableInfo, ok := self.tables[tableName]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	plan, err := sqlparser.ExecParse(sql, GetTable)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	if plan.PlanId == sqlparser.PLAN_DDL {
		return plan, nil
	}
	if mustCache {
		self.queries.Set(sql, plan)
	}
	return plan, self.tables[plan.TableName]
}

func (self *SchemaInfo) GetTable(tableName string) *TableInfo {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.tables[tableName]
}

func (self *SchemaInfo) getQuery(sql string) *sqlparser.ExecPlan {
	if cacheResult, ok := self.queries.Get(sql); ok {
		return cacheResult.(*sqlparser.ExecPlan)
	}
	return nil
}

func (self *SchemaInfo) SetQueryCacheSize(size int) {
	if size <= 0 {
		panic(NewTabletError(FAIL, "cache size %v out of range", size))
	}
	self.queryCacheSize = size
	self.queries.SetCapacity(uint64(size))
}

func (self *SchemaInfo) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/debug/schema/query_cache" {
		keys := self.queries.Keys()
		response.Header().Set("Content-Type", "text/plain")
		if keys == nil {
			response.Write([]byte("empty\n"))
			return
		}
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%s\n", v)))
			if plan := self.getQuery(v); plan != nil {
				if b, err := json.MarshalIndent(plan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
					response.Write(([]byte)("\n\n"))
				}
			}
		}
	} else if request.URL.Path == "/debug/schema/tables" {
		response.Header().Set("Content-Type", "text/plain")
		self.mu.Lock()
		tstats := make(map[string]struct{ hits, absent, misses int64 })
		var temp, totals struct{ hits, absent, misses int64 }
		for k, v := range self.tables {
			if v.CacheType != 0 {
				temp.hits, temp.absent, temp.misses = v.Stats()
				tstats[k] = temp
				totals.hits += temp.hits
				totals.absent += temp.absent
				totals.misses += temp.misses
			}
		}
		self.mu.Unlock()
		response.Write([]byte("{\n"))
		for k, v := range tstats {
			fmt.Fprintf(response, "\"%s\": {\"Hits\": %v, \"Absent\": %v, \"Misses\": %v},\n", k, v.hits, v.absent, v.misses)
		}
		fmt.Fprintf(response, "\"Totals\": {\"Hits\": %v, \"Absent\": %v, \"Misses\": %v}\n", totals.hits, totals.absent, totals.misses)
		response.Write([]byte("}\n"))
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}
