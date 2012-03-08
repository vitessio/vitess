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
	"fmt"
	"net/http"
	"sync"
	"time"
	"vitess/cache"
	"vitess/relog"
	"vitess/timer"
	"vitess/vt/schema"
	"vitess/vt/sqlparser"
)

type SchemaInfo struct {
	sync.Mutex
	Tables           map[string]*TableInfo
	QueryCacheSize   int
	Queries          *cache.LRUCache
	ConnFactory      CreateConnectionFunc
	SchemaReloadTime time.Duration
	LastReload       time.Time
	ticks            *timer.Timer
}

func NewSchemaInfo(queryCacheSize int, schemaReloadTime time.Duration) *SchemaInfo {
	self := &SchemaInfo{
		QueryCacheSize:   queryCacheSize,
		SchemaReloadTime: schemaReloadTime,
		ticks:            timer.NewTimer(schemaReloadTime),
	}
	http.Handle("/debug/query_cache", self)
	return self
}

func (self *SchemaInfo) Open(ConnFactory CreateConnectionFunc, cachingInfo map[string]uint64) {
	conn, err := ConnFactory()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get connection: %v", err))
	}
	defer conn.Close()

	if cachingInfo == nil {
		cachingInfo = make(map[string]uint64)
	}
	self.LastReload = time.Now()
	tables, err := conn.ExecuteFetch([]byte("show tables"), 10000)
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get table list: %v", err))
	}
	self.Tables = make(map[string]*TableInfo, len(tables.Rows))
	self.Tables["dual"] = NewTableInfo(conn, "dual", 0)
	for _, row := range tables.Rows {
		tableName := row[0].(string)
		tableInfo := NewTableInfo(conn, tableName, cachingInfo[tableName])
		if tableInfo == nil {
			continue
		}
		self.Tables[tableName] = tableInfo
	}
	self.Queries = cache.NewLRUCache(uint64(self.QueryCacheSize))
	self.ConnFactory = ConnFactory
	go self.SchemaReloader()
}

func (self *SchemaInfo) Close() {
	self.ticks.Close()
	self.Tables = nil
	self.Queries = nil
	self.ConnFactory = nil
}

func (self *SchemaInfo) SchemaReloader() {
	for self.ticks.Next() {
		self.Reload()
	}
}

func (self *SchemaInfo) Reload() {
	conn, err := self.ConnFactory()
	if err != nil {
		relog.Error("Could not get connection for reload: %v", err)
		return
	}
	defer conn.Close()

	query_for_schema_reload := fmt.Sprintf("show table status where unix_timestamp(create_time) > %v", self.LastReload.Unix())
	self.LastReload = time.Now()
	tables, err := conn.ExecuteFetch([]byte(query_for_schema_reload), 10000)
	if err != nil {
		relog.Error("Could not get table list for reload: %v", err)
		return
	}
	if len(tables.Rows) == 0 {
		return
	}
	for _, row := range tables.Rows {
		tableName := row[0].(string)
		relog.Info("Reloading: %s", tableName)
		tableInfo := self.get(tableName)
		if tableInfo != nil {
			self.Put(tableInfo)
			self.AlterTable(tableName, 0)
		} else {
			self.CreateTable(tableName, 0)
		}
	}
}

func (self *SchemaInfo) CreateTable(tableName string, cacheSize uint64) {
	conn, err := self.ConnFactory()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get connection for create table %s: %v", tableName, err))
	}
	defer conn.Close()

	tableInfo := NewTableInfo(conn, tableName, cacheSize)
	if tableInfo == nil {
		panic(NewTabletError(FATAL, "Could not create table %s", tableName))
	}
	self.Lock()
	defer self.Unlock()
	if _, ok := self.Tables[tableName]; ok {
		panic(NewTabletError(FAIL, "Table %s already exists", tableName))
	}
	self.Tables[tableName] = tableInfo
}

func (self *SchemaInfo) DropTable(tableName string) {
	self.Lock()
	defer self.Unlock()
	tableInfo, ok := self.Tables[tableName]
	if !ok {
		panic(NewTabletError(FAIL, "Table %s doesn't exists", tableName))
	}
	delete(self.Tables, tableName)

	tableInfo.Lock()
	defer tableInfo.Unlock()
	self.Queries.Clear()
}

func (self *SchemaInfo) AlterTable(tableName string, cacheSize uint64) {
	self.DropTable(tableName)

	self.CreateTable(tableName, cacheSize)
}

func (self *SchemaInfo) SetRowCache(tableName string, cacheSize uint64) {
	if self.simpleSetRowCache(tableName, cacheSize) {
		return
	}
	self.DropTable(tableName)
	self.CreateTable(tableName, cacheSize)
}

func (self *SchemaInfo) simpleSetRowCache(tableName string, cacheSize uint64) bool {
	tableInfo := self.get(tableName)
	defer self.Put(tableInfo)
	if tableInfo.CacheType == 1 && cacheSize != 0 {
		tableInfo.RowCache.SetCapacity(cacheSize)
		return true
	}
	return false
}

// Caller for GetPlan must call Put(tableInfo) to release lock on TableInfo
func (self *SchemaInfo) GetPlan(sql string, mustCache bool) (*sqlparser.ExecPlan, *TableInfo) {
	self.Lock()
	defer self.Unlock()
	if plan := self.getQuery(sql); plan != nil {
		return plan, self.get(plan.TableName)
	}

	GetTable := func(tableName string) (table *schema.Table, ok bool) {
		tableInfo, ok := self.Tables[tableName]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	plan, err := sqlparser.ExecParse(sql, GetTable)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}

	if mustCache {
		self.Queries.Set(sql, plan)
	}
	return plan, self.get(plan.TableName)
}

// Caller for GetTable must call Put(tableInfo) to release lock on TableInfo
func (self *SchemaInfo) GetTable(tableName string) *TableInfo {
	self.Lock()
	defer self.Unlock()
	return self.get(tableName)
}

func (self *SchemaInfo) get(tableName string) *TableInfo {
	tableInfo, ok := self.Tables[tableName]
	if ok {
		tableInfo.RLock()
	}
	return tableInfo
}

func (self *SchemaInfo) Put(tableInfo *TableInfo) {
	if tableInfo != nil {
		tableInfo.RUnlock()
	}
}

func (self *SchemaInfo) getQuery(sql string) *sqlparser.ExecPlan {
	if cacheResult, ok := self.Queries.Get(sql); ok {
		return cacheResult.(*sqlparser.ExecPlan)
	}
	return nil
}

func (self *SchemaInfo) SetQueryCacheSize(size int) {
	if size <= 0 {
		panic(NewTabletError(FAIL, "cache size %v out of range", size))
	}
	self.QueryCacheSize = size
	self.Queries.SetCapacity(uint64(size))
}

func (self *SchemaInfo) SetSchemaReloadTime(reload_time time.Duration) {
	self.SchemaReloadTime = reload_time
	self.ticks.Trigger()
	self.ticks.SetInterval(reload_time)
}

func (self *SchemaInfo) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	keys := self.Queries.Keys()
	response.Header().Set("Content-Type", "text/plain")
	if keys == nil {
		response.Write([]byte("empty\n"))
		return
	}
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
	for _, v := range keys {
		response.Write([]byte(fmt.Sprintf("%s\n", v)))
	}
}
