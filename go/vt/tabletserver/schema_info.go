// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/cache"
	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/timer"
	"code.google.com/p/vitess/go/vt/schema"
	"code.google.com/p/vitess/go/vt/sqlparser"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const base_show_tables = "select table_name, table_type, unix_timestamp(create_time), table_comment from information_schema.tables where table_schema = database()"

type ExecPlan struct {
	*sqlparser.ExecPlan
	TableInfo *TableInfo
	Fields    []proto.Field
}

func (*ExecPlan) Size() int {
	return 1
}

type SchemaInfo struct {
	mu             sync.Mutex
	tables         map[string]*TableInfo
	queryCacheSize int
	queries        *cache.LRUCache
	connFactory    CreateConnectionFunc
	cachePool      *CachePool
	reloadTime     time.Duration
	lastChange     time.Time
	ticks          *timer.Timer
}

func NewSchemaInfo(queryCacheSize int, reloadTime time.Duration) *SchemaInfo {
	si := &SchemaInfo{
		queryCacheSize: queryCacheSize,
		reloadTime:     reloadTime,
		ticks:          timer.NewTimer(reloadTime),
	}
	http.Handle("/debug/schema/", si)
	return si
}

func (si *SchemaInfo) Open(connFactory CreateConnectionFunc, cachePool *CachePool) {
	conn, err := connFactory()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get connection: %v", err))
	}
	defer conn.Close()

	si.cachePool = cachePool
	tables, err := conn.ExecuteFetch([]byte(base_show_tables), 10000, false)
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get table list: %v", err))
	}
	si.tables = make(map[string]*TableInfo, len(tables.Rows))
	si.tables["dual"] = NewTableInfo(conn, "dual", "VIEW", nil, "", si.cachePool)
	for _, row := range tables.Rows {
		tableName := row[0].(string)
		si.updateLastChange(row[2])
		tableInfo := NewTableInfo(
			conn,
			tableName,
			row[1].(string), // table_type
			row[2],          // create_time
			row[3].(string), // table_comment
			si.cachePool,
		)
		if tableInfo == nil {
			continue
		}
		si.tables[tableName] = tableInfo
	}
	si.queries = cache.NewLRUCache(uint64(si.queryCacheSize))
	si.connFactory = connFactory
	go si.Reloader()
}

func (si *SchemaInfo) updateLastChange(createTime interface{}) {
	if createTime == nil {
		return
	}
	t, err := strconv.ParseInt(createTime.(string), 10, 64)
	if err != nil {
		relog.Warning("Could not parse time %s: %v", createTime.(string), err)
		return
	}
	if si.lastChange.Unix() < t {
		si.lastChange = time.Unix(t, 0)
	}
}

func (si *SchemaInfo) Close() {
	si.ticks.Close()
	si.tables = nil
	si.queries = nil
	si.connFactory = nil
}

func (si *SchemaInfo) Reloader() {
	si.ticks.Start()
	for si.ticks.Next() {
		si.Reload()
	}
}

func (si *SchemaInfo) Reload() {
	conn, err := si.connFactory()
	if err != nil {
		relog.Error("Could not get connection for reload: %v", err)
		return
	}
	defer conn.Close()
	tables, err := conn.ExecuteFetch([]byte(fmt.Sprintf("%s and unix_timestamp(create_time) > %v", base_show_tables, si.lastChange.Unix())), 1000, false)
	if err != nil {
		relog.Warning("Could not get table list for reload: %v", err)
	}
	for _, row := range tables.Rows {
		tableName := row[0].(string)
		si.updateLastChange(row[2])
		relog.Info("Reloading: %s", tableName)
		si.mu.Lock()
		_, ok := si.tables[tableName]
		si.mu.Unlock()
		if ok {
			si.DropTable(tableName)
		}
		si.createTable(conn, tableName)
	}
}

func (si *SchemaInfo) CreateTable(tableName string) {
	conn, err := si.connFactory()
	if err != nil {
		panic(NewTabletError(FATAL, "Could not get connection for create table %s: %v", tableName, err))
	}
	defer conn.Close()
	si.createTable(conn, tableName)
}

func (si *SchemaInfo) createTable(conn *DBConnection, tableName string) {
	tables, err := conn.ExecuteFetch([]byte(fmt.Sprintf("%s and table_name = '%s'", base_show_tables, tableName)), 1, false)
	if err != nil {
		panic(NewTabletError(FAIL, "Error fetching table %s: %v", tableName, err))
	}
	if len(tables.Rows) != 1 {
		panic(NewTabletError(FAIL, "rows for %s !=1: %v", tableName, len(tables.Rows)))
	}
	tableInfo := NewTableInfo(
		conn,
		tableName,
		tables.Rows[0][1].(string), // table_type
		tables.Rows[0][2],          // create_time
		tables.Rows[0][3].(string), // table_comment
		si.cachePool,
	)
	if tableInfo == nil {
		panic(NewTabletError(FATAL, "Could not read table info: %s", tableName))
	}
	if tableInfo.CacheType != 0 {
		relog.Info("Initialized cached table: %s", tableInfo.Cache.prefix)
	} else {
		relog.Info("Initialized table: %s", tableName)
	}
	si.mu.Lock()
	defer si.mu.Unlock()
	if _, ok := si.tables[tableName]; ok {
		panic(NewTabletError(FAIL, "Table %s already exists", tableName))
	}
	si.tables[tableName] = tableInfo
}

func (si *SchemaInfo) DropTable(tableName string) {
	si.mu.Lock()
	defer si.mu.Unlock()
	delete(si.tables, tableName)
	si.queries.Clear()
	relog.Info("Table %s forgotten", tableName)
}

func (si *SchemaInfo) GetPlan(sql string, mustCache bool) (plan *ExecPlan) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if plan := si.getQuery(sql); plan != nil {
		return plan
	}

	var tableInfo *TableInfo
	GetTable := func(tableName string) (table *schema.Table, ok bool) {
		tableInfo, ok = si.tables[tableName]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	splan, err := sqlparser.ExecParse(sql, GetTable)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	plan = &ExecPlan{splan, tableInfo, nil}
	if plan.PlanId.IsSelect() && plan.ColumnNumbers != nil {
		plan.Fields = applyFieldFilter(plan.ColumnNumbers, tableInfo.Fields)
	}
	if plan.PlanId == sqlparser.PLAN_DDL {
		return plan
	}
	if mustCache {
		si.queries.Set(sql, plan)
	}
	return plan
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It also just returns the parsed query.
func (si *SchemaInfo) GetStreamPlan(sql string) *sqlparser.ParsedQuery {
	fullQuery, err := sqlparser.StreamExecParse(sql)
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	return fullQuery
}

func (si *SchemaInfo) SetFields(sql string, plan *ExecPlan, fields []proto.Field) {
	si.mu.Lock()
	defer si.mu.Unlock()
	newPlan := &ExecPlan{plan.ExecPlan, plan.TableInfo, fields}
	si.queries.Set(sql, newPlan)
}

func (si *SchemaInfo) GetTable(tableName string) *TableInfo {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.tables[tableName]
}

func (si *SchemaInfo) getQuery(sql string) *ExecPlan {
	if cacheResult, ok := si.queries.Get(sql); ok {
		return cacheResult.(*ExecPlan)
	}
	return nil
}

func (si *SchemaInfo) SetQueryCacheSize(size int) {
	if size <= 0 {
		panic(NewTabletError(FAIL, "cache size %v out of range", size))
	}
	si.queryCacheSize = size
	si.queries.SetCapacity(uint64(size))
}

func (si *SchemaInfo) SetReloadTime(reloadTime time.Duration) {
	si.reloadTime = reloadTime
	si.ticks.Trigger()
	si.ticks.SetInterval(reloadTime)
}

func (si *SchemaInfo) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/debug/schema/query_cache" {
		keys := si.queries.Keys()
		response.Header().Set("Content-Type", "text/plain")
		if keys == nil {
			response.Write([]byte("empty\n"))
			return
		}
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%s\n", v)))
			if plan := si.getQuery(v); plan != nil {
				if b, err := json.MarshalIndent(plan.ExecPlan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
					response.Write(([]byte)("\n\n"))
				}
			}
		}
	} else if request.URL.Path == "/debug/schema/tables" {
		response.Header().Set("Content-Type", "text/plain")
		si.mu.Lock()
		tstats := make(map[string]struct{ hits, absent, misses, invalidations int64 })
		var temp, totals struct{ hits, absent, misses, invalidations int64 }
		for k, v := range si.tables {
			if v.CacheType != 0 {
				temp.hits, temp.absent, temp.misses, temp.invalidations = v.Stats()
				tstats[k] = temp
				totals.hits += temp.hits
				totals.absent += temp.absent
				totals.misses += temp.misses
				totals.invalidations += temp.invalidations
			}
		}
		si.mu.Unlock()
		response.Write([]byte("{\n"))
		for k, v := range tstats {
			fmt.Fprintf(response, "\"%s\": {\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v},\n", k, v.hits, v.absent, v.misses, v.invalidations)
		}
		fmt.Fprintf(response, "\"Totals\": {\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v}\n", totals.hits, totals.absent, totals.misses, totals.invalidations)
		response.Write([]byte("}\n"))
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}

// Convenience functions
func applyFieldFilter(columnNumbers []int, input []proto.Field) (output []proto.Field) {
	output = make([]proto.Field, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		}
		output[colIndex] = input[colPointer]
	}
	return output
}

func applyFilter(columnNumbers []int, input []interface{}) (output []interface{}) {
	output = make([]interface{}, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		}
	}
	return output
}
