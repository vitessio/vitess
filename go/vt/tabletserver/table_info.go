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
	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/schema"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TableInfo struct {
	sync.RWMutex
	*schema.Table
	Cache  *RowCache
	Fields []mysql.Field
	// stats updated by sqlquery.go
	hits, misses int64
}

func NewTableInfo(conn *DBConnection, tableName string, cachePool *CachePool) (self *TableInfo) {
	if tableName == "dual" {
		return &TableInfo{Table: schema.NewTable(tableName)}
	}
	self = loadTableInfo(conn, tableName)
	self.initRowCache(conn, cachePool)
	return self
}

func loadTableInfo(conn *DBConnection, tableName string) (self *TableInfo) {
	self = &TableInfo{Table: schema.NewTable(tableName)}
	if !self.fetchColumns(conn) {
		return nil
	}
	if !self.fetchIndexes(conn) {
		return nil
	}
	return self
}

func (self *TableInfo) fetchColumns(conn *DBConnection) bool {
	columns, err := conn.ExecuteFetch([]byte(fmt.Sprintf("describe %s", self.Name)), 10000)
	if err != nil {
		relog.Warning("%s", err.Error())
		return false
	}
	for _, row := range columns.Rows {
		self.AddColumn(row[0].(string), row[1].(string))
	}
	return true
}

func (self *TableInfo) fetchIndexes(conn *DBConnection) bool {
	indexes, err := conn.ExecuteFetch([]byte(fmt.Sprintf("show index from %s", self.Name)), 10000)
	if err != nil {
		relog.Warning("%s", err.Error())
		return false
	}
	var currentIndex *schema.Index
	currentName := ""
	for _, row := range indexes.Rows {
		indexName := row[2].(string)
		if currentName != indexName {
			currentIndex = self.AddIndex(indexName)
			currentName = indexName
		}
		currentIndex.AddColumn(row[4].(string))
	}
	if len(self.Indexes) == 0 {
		return true
	}
	pkIndex := self.Indexes[0]
	if pkIndex.Name != "PRIMARY" {
		return true
	}
	self.PKColumns = make([]int, len(pkIndex.Columns))
	for i, pkCol := range pkIndex.Columns {
		self.PKColumns[i] = self.FindColumn(pkCol)
	}
	return true
}

const base_show_tables = "select table_type, create_time, table_comment from information_schema.tables where table_schema = database()"

func (self *TableInfo) initRowCache(conn *DBConnection, cachePool *CachePool) {
	if cachePool.IsClosed() {
		return
	}

	metaRows, err := conn.ExecuteFetch([]byte(fmt.Sprintf("%s and table_name = '%s'", base_show_tables, self.Name)), 1)
	if err != nil {
		relog.Warning("Failed to fetch table info for %s, table will not be cached: %s", self.Name, err.Error())
		return
	}
	if len(metaRows.Rows) != 1 {
		panic(err)
	}
	metaRow := metaRows.Rows[0]

	comment := metaRow[2].(string)
	if strings.Contains(comment, "vtocc_nocache") {
		relog.Info("%s commented as vtocc_nocache. Will not be cached.", self.Name)
		return
	}

	tableType := metaRow[0].(string)
	if tableType == "VIEW" {
		relog.Info("%s is a view. Will not be cached.", self.Name)
		return
	}

	createTime := metaRow[1]
	if createTime == nil {
		relog.Warning("%s has no time stamp. Will not be cached.", self.Name)
		return
	}
	ts, err := time.Parse("2006-01-02 15:04:05", createTime.(string))
	if err != nil {
		relog.Warning("Time read error %v. Will not be cached.", err, self.Name)
		return
	}
	if self.PKColumns == nil {
		relog.Info("Table %s has no primary key. Will not be cached.", self.Name)
		return
	}
	for col := range self.PKColumns {
		if self.ColumnCategory[col] == schema.CAT_OTHER {
			relog.Info("Table %s pk has unsupported column types. Will not be cached.", self.Name)
		}
	}
	rowInfo, err := conn.ExecuteFetch([]byte(fmt.Sprintf("select * from %s where 1!=1", self.Name)), 10000)
	if err != nil {
		relog.Warning("Failed to fetch column info for %s, table will not be cached: %s", self.Name, err.Error())
		return
	}
	self.Fields = rowInfo.Fields
	self.CacheType = 1
	self.Cache = NewRowCache(self.Name, ts, cachePool)
}

func (self *TableInfo) StatsJSON() string {
	if self.Cache == nil {
		return fmt.Sprintf("null")
	}
	return fmt.Sprintf("{\"Hits\": %v, \"Misses\": %v}",
		&self.hits,
		&self.misses,
	)
}

func (self *TableInfo) Stats() (hits, misses int64) {
	return atomic.LoadInt64(&self.hits), atomic.LoadInt64(&self.misses)
}
