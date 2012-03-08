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
	"sync"
	"vitess/cache"
	"vitess/mysql"
	"vitess/relog"
	"vitess/vt/schema"
)

type TableInfo struct {
	sync.RWMutex
	*schema.Table
	RowCache *cache.LRUCache
	Fields   []mysql.Field
	// stats updated by sqlquery.go
	hits, misses int64
}

func NewTableInfo(conn *SmartConnection, tableName string, cacheSize uint64) (self *TableInfo) {
	self = loadTableInfo(conn, tableName)
	if cacheSize != 0 {
		self.initRowCache(conn, cacheSize)
	}
	return self
}

func loadTableInfo(conn *SmartConnection, tableName string) (self *TableInfo) {
	self = &TableInfo{Table: schema.NewTable(tableName)}
	if tableName == "dual" {
		return self
	}
	if !self.fetchColumns(conn) {
		return nil
	}
	if !self.fetchIndexes(conn) {
		return nil
	}
	return self
}

func (self *TableInfo) fetchColumns(conn *SmartConnection) bool {
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

func (self *TableInfo) fetchIndexes(conn *SmartConnection) bool {
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

func (self *TableInfo) initRowCache(conn *SmartConnection, cacheSize uint64) {
	if self.PKColumns == nil {
		relog.Warning("Table %s has no primary key. Will not be cached.", self.Name)
		return
	}
	rowInfo, err := conn.ExecuteFetch([]byte(fmt.Sprintf("select * from %s where 1!=1", self.Name)), 10000)
	if err != nil {
		relog.Warning("Failed to fetch column info for %s, table will not be cached: %s", self.Name, err.Error())
		return
	}
	self.Fields = rowInfo.Fields
	self.CacheType = 1
	self.CacheSize = cacheSize
	self.RowCache = cache.NewLRUCache(self.CacheSize)
}

func (self *TableInfo) String() string {
	if self.RowCache == nil {
		return fmt.Sprintf("{}")
	}
	return fmt.Sprintf("{\"RowCache\": %v, \"Hits\": %v, \"Misses\": %v}",
		self.RowCache,
		&self.hits,
		&self.misses,
	)
}
