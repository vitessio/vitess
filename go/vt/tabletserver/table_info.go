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
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync/atomic"
)

var hashRegistry map[string]string = make(map[string]string)

type TableInfo struct {
	*schema.Table
	Cache  *RowCache
	Fields []mysql.Field
	// stats updated by sqlquery.go
	hits, absent, misses, invalidations int64
}

func NewTableInfo(conn *DBConnection, tableName string, tableType string, createTime interface{}, comment string, cachePool *CachePool) (self *TableInfo) {
	if tableName == "dual" {
		return &TableInfo{Table: schema.NewTable(tableName)}
	}
	self = loadTableInfo(conn, tableName)
	self.initRowCache(conn, tableType, createTime, comment, cachePool)
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
		cardinality, err := strconv.ParseUint(row[6].(string), 0, 64)
		if err != nil {
			relog.Warning("%s", err)
		}
		currentIndex.AddColumn(row[4].(string), cardinality)
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
	// Primary key contains all table columns
	pkIndex.DataColumns = self.Columns
	// Secondary indices contain all primary key columns
	for i := 1; i < len(self.Indexes); i++ {
		self.Indexes[i].DataColumns = pkIndex.Columns
	}
	return true
}

func (self *TableInfo) initRowCache(conn *DBConnection, tableType string, createTime interface{}, comment string, cachePool *CachePool) {
	if cachePool.IsClosed() {
		return
	}

	if strings.Contains(comment, "vtocc_nocache") {
		relog.Info("%s commented as vtocc_nocache. Will not be cached.", self.Name)
		return
	}

	if tableType == "VIEW" {
		relog.Info("%s is a view. Will not be cached.", self.Name)
		return
	}

	if self.PKColumns == nil {
		relog.Info("Table %s has no primary key. Will not be cached.", self.Name)
		return
	}
	for col := range self.PKColumns {
		if self.ColumnCategory[col] == schema.CAT_OTHER {
			relog.Info("Table %s pk has unsupported column types. Will not be cached.", self.Name)
			return
		}
	}

	rowInfo, err := conn.ExecuteFetch([]byte(fmt.Sprintf("select * from %s where 1!=1", self.Name)), 10000)
	if err != nil {
		relog.Warning("Failed to fetch column info for %s, table will not be cached: %s", self.Name, err.Error())
		return
	}
	thash := self.computePrefix(conn, createTime)
	if thash == "" {
		return
	}

	self.Fields = rowInfo.Fields
	self.CacheType = 1
	self.Cache = NewRowCache(self.Name, thash, cachePool)
}

func (self *TableInfo) computePrefix(conn *DBConnection, createTime interface{}) string {
	if createTime == nil {
		relog.Warning("%s has no time stamp. Will not be cached.", self.Name)
		return ""
	}
	createTable, err := conn.ExecuteFetch([]byte(fmt.Sprintf("show create table %s", self.Name)), 10000)
	if err != nil {
		relog.Warning("Couldnt read table info: %v", err)
		return ""
	}
	thash := base64fnv(createTable.Rows[0][1].(string) + createTime.(string))
	if _, ok := hashRegistry[thash]; ok {
		relog.Warning("Hash collision for %s (schema revert?). Will not be cached", self.Name)
		return ""
	}
	hashRegistry[thash] = self.Name
	return thash
}

func (self *TableInfo) StatsJSON() string {
	if self.Cache == nil {
		return fmt.Sprintf("null")
	}
	h, a, m, i := self.Stats()
	return fmt.Sprintf("{\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v}", h, a, m, i)
}

func (self *TableInfo) Stats() (hits, absent, misses, invalidations int64) {
	return atomic.LoadInt64(&self.hits), atomic.LoadInt64(&self.absent), atomic.LoadInt64(&self.misses), atomic.LoadInt64(&self.invalidations)
}

func base64fnv(s string) string {
	h := fnv.New32a()
	h.Write(([]byte)(s))
	v := h.Sum32()
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	b64 := make([]byte, base64.StdEncoding.EncodedLen(len(b)))
	base64.StdEncoding.Encode(b64, b)
	for b64[len(b64)-1] == '=' {
		b64 = b64[:len(b64)-1]
	}
	return string(b64)
}
