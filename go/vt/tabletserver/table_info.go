// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/vt/schema"
)

type TableInfo struct {
	*schema.Table
	Cache *RowCache
	// stats updated by sqlquery.go
	hits, absent, misses, invalidations int64
}

func NewTableInfo(conn PoolConnection, tableName string, tableType string, createTime sqltypes.Value, comment string, cachePool *CachePool, hashRegistry map[string]string) (self *TableInfo) {
	if tableName == "dual" {
		return &TableInfo{Table: schema.NewTable(tableName)}
	}
	self = loadTableInfo(conn, tableName)
	self.initRowCache(conn, tableType, createTime, comment, cachePool, hashRegistry)
	return self
}

func loadTableInfo(conn PoolConnection, tableName string) (self *TableInfo) {
	self = &TableInfo{Table: schema.NewTable(tableName)}
	if !self.fetchColumns(conn) {
		return nil
	}
	if !self.fetchIndexes(conn) {
		return nil
	}
	return self
}

func (self *TableInfo) fetchColumns(conn PoolConnection) bool {
	columns, err := conn.ExecuteFetch([]byte(fmt.Sprintf("describe %s", self.Name)), 10000, false)
	if err != nil {
		relog.Warning("%s", err.Error())
		return false
	}
	for _, row := range columns.Rows {
		self.AddColumn(row[0].String(), row[1].String(), row[4], row[5].String())
	}
	return true
}

func (self *TableInfo) fetchIndexes(conn PoolConnection) bool {
	indexes, err := conn.ExecuteFetch([]byte(fmt.Sprintf("show index from %s", self.Name)), 10000, false)
	if err != nil {
		relog.Warning("%s", err.Error())
		return false
	}
	var currentIndex *schema.Index
	currentName := ""
	for _, row := range indexes.Rows {
		indexName := row[2].String()
		if currentName != indexName {
			currentIndex = self.AddIndex(indexName)
			currentName = indexName
		}
		var cardinality uint64
		if !row[6].IsNull() {
			cardinality, err = strconv.ParseUint(row[6].String(), 0, 64)
			if err != nil {
				relog.Warning("%s", err)
			}
		}
		currentIndex.AddColumn(row[4].String(), cardinality)
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
	for _, col := range self.Columns {
		pkIndex.DataColumns = append(pkIndex.DataColumns, col.Name)
	}
	// Secondary indices contain all primary key columns
	for i := 1; i < len(self.Indexes); i++ {
		self.Indexes[i].DataColumns = pkIndex.Columns
	}
	return true
}

func (self *TableInfo) initRowCache(conn PoolConnection, tableType string, createTime sqltypes.Value, comment string, cachePool *CachePool, hashRegistry map[string]string) {
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
	for _, col := range self.PKColumns {
		if self.Columns[col].Category == schema.CAT_OTHER {
			relog.Info("Table %s pk has unsupported column types. Will not be cached.", self.Name)
			return
		}
	}

	thash := self.computePrefix(conn, createTime, hashRegistry)
	if thash == "" {
		return
	}

	self.CacheType = 1
	self.Cache = NewRowCache(self, thash, cachePool)
}

var autoIncr = regexp.MustCompile("auto_increment=\\d+")

func (self *TableInfo) computePrefix(conn PoolConnection, createTime sqltypes.Value, hashRegistry map[string]string) string {
	if createTime.IsNull() {
		relog.Warning("%s has no time stamp. Will not be cached.", self.Name)
		return ""
	}
	createTable, err := conn.ExecuteFetch([]byte(fmt.Sprintf("show create table %s", self.Name)), 10000, false)
	if err != nil {
		relog.Warning("Couldnt read table info: %v", err)
		return ""
	}
	// Normalize & remove auto_increment because it changes on every insert
	norm1 := strings.ToLower(createTable.Rows[0][1].String())
	norm2 := autoIncr.ReplaceAllLiteralString(norm1, "")
	thash := base64fnv(norm2 + createTime.String())
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
