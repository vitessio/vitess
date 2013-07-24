// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/schema"
)

type TableInfo struct {
	*schema.Table
	Cache *RowCache
	// stats updated by sqlquery.go
	hits, absent, misses, invalidations sync2.AtomicInt64
}

func NewTableInfo(conn PoolConnection, tableName string, tableType string, createTime sqltypes.Value, comment string, cachePool *CachePool) (ti *TableInfo) {
	if tableName == "dual" {
		return &TableInfo{Table: schema.NewTable(tableName)}
	}
	ti = loadTableInfo(conn, tableName)
	ti.initRowCache(conn, tableType, createTime, comment, cachePool)
	return ti
}

func loadTableInfo(conn PoolConnection, tableName string) (ti *TableInfo) {
	ti = &TableInfo{Table: schema.NewTable(tableName)}
	if !ti.fetchColumns(conn) {
		return nil
	}
	if !ti.fetchIndexes(conn) {
		return nil
	}
	return ti
}

func (ti *TableInfo) fetchColumns(conn PoolConnection) bool {
	columns, err := conn.ExecuteFetch(fmt.Sprintf("describe %s", ti.Name), 10000, false)
	if err != nil {
		relog.Warning("%s", err.Error())
		return false
	}
	for _, row := range columns.Rows {
		ti.AddColumn(row[0].String(), row[1].String(), row[4], row[5].String())
	}
	return true
}

func (ti *TableInfo) SetPK(colnames []string) error {
	pkIndex := schema.NewIndex("PRIMARY")
	colnums := make([]int, len(colnames))
	for i, colname := range colnames {
		colnums[i] = ti.FindColumn(colname)
		if colnums[i] == -1 {
			return fmt.Errorf("column %s not found", colname)
		}
		pkIndex.AddColumn(colname, 1)
	}
	for _, col := range ti.Columns {
		pkIndex.DataColumns = append(pkIndex.DataColumns, col.Name)
	}
	if len(ti.Indexes) == 0 {
		ti.Indexes = make([]*schema.Index, 1)
	} else if ti.Indexes[0].Name != "PRIMARY" {
		ti.Indexes = append(ti.Indexes, nil)
		copy(ti.Indexes[1:], ti.Indexes[:len(ti.Indexes)-1])
	} // else we replace the currunt primary key
	ti.Indexes[0] = pkIndex
	ti.PKColumns = colnums
	return nil
}

func (ti *TableInfo) fetchIndexes(conn PoolConnection) bool {
	indexes, err := conn.ExecuteFetch(fmt.Sprintf("show index from %s", ti.Name), 10000, false)
	if err != nil {
		relog.Warning("%s", err.Error())
		return false
	}
	var currentIndex *schema.Index
	currentName := ""
	for _, row := range indexes.Rows {
		indexName := row[2].String()
		if currentName != indexName {
			currentIndex = ti.AddIndex(indexName)
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
	if len(ti.Indexes) == 0 {
		return true
	}
	pkIndex := ti.Indexes[0]
	if pkIndex.Name != "PRIMARY" {
		return true
	}
	ti.PKColumns = make([]int, len(pkIndex.Columns))
	for i, pkCol := range pkIndex.Columns {
		ti.PKColumns[i] = ti.FindColumn(pkCol)
	}
	// Primary key contains all table columns
	for _, col := range ti.Columns {
		pkIndex.DataColumns = append(pkIndex.DataColumns, col.Name)
	}
	// Secondary indices contain all primary key columns
	for i := 1; i < len(ti.Indexes); i++ {
		ti.Indexes[i].DataColumns = pkIndex.Columns
	}
	return true
}

func (ti *TableInfo) initRowCache(conn PoolConnection, tableType string, createTime sqltypes.Value, comment string, cachePool *CachePool) {
	if cachePool.IsClosed() {
		return
	}

	if strings.Contains(comment, "vtocc_nocache") {
		relog.Info("%s commented as vtocc_nocache. Will not be cached.", ti.Name)
		return
	}

	if tableType == "VIEW" {
		relog.Info("%s is a view. Will not be cached.", ti.Name)
		return
	}

	if ti.PKColumns == nil {
		relog.Info("Table %s has no primary key. Will not be cached.", ti.Name)
		return
	}
	for _, col := range ti.PKColumns {
		if ti.Columns[col].Category == schema.CAT_OTHER {
			relog.Info("Table %s pk has unsupported column types. Will not be cached.", ti.Name)
			return
		}
	}

	ti.CacheType = schema.CACHE_RW
	ti.Cache = NewRowCache(ti, cachePool)
}

func (ti *TableInfo) StatsJSON() string {
	if ti.Cache == nil {
		return fmt.Sprintf("null")
	}
	h, a, m, i := ti.Stats()
	return fmt.Sprintf("{\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v}", h, a, m, i)
}

func (ti *TableInfo) Stats() (hits, absent, misses, invalidations int64) {
	return ti.hits.Get(), ti.absent.Get(), ti.misses.Get(), ti.invalidations.Get()
}
