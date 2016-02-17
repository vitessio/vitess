// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
	"golang.org/x/net/context"
)

// TableInfo contains the tabletserver related info for a table.
// It's a superset of schema.Table.
type TableInfo struct {
	*schema.Table
	Cache *RowCache
	// rowcache stats updated by query_executor.go and query_engine.go.
	hits, absent, misses, invalidations sync2.AtomicInt64
}

// NewTableInfo creates a new TableInfo.
func NewTableInfo(conn *DBConn, tableName string, tableType string, comment string, cachePool *CachePool) (ti *TableInfo, err error) {
	ti, err = loadTableInfo(conn, tableName)
	if err != nil {
		return nil, err
	}
	ti.initRowCache(conn, tableType, comment, cachePool)
	return ti, nil
}

func loadTableInfo(conn *DBConn, tableName string) (ti *TableInfo, err error) {
	ti = &TableInfo{Table: schema.NewTable(tableName)}
	if err = ti.fetchColumns(conn); err != nil {
		return nil, err
	}
	if err = ti.fetchIndexes(conn); err != nil {
		return nil, err
	}
	return ti, nil
}

func (ti *TableInfo) fetchColumns(conn *DBConn) error {
	qr, err := conn.Exec(context.Background(), fmt.Sprintf("select * from `%s` where 1 != 1", ti.Name), 10000, true)
	if err != nil {
		return err
	}
	fieldTypes := make(map[string]querypb.Type, len(qr.Fields))
	for _, field := range qr.Fields {
		fieldTypes[field.Name] = field.Type
	}
	columns, err := conn.Exec(context.Background(), fmt.Sprintf("describe `%s`", ti.Name), 10000, false)
	if err != nil {
		return err
	}
	for _, row := range columns.Rows {
		name := row[0].String()
		columnType, ok := fieldTypes[name]
		if !ok {
			log.Warningf("Table: %s, column %s not found in select list, skipping.", ti.Name, name)
			continue
		}
		ti.AddColumn(name, columnType, row[4], row[5].String())
	}
	return nil
}

// SetPK sets the pk columns for a TableInfo.
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

func (ti *TableInfo) fetchIndexes(conn *DBConn) error {
	indexes, err := conn.Exec(context.Background(), fmt.Sprintf("show index from `%s`", ti.Name), 10000, false)
	if err != nil {
		return err
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
				log.Warningf("%s", err)
			}
		}
		currentIndex.AddColumn(row[4].String(), cardinality)
	}
	if len(ti.Indexes) == 0 {
		return nil
	}
	pkIndex := ti.Indexes[0]
	if pkIndex.Name != "PRIMARY" {
		return nil
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
		for _, c := range ti.Indexes[i].Columns {
			ti.Indexes[i].DataColumns = append(ti.Indexes[i].DataColumns, c)
		}
		for _, c := range pkIndex.Columns {
			// pk columns may already be part of the index. So,
			// check before adding.
			if ti.Indexes[i].FindDataColumn(c) != -1 {
				continue
			}
			ti.Indexes[i].DataColumns = append(ti.Indexes[i].DataColumns, c)
		}
	}
	return nil
}

func (ti *TableInfo) initRowCache(conn *DBConn, tableType string, comment string, cachePool *CachePool) {
	if cachePool.IsClosed() {
		return
	}

	if strings.Contains(comment, "vitess_nocache") {
		log.Infof("%s commented as vitess_nocache. Will not be cached.", ti.Name)
		return
	}

	if tableType == "VIEW" {
		log.Infof("%s is a view. Will not be cached.", ti.Name)
		return
	}

	if ti.PKColumns == nil {
		log.Infof("Table %s has no primary key. Will not be cached.", ti.Name)
		return
	}
	for _, col := range ti.PKColumns {
		if sqltypes.IsIntegral(ti.Columns[col].Type) || ti.Columns[col].Type == sqltypes.VarBinary {
			continue
		}
		log.Infof("Table %s pk has unsupported column types. Will not be cached.", ti.Name)
		return
	}

	ti.CacheType = schema.CacheRW
	ti.Cache = NewRowCache(ti, cachePool)
}

// StatsJSON returns a JSON representation of the TableInfo stats.
func (ti *TableInfo) StatsJSON() string {
	if ti.Cache == nil {
		return fmt.Sprintf("null")
	}
	h, a, m, i := ti.Stats()
	return fmt.Sprintf("{\"Hits\": %v, \"Absent\": %v, \"Misses\": %v, \"Invalidations\": %v}", h, a, m, i)
}

// Stats returns the stats for TableInfo.
func (ti *TableInfo) Stats() (hits, absent, misses, invalidations int64) {
	return ti.hits.Get(), ti.absent.Get(), ti.misses.Get(), ti.invalidations.Get()
}
