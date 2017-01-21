// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	querypb "github.com/youtube/vitess/go/vt/proto/query"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// TableInfo contains the tabletserver related info for a table.
// It's a superset of schema.Table.
type TableInfo struct {
	*schema.Table

	// Seq must be locked before accessing the sequence vars.
	// If CurVal==LastVal, we have to cache new values.
	Seq     sync.Mutex
	NextVal int64
	LastVal int64

	// MessageInfo contains info for message tables.
	MessageInfo *MessageInfo
}

// MessageInfo contains info specific to message tables.
type MessageInfo struct {
	// IDPKIndex is the index of the ID column
	// in PKvalues. This is used to extract the ID
	// value for message tables to discard items
	// from the cache.
	IDPKIndex int

	// Fields stores the field info to be
	// returned for subscribers.
	Fields []*querypb.Field

	// AckWaitDuration specifies how long to wait after
	// the message was first sent. The back-off doubles
	// every attempt.
	AckWaitDuration time.Duration

	// PurgeAfterDuration specifies the time after which
	// a successfully acked message can be deleted.
	PurgeAfterDuration time.Duration

	// BatchSize specifies the max number of events to
	// send per response.
	BatchSize int

	// CacheSize specifies the number of messages to keep
	// in cache. Anything that cannot fit in the cache
	// is sent as best effort.
	CacheSize int

	// PollInterval specifies the polling frequency to
	// look for messages to be sent.
	PollInterval time.Duration
}

// NewTableInfo creates a new TableInfo.
func NewTableInfo(conn *DBConn, tableName string, tableType string, comment string) (ti *TableInfo, err error) {
	ti, err = loadTableInfo(conn, tableName)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.Contains(comment, "vitess_sequence"):
		ti.Type = schema.Sequence
	case strings.Contains(comment, "vitess_message"):
		if err := ti.loadMessageInfo(comment); err != nil {
			return nil, err
		}
		ti.Type = schema.Message
	}
	return ti, nil
}

func loadTableInfo(conn *DBConn, tableName string) (ti *TableInfo, err error) {
	ti = &TableInfo{Table: schema.NewTable(tableName)}
	sqlTableName := sqlparser.String(ti.Name)
	if err = ti.fetchColumns(conn, sqlTableName); err != nil {
		return nil, err
	}
	if err = ti.fetchIndexes(conn, sqlTableName); err != nil {
		return nil, err
	}
	return ti, nil
}

func (ti *TableInfo) fetchColumns(conn *DBConn, sqlTableName string) error {
	qr, err := conn.Exec(localContext(), fmt.Sprintf("select * from %s where 1 != 1", sqlTableName), 0, true)
	if err != nil {
		return err
	}
	fieldTypes := make(map[string]querypb.Type, len(qr.Fields))
	// TODO(sougou): Store the full field info in the schema.
	for _, field := range qr.Fields {
		fieldTypes[field.Name] = field.Type
	}
	columns, err := conn.Exec(localContext(), fmt.Sprintf("describe %s", sqlTableName), 10000, false)
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
		colnums[i] = ti.FindColumn(sqlparser.NewColIdent(colname))
		if colnums[i] == -1 {
			return fmt.Errorf("column %s not found", colname)
		}
		pkIndex.AddColumn(colname, 1)
	}
	for _, col := range ti.Columns {
		pkIndex.DataColumns = append(pkIndex.DataColumns, col.Name)
	}
	ti.Indexes = append(ti.Indexes, nil)
	copy(ti.Indexes[1:], ti.Indexes[:len(ti.Indexes)-1])
	ti.Indexes[0] = pkIndex
	ti.PKColumns = colnums
	return nil
}

func (ti *TableInfo) fetchIndexes(conn *DBConn, sqlTableName string) error {
	indexes, err := conn.Exec(localContext(), fmt.Sprintf("show index from %s", sqlTableName), 10000, false)
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
	if !ti.HasPrimary() {
		return nil
	}
	pkIndex := ti.Indexes[0]
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

func (ti *TableInfo) loadMessageInfo(comment string) error {
	findCols := []string{
		"time_scheduled",
		"id",
		"time_next",
		"epoch",
		"time_created",
		"time_acked",
		"message",
	}
	ti.MessageInfo = &MessageInfo{
		Fields: make([]*querypb.Field, 2),
	}
	// Extract keyvalues.
	keyvals := make(map[string]string)
	inputs := strings.Split(comment, ",")
	for _, input := range inputs {
		kv := strings.Split(input, "=")
		if len(kv) != 2 {
			continue
		}
		keyvals[kv[0]] = kv[1]
	}
	var err error
	if ti.MessageInfo.AckWaitDuration, err = getDuration(keyvals, "vt_ack_wait"); err != nil {
		return err
	}
	if ti.MessageInfo.PurgeAfterDuration, err = getDuration(keyvals, "vt_purge_after"); err != nil {
		return err
	}
	if ti.MessageInfo.BatchSize, err = getNum(keyvals, "vt_batch_size"); err != nil {
		return err
	}
	if ti.MessageInfo.CacheSize, err = getNum(keyvals, "vt_cache_size"); err != nil {
		return err
	}
	if ti.MessageInfo.PollInterval, err = getDuration(keyvals, "vt_poller_interval"); err != nil {
		return err
	}
	for _, col := range findCols {
		num := ti.FindColumn(sqlparser.NewColIdent(col))
		if num == -1 {
			return fmt.Errorf("%s missing from message table: %s", col, ti.Name.String())
		}
		switch col {
		case "id":
			ti.MessageInfo.Fields[0] = &querypb.Field{
				Name: ti.Columns[num].Name.String(),
				Type: ti.Columns[num].Type,
			}
		case "message":
			ti.MessageInfo.Fields[1] = &querypb.Field{
				Name: ti.Columns[num].Name.String(),
				Type: ti.Columns[num].Type,
			}
		}
	}
	for i, j := range ti.PKColumns {
		if ti.Columns[j].Name.EqualString("id") {
			ti.MessageInfo.IDPKIndex = i
			return nil
		}
	}
	return fmt.Errorf("id column is not part of the primary key for message table: %s", ti.Name.String())
}

func getDuration(in map[string]string, key string) (time.Duration, error) {
	sv := in[key]
	if sv == "" {
		return 0, fmt.Errorf("Attribute %s not specified for message table", key)
	}
	v, err := strconv.ParseFloat(sv, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(v * 1e9), nil
}

func getNum(in map[string]string, key string) (int, error) {
	sv := in[key]
	if sv == "" {
		return 0, fmt.Errorf("Attribute %s not specified for message table", key)
	}
	v, err := strconv.Atoi(sv)
	if err != nil {
		return 0, err
	}
	return v, nil
}
