/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// LoadTable creates a Table from the schema info in the database.
func LoadTable(conn *connpool.DBConn, tableName string, tableType string, comment string) (*Table, error) {
	ta := NewTable(tableName)
	sqlTableName := sqlparser.String(ta.Name)
	if err := fetchColumns(ta, conn, sqlTableName); err != nil {
		return nil, err
	}
	if err := fetchIndexes(ta, conn, sqlTableName); err != nil {
		return nil, err
	}
	switch {
	case strings.Contains(comment, "vitess_sequence"):
		ta.Type = Sequence
		ta.SequenceInfo = &SequenceInfo{}
	case strings.Contains(comment, "vitess_message"):
		if err := loadMessageInfo(ta, comment); err != nil {
			return nil, err
		}
		ta.Type = Message
	}
	return ta, nil
}

// LoadTableBasic creates a Table with just the column info loaded.
func LoadTableBasic(conn *connpool.DBConn, tableName string) (*Table, error) {
	ta := NewTable(tableName)
	if err := fetchColumns(ta, conn, tableName); err != nil {
		return nil, err
	}
	return ta, nil
}

func fetchColumns(ta *Table, conn *connpool.DBConn, sqlTableName string) error {
	qr, err := conn.Exec(tabletenv.LocalContext(), fmt.Sprintf("select * from %s where 1 != 1", sqlTableName), 0, true)
	if err != nil {
		return err
	}
	ta.Fields = qr.Fields
	return nil
}

func fetchIndexes(ta *Table, conn *connpool.DBConn, sqlTableName string) error {
	indexes, err := conn.Exec(tabletenv.LocalContext(), fmt.Sprintf("show index from %s", sqlTableName), 10000, false)
	if err != nil {
		return err
	}
	var currentIndex *Index
	currentName := ""
	for _, row := range indexes.Rows {
		indexName := row[2].ToString()
		if currentName != indexName {
			currentIndex = ta.AddIndex(indexName, row[1].ToString() == "0")
			currentName = indexName
		}
		var cardinality uint64
		if !row[6].IsNull() {
			cardinality, err = sqltypes.ToUint64(row[6])
			if err != nil {
				log.Warningf("%s", err)
			}
		}
		currentIndex.AddColumn(row[4].ToString(), cardinality)
	}
	ta.Done()
	return nil
}

func loadMessageInfo(ta *Table, comment string) error {
	hiddenCols := map[string]struct{}{
		"time_next":  {},
		"epoch":      {},
		"time_acked": {},
	}

	requiredCols := []string{
		"id",
		"time_next",
		"epoch",
		"time_acked",
	}

	ta.MessageInfo = &MessageInfo{}
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
	ta.MessageInfo.Topic = getTopic(keyvals)

	if ta.MessageInfo.AckWaitDuration, err = getDuration(keyvals, "vt_ack_wait"); err != nil {
		return err
	}
	if ta.MessageInfo.PurgeAfterDuration, err = getDuration(keyvals, "vt_purge_after"); err != nil {
		return err
	}
	if ta.MessageInfo.BatchSize, err = getNum(keyvals, "vt_batch_size"); err != nil {
		return err
	}
	if ta.MessageInfo.CacheSize, err = getNum(keyvals, "vt_cache_size"); err != nil {
		return err
	}
	if ta.MessageInfo.PollInterval, err = getDuration(keyvals, "vt_poller_interval"); err != nil {
		return err
	}
	for _, col := range requiredCols {
		num := ta.FindColumn(sqlparser.NewColIdent(col))
		if num == -1 {
			return fmt.Errorf("%s missing from message table: %s", col, ta.Name.String())
		}
	}

	// Load user-defined columns. Any "unrecognized" column is user-defined.
	for _, field := range ta.Fields {
		if _, ok := hiddenCols[strings.ToLower(field.Name)]; ok {
			continue
		}
		ta.MessageInfo.Fields = append(ta.MessageInfo.Fields, field)
	}
	return nil
}

func getDuration(in map[string]string, key string) (time.Duration, error) {
	sv := in[key]
	if sv == "" {
		return 0, fmt.Errorf("attribute %s not specified for message table", key)
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
		return 0, fmt.Errorf("attribute %s not specified for message table", key)
	}
	v, err := strconv.Atoi(sv)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func getTopic(in map[string]string) string {
	return in["vt_topic"]
}
