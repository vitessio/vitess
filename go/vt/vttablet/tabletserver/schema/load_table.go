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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// LoadTable creates a Table from the schema info in the database.
func LoadTable(conn *connpool.DBConn, tableName string, comment string) (*Table, error) {
	ta := NewTable(tableName)
	sqlTableName := sqlparser.String(ta.Name)
	if err := fetchColumns(ta, conn, sqlTableName); err != nil {
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

func fetchColumns(ta *Table, conn *connpool.DBConn, sqlTableName string) error {
	qr, err := conn.Exec(tabletenv.LocalContext(), fmt.Sprintf("select * from %s where 1 != 1", sqlTableName), 0, true)
	if err != nil {
		return err
	}
	ta.Fields = qr.Fields
	return nil
}

func loadMessageInfo(ta *Table, comment string) error {
	hiddenCols := map[string]struct{}{
		"priority":   {},
		"time_next":  {},
		"epoch":      {},
		"time_acked": {},
	}

	requiredCols := []string{
		"id",
		"priority",
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

	// errors are ignored because these fields are optional and 0 is the default value
	ta.MessageInfo.MinBackoff, _ = getDuration(keyvals, "vt_min_backoff")
	// the original default minimum backoff was based on ack wait timeout, so this preserves that
	if ta.MessageInfo.MinBackoff == 0 {
		ta.MessageInfo.MinBackoff = ta.MessageInfo.AckWaitDuration
	}

	ta.MessageInfo.MaxBackoff, _ = getDuration(keyvals, "vt_max_backoff")

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
