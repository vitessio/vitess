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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
)

// LoadTable creates a Table from the schema info in the database.
func LoadTable(conn *connpool.DBConn, databaseName, tableName, tableType string, comment string) (*Table, error) {
	ta := NewTable(tableName, NoType)
	sqlTableName := sqlparser.String(ta.Name)
	if err := fetchColumns(ta, conn, databaseName, sqlTableName); err != nil {
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
	case strings.Contains(tableType, "VIEW"):
		ta.Type = View
	}
	return ta, nil
}

func fetchColumns(ta *Table, conn *connpool.DBConn, databaseName, sqlTableName string) error {
	ctx := context.Background()
	exec := func(query string, maxRows int, wantFields bool) (*sqltypes.Result, error) {
		return conn.Exec(ctx, query, maxRows, wantFields)
	}
	fields, _, err := mysqlctl.GetColumns(databaseName, sqlTableName, exec)
	if err != nil {
		return err
	}
	ta.Fields = fields
	return nil
}

func loadMessageInfo(ta *Table, comment string) error {
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

	// these columns are required for message manager to function properly, but only
	// id is required to be streamed to subscribers
	requiredCols := []string{
		"id",
		"priority",
		"time_next",
		"epoch",
		"time_acked",
	}

	// by default, these columns are loaded for the message manager, but not sent to subscribers
	// via stream * from msg_tbl
	hiddenCols := map[string]struct{}{
		"priority":   {},
		"time_next":  {},
		"epoch":      {},
		"time_acked": {},
	}

	// make sure required columns exist in the table schema
	for _, col := range requiredCols {
		num := ta.FindColumn(sqlparser.NewIdentifierCI(col))
		if num == -1 {
			return fmt.Errorf("%s missing from message table: %s", col, ta.Name.String())
		}
	}

	// check to see if the user has specified columns to stream to subscribers
	specifiedCols := parseMessageCols(keyvals, "vt_message_cols")

	if len(specifiedCols) > 0 {
		// make sure that all the specified columns exist in the table schema
		for _, col := range specifiedCols {
			num := ta.FindColumn(sqlparser.NewIdentifierCI(col))
			if num == -1 {
				return fmt.Errorf("%s missing from message table: %s", col, ta.Name.String())
			}
		}

		// the original implementation in message_manager assumes id is the first column, as originally users
		// could not restrict columns. As the PK, id is required, and by requiring it as the first column,
		// we avoid the need to change the implementation.
		if specifiedCols[0] != "id" {
			return fmt.Errorf("vt_message_cols must begin with id: %s", ta.Name.String())
		}
		ta.MessageInfo.Fields = getSpecifiedMessageFields(ta.Fields, specifiedCols)
	} else {
		ta.MessageInfo.Fields = getDefaultMessageFields(ta.Fields, hiddenCols)
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

// parseMessageCols parses the vt_message_cols attribute. It doesn't error out if the attribute is not specified
// because the default behavior is to stream all columns to subscribers, and if done incorrectly, later checks
// to see if the columns exist in the table schema will fail.
func parseMessageCols(in map[string]string, key string) []string {
	sv := in[key]
	cols := strings.Split(sv, "|")
	if len(cols) == 1 && strings.TrimSpace(cols[0]) == "" {
		return nil
	}
	return cols
}

func getDefaultMessageFields(tableFields []*querypb.Field, hiddenCols map[string]struct{}) []*querypb.Field {
	fields := make([]*querypb.Field, 0, len(tableFields))
	// Load user-defined columns. Any "unrecognized" column is user-defined.
	for _, field := range tableFields {
		if _, ok := hiddenCols[strings.ToLower(field.Name)]; ok {
			continue
		}

		fields = append(fields, field)
	}
	return fields
}

// we have already validated that all the specified columns exist in the table schema, so we don't need to
// check again and possibly return an error here.
func getSpecifiedMessageFields(tableFields []*querypb.Field, specifiedCols []string) []*querypb.Field {
	fields := make([]*querypb.Field, 0, len(specifiedCols))
	for _, col := range specifiedCols {
		for _, field := range tableFields {
			if res, _ := evalengine.NullsafeCompare(sqltypes.NewVarChar(field.Name), sqltypes.NewVarChar(strings.TrimSpace(col)), collations.Default()); res == 0 {
				fields = append(fields, field)
				break
			}
		}
	}
	return fields
}
