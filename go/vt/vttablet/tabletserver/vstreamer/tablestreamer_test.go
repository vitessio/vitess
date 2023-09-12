/*
Copyright 2023 The Vitess Authors.

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

package vstreamer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestTableStreamer streams all tables and ensures all rows are received in the correct order.
func TestTableStreamer(t *testing.T) {
	ctx := context.Background()
	execStatements(t, []string{
		// Single PK
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa'), (2, 'bbb')",
		// Composite PK
		"create table t2(id1 int, id2 int, val varbinary(128), primary key(id1, id2))",
		"insert into t2 values (1, 2, 'aaa'), (1, 3, 'bbb')",
		// No PK
		"create table t3(id int, val varbinary(128))",
		"insert into t3 values (1, 'aaa'), (2, 'bbb')",
		// Three-column PK
		"create table t4(id1 int, id2 int, id3 int, val varbinary(128), primary key(id1, id2, id3))",
		"insert into t4 values (1, 2, 3, 'aaa'), (2, 3, 4, 'bbb')",
	})

	defer execStatements(t, []string{
		"drop table t1",
		"drop table t2",
		"drop table t3",
		"drop table t4",
	})

	engine.se.Reload(context.Background())

	wantStream := []string{
		"table_name:\"t1\" fields:{name:\"id\" type:INT32 table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"id\" column_length:11 charset:63 flags:53251} fields:{name:\"val\" type:VARBINARY table:\"t1\" org_table:\"t1\" database:\"vttest\" org_name:\"val\" column_length:128 charset:63 flags:128} pkfields:{name:\"id\" type:INT32 charset:63 flags:53251}",
		"table_name:\"t1\" rows:{lengths:1 lengths:3 values:\"1aaa\"} rows:{lengths:1 lengths:3 values:\"2bbb\"} lastpk:{lengths:1 values:\"2\"}",
		"table_name:\"t2\" fields:{name:\"id1\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id1\" column_length:11 charset:63 flags:53251} fields:{name:\"id2\" type:INT32 table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"id2\" column_length:11 charset:63 flags:53251} fields:{name:\"val\" type:VARBINARY table:\"t2\" org_table:\"t2\" database:\"vttest\" org_name:\"val\" column_length:128 charset:63 flags:128} pkfields:{name:\"id1\" type:INT32 charset:63 flags:53251} pkfields:{name:\"id2\" type:INT32 charset:63 flags:53251}",
		"table_name:\"t2\" rows:{lengths:1 lengths:1 lengths:3 values:\"12aaa\"} rows:{lengths:1 lengths:1 lengths:3 values:\"13bbb\"} lastpk:{lengths:1 lengths:1 values:\"13\"}",
		"table_name:\"t3\" fields:{name:\"id\" type:INT32 table:\"t3\" org_table:\"t3\" database:\"vttest\" org_name:\"id\" column_length:11 charset:63 flags:32768} fields:{name:\"val\" type:VARBINARY table:\"t3\" org_table:\"t3\" database:\"vttest\" org_name:\"val\" column_length:128 charset:63 flags:128} pkfields:{name:\"id\" type:INT32 charset:63 flags:32768} pkfields:{name:\"val\" type:VARBINARY charset:63 flags:128}",
		"table_name:\"t3\" rows:{lengths:1 lengths:3 values:\"1aaa\"} rows:{lengths:1 lengths:3 values:\"2bbb\"} lastpk:{lengths:1 lengths:3 values:\"2bbb\"}",
		"table_name:\"t4\" fields:{name:\"id1\" type:INT32 table:\"t4\" org_table:\"t4\" database:\"vttest\" org_name:\"id1\" column_length:11 charset:63 flags:53251} fields:{name:\"id2\" type:INT32 table:\"t4\" org_table:\"t4\" database:\"vttest\" org_name:\"id2\" column_length:11 charset:63 flags:53251} fields:{name:\"id3\" type:INT32 table:\"t4\" org_table:\"t4\" database:\"vttest\" org_name:\"id3\" column_length:11 charset:63 flags:53251} fields:{name:\"val\" type:VARBINARY table:\"t4\" org_table:\"t4\" database:\"vttest\" org_name:\"val\" column_length:128 charset:63 flags:128} pkfields:{name:\"id1\" type:INT32 charset:63 flags:53251} pkfields:{name:\"id2\" type:INT32 charset:63 flags:53251} pkfields:{name:\"id3\" type:INT32 charset:63 flags:53251}",
		"table_name:\"t4\" rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:\"123aaa\"} rows:{lengths:1 lengths:1 lengths:1 lengths:3 values:\"234bbb\"} lastpk:{lengths:1 lengths:1 lengths:1 values:\"234\"}",
	}
	var gotStream []string
	err := engine.StreamTables(ctx, func(response *binlogdatapb.VStreamTablesResponse) error {
		response.Gtid = ""
		for _, fld := range response.Fields {
			fld.ColumnType = ""
		}
		gotStream = append(gotStream, fmt.Sprintf("%v", response))
		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, wantStream, gotStream)
	require.Equal(t, int64(4), engine.tableStreamerNumTables.Get())
}
