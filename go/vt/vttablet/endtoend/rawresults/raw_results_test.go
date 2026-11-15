/*
Copyright 2026 The Vitess Authors.

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

package rawresults

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// queries are run with every method that returns rows.
var queries = []string{
	"select * from rawtext order by id",
	"select * from rawtext where 1 != 1",
	"select concat(l1, 'x'), upper(gbk), convert(sjis using utf8mb4), convert(l1 using ucs2), cast(u4 as char(1)), left(gb18030, 1), json_object('a', l1), j->>'$.a', j->'$.b', cast(j as char), hex(l1), e + 0 from rawtext order by id",
	"select group_concat(l1t order by id), min(gbk), max(uct), count(distinct euckr) from rawtext",
	"select l1 from rawtext union select u4b from rawtext order by 1",
	// an error is the same in both modes
	"select l1 from rawtext union select gbk from rawtext order by 1",
	"select 'Müller', _latin1 x'FC', concat(_gbk x'D6D0', 'x'), cast(null as char), n, d, dt, bt + 0, ST_AsText(g) from rawtext where id = 1",
	"show full columns from rawtext",
	"show create table rawtext",
	"select column_name, collation_name, column_default from information_schema.columns where table_name = 'rawtext' order by ordinal_position",
}

func newClient(marked bool) *framework.QueryClient {
	ctx := callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "dev"})
	if marked {
		ctx = queryservice.ContextWithRawResultCharset(ctx)
	}
	return framework.NewClientWithContext(ctx)
}

// run runs every query with every method that returns rows, and returns each
// result, or its error, by method and query.
func run(t *testing.T, marked bool) map[string]string {
	t.Helper()
	out := map[string]string{}
	record := func(method, query string, qr *sqltypes.Result, err error) {
		if err != nil {
			out[method+" "+query] = "error: " + err.Error()
			return
		}
		out[method+" "+query] = describe(qr)
	}
	for _, query := range queries {
		client := newClient(marked)
		qr, err := client.Execute(query, nil)
		record("Execute", query, qr, err)
		qr, err = client.StreamExecute(query, nil)
		record("StreamExecute", query, qr, err)

		client = newClient(marked)
		qr, err = client.BeginExecute(query, nil, nil)
		record("BeginExecute", query, qr, err)
		require.NoError(t, client.Rollback())

		client = newClient(marked)
		qr, err = client.StreamBeginExecuteWithOptions(query, nil, nil, &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL})
		record("BeginStreamExecute", query, qr, err)
		require.NoError(t, client.Rollback())

		// get_lock needs a reserved connection
		client = newClient(marked)
		_, err = client.ReserveExecute("select get_lock('rawresults', 1)", nil, nil)
		require.NoError(t, err)
		qr, err = client.Execute(query, nil)
		record("ReserveExecute", query, qr, err)
		qr, err = client.StreamExecute(query, nil)
		record("ReserveStreamExecute", query, qr, err)
		require.NoError(t, client.Release())

		// with a setting, on a connection of the pools that applies it
		settings := []string{"set @@sql_safe_updates = 0"}
		client = newClient(marked)
		qr, err = client.ReserveExecute(query, settings, nil)
		record("ReserveExecute with settings", query, qr, err)
		client = newClient(marked)
		qr, err = client.ReserveStreamExecute(query, settings, nil)
		record("ReserveStreamExecute with settings", query, qr, err)

		client = newClient(marked)
		qr, err = client.ReserveBeginExecute(query, settings, nil, nil)
		record("ReserveBeginExecute", query, qr, err)
		require.NoError(t, client.Release())
		client = newClient(marked)
		qr, err = client.ReserveBeginStreamExecute(query, settings, nil, nil)
		record("ReserveBeginStreamExecute", query, qr, err)
		require.NoError(t, client.Release())
	}
	return out
}

// describe writes every part of the fields and the bytes of the values.
func describe(qr *sqltypes.Result) string {
	s := ""
	for _, f := range qr.Fields {
		s += fmt.Sprintf("field %q %q %q %q %q %v collation=%d length=%d flags=%d decimals=%d\n", f.Name, f.OrgName, f.Table, f.OrgTable, f.Database, f.Type, f.Charset, f.ColumnLength, f.Flags, f.Decimals)
	}
	for _, row := range qr.Rows {
		for _, v := range row {
			if v.IsNull() {
				s += "NULL "
				continue
			}
			s += fmt.Sprintf("%v:%X ", v.Type(), v.Raw())
		}
		s += "\n"
	}
	return s + fmt.Sprintf("rows affected %d", qr.RowsAffected)
}

// restart restarts the tablet server with or without
// --queryserver-config-raw-result-charset.
func restart(t *testing.T, raw bool) {
	t.Helper()
	framework.Server.StopService()
	framework.Server.Config().RawResultCharset = raw
	require.NoError(t, framework.Server.StartService(framework.Target, dbcfgs, nil))
}

// TestLegacyCallerSeesTheSameResult runs every query, with every method that
// returns rows, as a caller that does not mark its requests, on the tablet with
// --queryserver-config-raw-result-charset, and again on the same tablet without
// it, and checks that the results are the same in every part of every field
// and in every byte. It also checks that a caller that marks its requests gets
// the text as MySQL sends it with character_set_results = NULL, and that
// resultcharset.ToConnection, as vtgate applies it, turns that into the same
// result.
func TestLegacyCallerSeesTheSameResult(t *testing.T) {
	legacy := run(t, false)
	marked := run(t, true)
	markedConverted := map[string]string{}
	for _, query := range queries {
		qr, err := newClient(true).Execute(query, nil)
		if err != nil {
			markedConverted[query] = "error: " + err.Error()
			continue
		}
		markedConverted[query] = describe(resultcharset.ToConnection(qr.Fields, collations.CollationUtf8mb4ID, true).Result(qr))
	}

	// The raw text differs where MySQL converts: the latin1 column has
	// 4D FC 6C 6C 65 72 with the collation latin1_german2_ci (31) and the
	// length 10.
	qr, err := newClient(true).Execute("select l1 from rawtext where id = 1", nil)
	require.NoError(t, err)
	assert.Equal(t, "M\xFCller", qr.Rows[0][0].ToString())
	assert.Equal(t, uint32(31), qr.Fields[0].Charset)
	assert.Equal(t, uint32(10), qr.Fields[0].ColumnLength)

	restart(t, false)
	t.Cleanup(func() { restart(t, true) })
	plain := run(t, false)

	require.Len(t, legacy, len(plain))
	for key, want := range plain {
		assert.Equal(t, want, legacy[key], key)
	}
	for _, query := range queries {
		assert.Equal(t, plain["Execute "+query], markedConverted[query], query)
	}

	assert.NotEqual(t, plain["Execute "+queries[0]], marked["Execute "+queries[0]])
}

// resultsCharset returns @@character_set_results as the connection that
// serves the next query of client sees it, and "NULL" for NULL.
func resultsCharset(t *testing.T, client *framework.QueryClient) string {
	t.Helper()
	qr, err := client.Execute("select @@character_set_results", nil)
	require.NoError(t, err)
	if qr.Rows[0][0].IsNull() {
		return "NULL"
	}
	return qr.Rows[0][0].ToString()
}

// TestSessionKeepsRawResults reads @@character_set_results on the connections
// of the pools that serve queries after every path that sets up, resets or
// replaces their session: a new connection of each pool, a connection with a
// setting and the reset of the setting, a reserved connection and the pool
// after its release, a transaction, a transaction with CLIENT_FOUND_ROWS, the
// app debug user, and a reconnect after MySQL killed the connection. Each pool
// has one connection, so the next query gets the connection that the previous
// one left behind.
func TestSessionKeepsRawResults(t *testing.T) {
	client := newClient(false)
	assert.Equal(t, "NULL", resultsCharset(t, client), "OLTP pool")

	qr, err := client.StreamExecute("select @@character_set_results", nil)
	require.NoError(t, err)
	assert.True(t, qr.Rows[0][0].IsNull(), "stream pool")

	// a setting is applied to the pooled connection and reset when the next
	// query has none
	qr, err = client.ReserveExecute("select @@character_set_results, @@sql_safe_updates", []string{"set @@sql_safe_updates = 1"}, nil)
	require.NoError(t, err)
	assert.True(t, qr.Rows[0][0].IsNull(), "with a setting")
	assert.Equal(t, "1", qr.Rows[0][1].ToString())
	qr, err = client.Execute("select @@character_set_results, @@sql_safe_updates", nil)
	require.NoError(t, err)
	assert.True(t, qr.Rows[0][0].IsNull(), "after the reset of the setting")
	assert.Equal(t, "0", qr.Rows[0][1].ToString())

	reserved := newClient(false)
	_, err = reserved.ReserveExecute("select get_lock('rawresults', 1)", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "NULL", resultsCharset(t, reserved), "reserved")
	require.NoError(t, reserved.Release())
	assert.Equal(t, "NULL", resultsCharset(t, client), "OLTP pool after a release")

	for _, foundRows := range []bool{false, true} {
		tx := newClient(false)
		require.NoError(t, tx.Begin(foundRows))
		assert.Equal(t, "NULL", resultsCharset(t, tx), "transaction, found rows %v", foundRows)
		require.NoError(t, tx.Commit())
	}

	appDebug := framework.NewClientWithContext(callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "vt_appdebug"}))
	assert.Equal(t, "NULL", resultsCharset(t, appDebug), "app debug user")

	// MySQL kills the connection of the OLTP pool; the next query
	// reconnects and runs on a new connection
	qr, err = client.Execute("select connection_id()", nil)
	require.NoError(t, err)
	id := qr.Rows[0][0].ToString()
	admin, err := mysql.Connect(t.Context(), &connParams)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	_, err = admin.ExecuteFetch("kill "+id, 0, false)
	require.NoError(t, err)
	assert.Eventually(t, func() bool {
		qr, err := admin.ExecuteFetch("select count(*) from information_schema.processlist where id = "+id, 1, false)
		return err == nil && qr.Rows[0][0].ToString() == "0"
	}, 30*time.Second, 10*time.Millisecond)
	assert.Equal(t, "NULL", resultsCharset(t, client), "after a reconnect")
	qr, err = client.Execute("select connection_id()", nil)
	require.NoError(t, err)
	assert.NotEqual(t, id, qr.Rows[0][0].ToString(), "the connection was replaced")
}

// TestProcedureThatSetsNames calls a stored procedure whose body runs SET
// NAMES latin1, which the tablet cannot see.
//
// Outside a transaction the tablet closes the connection that ran a CALL, so
// the next query of the OLTP pool, which has one connection, runs on a new
// one with character_set_results = NULL.
//
// In a transaction the connection stays, and MySQL sends its later results in
// latin1 with the collation latin1_swedish_ci in every text field: 'Müller'
// as 4D FC 6C 6C 65 72, and '中文', which latin1 does not have, as "??". The
// tablet converts by the collation in the fields, so a caller without the mark
// gets 4D C3 BC 6C 6C 65 72 with the connection collation, and a caller with
// the mark gets what MySQL sent.
func TestProcedureThatSetsNames(t *testing.T) {
	admin, err := mysql.Connect(t.Context(), &connParams)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	_, err = admin.ExecuteFetch("create procedure rawnames() begin set names latin1; end", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.ExecuteFetch("drop procedure rawnames", 0, false) })

	client := newClient(false)
	qr, err := client.Execute("select connection_id()", nil)
	require.NoError(t, err)
	id := qr.Rows[0][0].ToString()
	_, err = client.Execute("call rawnames()", nil)
	require.NoError(t, err)
	qr, err = client.Execute("select connection_id(), @@character_set_results", nil)
	require.NoError(t, err)
	assert.NotEqual(t, id, qr.Rows[0][0].ToString(), "the connection was replaced")
	assert.True(t, qr.Rows[0][1].IsNull())

	want := map[bool]string{
		false: `field "l1" "l1" "rawtext" "rawtext" "vttest" VARCHAR collation=255 length=40 flags=0 decimals=0
field "gbk" "gbk" "rawtext" "rawtext" "vttest" VARCHAR collation=255 length=40 flags=0 decimals=0
VARCHAR:4DC3BC6C6C6572 VARCHAR:3F3F 
rows affected 0`,
		true: `field "l1" "l1" "rawtext" "rawtext" "vttest" VARCHAR collation=8 length=10 flags=0 decimals=0
field "gbk" "gbk" "rawtext" "rawtext" "vttest" VARCHAR collation=8 length=10 flags=0 decimals=0
VARCHAR:4DFC6C6C6572 VARCHAR:3F3F 
rows affected 0`,
	}
	for marked, want := range want {
		tx := newClient(marked)
		require.NoError(t, tx.Begin(false))
		_, err = tx.Execute("call rawnames()", nil)
		require.NoError(t, err)
		qr, err = tx.Execute("select l1, gbk from rawtext where id = 1", nil)
		require.NoError(t, err)
		assert.Equal(t, want, describe(qr), "marked %v", marked)
		require.NoError(t, tx.Rollback())
	}
}
