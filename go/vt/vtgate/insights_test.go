/*
Copyright 2022 The Vitess Authors.

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

package vtgate

import (
	"context"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vtgate/logstats"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"
)

var (
	logger *streamlog.StreamLogger
)

type setupOptions struct {
	bufSize, patternLimit, rowsReadThreshold, responseTimeThreshold, maxPerInterval uint
	tableString                                                                     string
	maxRawQuerySize                                                                 uint
}

func setup(t *testing.T, brokers, publicID, username, password string, options setupOptions) (*Insights, error) {
	logger = streamlog.New("tests", 10)
	dfl := func(x, y uint) uint {
		if x != 0 {
			return x
		}
		return y
	}
	insights, err := initInsightsInner(logger, brokers, publicID, username, password,
		dfl(options.bufSize, 5*1024*1024),
		dfl(options.patternLimit, 10000),
		dfl(options.rowsReadThreshold, 1000),
		dfl(options.responseTimeThreshold, 1000),
		dfl(options.maxPerInterval, 100),
		dfl(options.maxRawQuerySize, 64),
		15*time.Second, true, true)
	if insights != nil {
		t.Cleanup(func() { insights.Drain() })
	}
	return insights, err
}

func TestInsightsNeedsDatabaseBranchID(t *testing.T) {
	_, err := setup(t, "localhost:1234", "", "", "", setupOptions{})
	assert.Error(t, err, "public_id is required")
}

func TestInsightsDisabled(t *testing.T) {
	_, err := setup(t, "", "", "", "", setupOptions{})
	assert.NoError(t, err)
}

func TestInsightsEnabled(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	assert.NoError(t, err)
}

func TestInsightsMissingUsername(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "", "password", setupOptions{})
	assert.Error(t, err, "without a username")
}

func TestInsightsMissingPassword(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "username", "", setupOptions{})
	assert.Error(t, err, "without a password")
}

func TestInsightsConnectionRefused(t *testing.T) {
	// send to a real Kafka endpoint, will fail
	insights, err := setup(t, "localhost:1", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)
	logger.Send(lsSlowQuery)
	require.True(t, insights.Drain(), "did not drain")
}

func TestInsightsSlowQuery(t *testing.T) {
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)
	messages := 0
	insights.Sender = func(buf []byte, topic, key string) error {
		messages++
		assert.Contains(t, string(buf), "select sleep(:vtg1)")
		assert.Contains(t, key, "mumblefoo/")
		assert.Equal(t, queryTopic, topic)
		return nil
	}
	logger.Send(lsSlowQuery)
	require.True(t, insights.Drain(), "did not drain")
	assert.Equal(t, 1, messages)
}

func TestInsightsSummaries(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select sleep(5)", responseTime: 5 * time.Second},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 2},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 3},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 5},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond, rowsRead: 7},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select sleep(5)", "total_duration:{seconds:5}", `statement_type:{value:\"SELECT\"}`),
			expect(queryStatsBundleTopic, "select sleep(5)", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo", "query_count:4", "sum_total_duration:{nanos:40000000}",
				"max_total_duration:{nanos:10000000}", "sum_rows_read:17", "max_rows_read:7"),
		})
}

func TestInsightsSchemaChanges(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "create table foo (bar int)"},
			{sql: "alter table foo add column bar int"},
			{sql: "drop table foo"},
			{sql: "rename table foo to bar"},
			{sql: "alter view foo as select * from bar"},
			{sql: "create view foo as select * from bar"},
			{sql: "drop view foo"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "create table foo"),
			expect(schemaChangeTopic, "CREATE TABLE `foo` (\\\\n\\\\t`bar` int\\\\n)", "operation:CREATE_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "alter table foo"),
			expect(schemaChangeTopic, "ALTER TABLE `foo` ADD COLUMN `bar` int", "operation:ALTER_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "drop table foo"),
			expect(schemaChangeTopic, "DROP TABLE `foo`", "operation:DROP_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "rename table foo"),
			expect(schemaChangeTopic, "RENAME TABLE `foo` TO `bar`", "operation:RENAME_TABLE", "normalized:true"),

			expect(queryStatsBundleTopic, "alter view foo"),
			expect(schemaChangeTopic, "ALTER VIEW `foo` AS SELECT * FROM `bar`", "operation:ALTER_VIEW", "normalized:true"),

			expect(queryStatsBundleTopic, "create view foo"),
			expect(schemaChangeTopic, "CREATE VIEW `foo` AS SELECT * FROM `bar`", "operation:CREATE_VIEW", "normalized:true"),

			expect(queryStatsBundleTopic, "drop view foo"),
			expect(schemaChangeTopic, "DROP VIEW `foo`", "operation:DROP_VIEW", "normalized:true"),
		})
}

func TestInsightsSchemaChangesNoTruncateTable(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "truncate table foo"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, "truncate table foo"),
		})
}

func TestInsightsTooManyPatterns(t *testing.T) {
	insightsTestHelper(t, true,
		setupOptions{patternLimit: 3},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Second},
			{sql: "select * from foo2", responseTime: 5 * time.Second},
			{sql: "select * from foo3", responseTime: 5 * time.Second},
			{sql: "select * from foo4", responseTime: 5 * time.Second},
			{sql: "select * from foo5", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select * from foo1", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo2", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo3", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo4", "total_duration:{seconds:5}"),
			expect(queryTopic, "select * from foo5", "total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo1", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo2", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
			expect(queryStatsBundleTopic, "select * from foo3", "query_count:1", "sum_total_duration:{seconds:5}", "max_total_duration:{seconds:5}"),
		})
}

func TestInsightsTooManyInteresting(t *testing.T) {
	insightsTestHelper(t, true,
		setupOptions{maxPerInterval: 4},
		[]insightsQuery{
			{sql: "select 1", responseTime: 5 * time.Second},
			{sql: "select 1", rowsRead: 20000},
			{sql: "select 1", error: "thou shalt not"},
			{sql: "select 1", responseTime: 6 * time.Second},
			{sql: "select 1", rowsRead: 21000},
			{sql: "select 1", error: "no but seriously"},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select 1", "total_duration:{seconds:5}"),
			expect(queryTopic, "select 1", "rows_read:20000"),
			expect(queryTopic, "<error>", "thou shalt not"),
			expect(queryTopic, "select 1", "total_duration:{seconds:6}"),
			expect(queryStatsBundleTopic, "select 1", "query_count:4", "sum_total_duration:{seconds:11}", "max_total_duration:{seconds:6}", "sum_rows_read:41000"),
			expect(queryStatsBundleTopic, "<error>", "query_count:2", "error_count:2"),
		})
}

func TestInsightsResponseTimeThreshold(t *testing.T) {
	insightsTestHelper(t, false,
		setupOptions{responseTimeThreshold: 500},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 400 * time.Millisecond},
			{sql: "select * from foo2", responseTime: 600 * time.Millisecond},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select * from foo2", "total_duration:{nanos:600000000}"),
		})
}

func TestInsightsRowsReadThreshold(t *testing.T) {
	insightsTestHelper(t, false,
		setupOptions{rowsReadThreshold: 42},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Millisecond, rowsRead: 88},
			{sql: "select * from foo2", responseTime: 5 * time.Millisecond, rowsRead: 15},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select * from foo1", "total_duration:{nanos:5000000}", "rows_read:88"),
		})
}

func TestInsightsKafkaBufferSize(t *testing.T) {
	insightsTestHelper(t, true,
		setupOptions{bufSize: 5},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Second},
		},
		nil)
}

func TestInsightsComments(t *testing.T) {
	insightsTestHelper(t, true,
		setupOptions{},
		[]insightsQuery{
			{sql: "select * from foo /*abc='xxx%2fyyy%3azzz'*/", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "xxx/yyy:zzz"),
			expect(queryStatsBundleTopic, "select * from foo").butNot("xxx"),
		})
}

func TestInsightsErrors(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select this does not parse", error: "syntax error at position 21 after 'does'"},
			{sql: "nor does this", error: "this is a fake error, BindVars: {'foo'}"},
			{sql: "third bogus", error: "another fake error, Sql: \"third bogus\""},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"syntax error at position <position>\"}`, `statement_type:{value:\"ERROR\"}`).butNot("does"),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"<error>\"}`, `statement_type:\"ERROR\"`, "query_count:3", "error_count:3").butNot("does", "foo", "bogus"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"this is a fake error\"}`, `statement_type:{value:\"ERROR\"}`).butNot("foo", "BindVars"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"another fake error\"}`, `statement_type:{value:\"ERROR\"}`).butNot("Sql", "bogus"),
		})
}

func TestInsightsSafeErrors(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select :vtg1", normalized: YES, error: "target: commerce.0.primary: vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: 29.998788288s, killing query ID 58 (CallerID: userData1)"},
			{sql: "select :vtg1", normalized: YES, error: `target: commerce.0.primary: vttablet: rpc error: code = Aborted desc = Row count exceeded 10000 (errno 10001) (sqlstate HY000) (CallerID: userData1): Sql: "select * from foo as f1 join foo as f2 join foo as f3 join foo as f4 join foo as f5 join foo as f6 where f1.id > :vtg1", BindVars: {#maxLimit: "type:INT64 value:\"10001\""vtg1: "type:INT64 value:\"0\"`},
			{sql: "select :vtg1", normalized: YES, error: "target: commerce.0.primary: vttablet: rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (18345369 vs. 16777216)"},
			{sql: "select :vtg1", normalized: YES, error: `target: commerce.0.primary: vttablet: rpc error: code = Canceled desc = EOF (errno 2013) (sqlstate HY000) (CallerID: userData1): Sql: "select :vtg1 from foo", BindVars: {#maxLimit: "type:INT64 value:\"10001\""vtg1: "type:INT64 value:\"1\"`},
			{sql: "select :vtg1", normalized: YES, error: `target: sharded.-40.primary: vttablet: rpc error: code = Unavailable desc = error reading from server: EOF`},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = Canceled`).butNot("foo", "BindVars", "Sql").count(2),
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = Aborted`).butNot("foo", "BindVars", "Sql"),
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = ResourceExhausted`),
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = Unavailable`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"select :vtg1 from dual\"}`, `statement_type:\"ERROR\"`, "query_count:5", "error_count:5").butNot("foo", "BindVars", ":vtg1"),
		})
}

func TestInsightsSavepoints(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "savepoint foo"},
			{sql: "savepoint bar"},
		},
		[]insightsKafkaExpectation{
			expect(queryStatsBundleTopic, `savepoint <id>`, "query_count:2", `statement_type:\"SAVEPOINT\"`).butNot("foo", "bar"),
		})
}

func TestInsightsExtraNormalization(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select beam.`User`.id, beam.`User`.`name` from beam.`User` where beam.`User`.id in (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12, :v13, :v14, :v15, :v16, :v17, :v18, :v19, :v20, :v21, :v22, :v23, :v24, :v25, :v26, :v27, :v28, :v29, :v30, :v31, :v32, :v33, :v34, :v35, :v36, :v37, :v38, :v39, :v40, :v41, :v42, :v43, :v44, :v45, :v46, :v47, :v48, :v49, :v50, :v51, :v52, :v53, :v54, :v55, :v56, :v57, :v58, :v59, :v60, :v61, :v62, :v63, :v64, :v65, :v66, :v67, :v68, :v69, :v70, :v71, :v72, :v73)", responseTime: 5 * time.Second},
			{sql: "select * from users where foo in (:v1, :v2) and bar in (:v3, :v4) and baz in (:v5) and blarg in (:v6)", responseTime: 5 * time.Second},
			{sql: "insert into foo values (:v1, :vtg2), (?, null), (:v3, :v4)", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, "select beam.`User`.id, beam.`User`.`name` from beam.`User` where beam.`User`.id in (<elements>)").butNot(":v73"),
			expect(queryStatsBundleTopic, "select beam.`User`.id, beam.`User`.`name` from beam.`User` where beam.`User`.id in (<elements>)").butNot(":v73"),
			expect(queryTopic, "select * from users where foo in (<elements>) and bar in (<elements>) and baz in (<elements>) and blarg in (<elements>)").butNot(":v2", ":v4", ":v5"),
			expect(queryStatsBundleTopic, "select * from users where foo in (<elements>) and bar in (<elements>) and baz in (<elements>) and blarg in (<elements>)").butNot(":v2", ":v4", ":v5"),
			expect(queryTopic, "insert into foo values <values>", `statement_type:{value:\"INSERT\"}`).butNot(":v1", ":vtg1", "null", "?"),
			expect(queryStatsBundleTopic, "insert into foo values <values>", `statement_type:\"INSERT\"`).butNot(":v1", ":vtg1", "null", "?"),
		})
}

func TestMakeKafkaKeyIsDeterministic(t *testing.T) {
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)

	sql := `this isn't even real sql`
	key := insights.makeKafkaKey(sql)
	assert.Equal(t, "mumblefoo/6edc967a", key)

	sql = `another string value`
	key = insights.makeKafkaKey(sql)
	assert.Equal(t, "mumblefoo/67374b03", key)
}

func TestTables(t *testing.T) {
	testCases := []struct {
		name, input    string
		split          []string
		message, avoid string
	}{
		{
			"empty",
			"",
			nil,
			"",
			"tables",
		},
		{
			"one table",
			"foo",
			[]string{"foo"},
			`tables:\"foo\"`,
			"",
		},
		{
			"two tables",
			"foo, bar",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			",",
		},
		{
			"two tables without a space",
			"foo,bar",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			",",
		},
		{
			"one table name with backticks",
			"`foo`",
			[]string{"foo"},
			`tables:\"foo\"`,
			"`",
		},
		{
			"two table names with backticks",
			"`foo`, `bar`",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			"`",
		},
		{
			"two table names with backticks, no space",
			"`foo`,`bar`",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			"`",
		},
		{
			"table name has a comma",
			"`foo, bar`",
			[]string{"foo, bar"},
			`tables:\"foo, bar\"`,
			"`",
		},
		{
			"table name is only partially quoted",
			"foo.`order`,b,`c,d`.e",
			[]string{"foo.order", "b", "c,d.e"},
			`tables:\"foo.order\" tables:\"b\" tables:\"c,d.e\"`,
			"`",
		},
		{
			"many parts, some in backticks",
			"`abc`.`def`.ghi.`j,kl`,`mno`.pqr.`stu`",
			[]string{"abc.def.ghi.j,kl", "mno.pqr.stu"},
			`tables:\"abc.def.ghi.j,kl\" tables:\"mno.pqr.stu\"`,
			"`",
		},
		{
			"unterminated backtick",
			"foo, `bar, baz",
			[]string{"foo", "bar, baz"},
			`tables:\"foo\" tables:\"bar, baz\"`,
			"`",
		},
		{
			"ends with a comma",
			"foo,bar,",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			"`",
		},
		{
			"extra commas",
			"foo,,,bar,,",
			[]string{"foo", "bar"},
			`tables:\"foo\" tables:\"bar\"`,
			",",
		},
		{
			"only a comma",
			",",
			nil,
			"",
			"tables",
		},
		{
			"commas and backticks extravaganza",
			"`foo,bar`, baz, `blah`, `lorem`, ipsum, `abc, xyz`",
			[]string{"foo,bar", "baz", "blah", "lorem", "ipsum", "abc, xyz"},
			`tables:\"foo,bar\" tables:\"baz\" tables:\"blah\" tables:\"lorem\" tables:\"ipsum\" tables:\"abc, xyz\"`,
			"`",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := splitTables(tc.input)
			assert.Equal(t, tc.split, op)

			e1 := expect(queryTopic, "select 1", " total_duration:{seconds:5}")
			e2 := expect(queryStatsBundleTopic, "select 1", "query_count:1 sum_total_duration:{seconds:5} max_total_duration:{seconds:5}")
			if tc.message != "" {
				e1.patterns = append(e1.patterns, tc.message)
				e2.patterns = append(e2.patterns, tc.message)
			}
			if tc.avoid != "" {
				e1 = e1.butNot(tc.avoid)
				e2 = e2.butNot(tc.avoid)
			}
			insightsTestHelper(t, true, setupOptions{tableString: tc.input},
				[]insightsQuery{
					{sql: "select 1", responseTime: 5 * time.Second},
				},
				[]insightsKafkaExpectation{
					e1,
					e2,
				})
		})
	}
}

func TestNormalization(t *testing.T) {
	testCases := []struct {
		input, output string
	}{
		// nothing to change
		{"select * from users where id=:vtg1", "select * from users where id = :vtg1"},

		// normalizer strips off comments
		{"/* with some leading comments */ select * from users where id=:vtg1 /* with some trailing comments */", "select * from users where id = :vtg1"},

		// savepoints
		{"savepoint foo", "savepoint <id>"},
		{"release savepoint bar", "release savepoint <id>"},

		// -- VALUES compaction
		// one tuple
		{"insert into xyz values (:v1, :v2)", "insert into xyz values <values>"},

		// case insensitive
		{"INSERT INTO xyz VALUES (:v1, :v2)", "insert into xyz values <values>"},

		// multiple tuples
		{"insert into xyz values (:v1, :v2), (:v3, null), (null, :v4)", "insert into xyz values <values>"},

		// multiple singles
		{"insert into xyz values (:v1), (null), (:v2)", "insert into xyz values <values>"},

		// question marks instead
		{"insert into xyz values (?, ?)", "insert into xyz values <values>"},

		// -- SET compaction
		// case insensitive
		{"SELECT 1 FROM x WHERE xyz IN (:vtg1, :vtg2) AND abc in (:v3, :v4)", "select 1 from x where xyz in (<elements>) and abc in (<elements>)"},

		// question marks instead
		{"SELECT 1 FROM x WHERE xyz IN (?, ?) AND abc in (?, ?)", "select 1 from x where xyz in (<elements>) and abc in (<elements>)"},

		// single element in list
		{"select 1 FROM x where xyz in (:bv1)", "select 1 from x where xyz in (<elements>)"},

		// very large :v sequence numbers
		{"select 1 from x where xyz in (:v8675309, :v8765000)", "select 1 from x where xyz in (<elements>)"},

		// nested, single
		{"select 1 from x where (abc, xyz) in ((:v1, :v2))", "select 1 from x where (abc, xyz) in (<elements>)"},

		// nested, multiple
		{"select 1 from x where (abc, xyz) in ((:vtg1, :vtg2), (:vtg3, :vtg4), (:vtg5, :vtg6))", "select 1 from x where (abc, xyz) in (<elements>)"},

		// nested, multiple, question marks
		{"select 1 from x where (abc, xyz) in ((?, ?), (?, ?), (?, ?))", "select 1 from x where (abc, xyz) in (<elements>)"},

		// mixed nested and simple
		{"select 1 from x where xyz in ((:v1, :v2), :v3)", "select 1 from x where xyz in (<elements>)"},

		// subqueries should not be normalized
		{"select 1 from x where xyz in (select distinct foo from bar)", "select 1 from x where xyz in (select distinct foo from bar)"},

		// stuff within a subquery should be normalized
		{"select 1 from x where xyz in (select distinct foo from bar where baz in (1,2,3))", "select 1 from x where xyz in (select distinct foo from bar where baz in (<elements>))"},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			out, err := normalizeSQL(tc.input)
			assert.NoError(t, err)
			assert.Equal(t, tc.output, out)
		})
	}
}

func TestStringTruncation(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"", ""},
		{"1234567890", "12345678"},
		{"1234567😂", "1234567"},
		{"123456😂", "123456"},
		{"12345😂", "12345"},
		{"1234😂", "1234😂"},
		{"123😂", "123😂"},
		{"12😂", "12😂"},
		{"1😂", "1😂"},
		{"😂", "😂"},
		{"1234567😂8", "1234567"},
		{"123456😂7", "123456"},
		{"12345😂6", "12345"},
		{"1234😂5", "1234😂"},
		{"123😂4", "123😂4"},
		{"12😂3", "12😂3"},
		{"1😂2", "1😂2"},
		{"😂1", "😂1"},
		{"123456\xcf\xcf\xcf\xcf", "123456\xcf\xcf"}, // invalid UTF-8
		{"123456\x8f\x8f\x8f\x8f", "123456\x8f\x8f"}, // invalid UTF-8
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			op := safelyTruncate(tc.in, 8)
			assert.Equal(t, tc.out, op)
			assert.LessOrEqual(t, len(op), 8)

			op = efficientlyTruncate(tc.in, 8)
			assert.Equal(t, tc.out, op)
			assert.LessOrEqual(t, len(op), 8)
		})
	}
}

func TestRawQueries(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{maxRawQuerySize: 32},
		[]insightsQuery{
			{sql: "select * from users where id = :vtg1", responseTime: 5 * time.Second,
				rawSQL: "select * from users where id=7"}, // no change
			{sql: "select * from users where id = :vtg1 and email = :vtg2", responseTime: 5 * time.Second,
				rawSQL: "select * from users where id=8 and email='alice@sample.com'"}, // truncates after `id=8 a`
			{sql: "insert into foo values (:v1,:v2),(:v3,:v4),(:v5,:v6)", responseTime: 5 * time.Second,
				rawSQL: "insert into foo values (1, 2), (3, 4), (5, 6)"}, // cleanly summarizes
			{sql: "insert into foo values (:v1,:v2)", responseTime: 5 * time.Second,
				rawSQL: "insert into foo values ('😂', 'bob')"}, // truncates after `😂', `
			{sql: "update foo set a = :vtg1 where id = :vtg2", responseTime: 5 * time.Second,
				rawSQL: "update foo set a=1 where id=7"}, // no change
			{sql: "update foo set a = :vtg1 where id = :vtg2", responseTime: 5 * time.Second,
				rawSQL: "update foo set a=1 where id='6😂7890'"}, // trucates after `id='6` without splitting the 😂
		},
		[]insightsKafkaExpectation{
			expect(queryTopic,
				`normalized_sql:{value:\"select * from users where id = :vtg1\"}`,
				`raw_sql:{value:\"select * from users where id=7\"}`).butNot("raw_sql_abbreviation"),
			expect(queryTopic,
				`normalized_sql:{value:\"select * from users where id = :vtg1 and email = :vtg2\"}`,
				`raw_sql:{value:\"select * from users where id=8 a\"}`,
				"raw_sql_abbreviation:TRUNCATED").butNot("alice"),
			expect(queryTopic,
				`normalized_sql:{value:\"insert into foo values <values>\"}`,
				`raw_sql:{value:\"insert into foo values (1, 2)\"}`,
				"raw_sql_abbreviation:SUMMARIZED").butNot("),", "(3, 4)", "(5, 6)"),
			expect(queryTopic,
				`normalized_sql:{value:\"insert into foo values <values>\"}`,
				`raw_sql:{value:\"insert into foo values ('😂', \"}`,
				"raw_sql_abbreviation:TRUNCATED").butNot("bob"),
			expect(queryTopic,
				`normalized_sql:{value:\"update foo set a = :vtg1 where id = :vtg2\"}`,
				`raw_sql:{value:\"update foo set a=1 where id=7\"}`).butNot("raw_sql_abbreviation"),
			expect(queryTopic,
				`normalized_sql:{value:\"update foo set a = :vtg1 where id = :vtg2\"}`,
				`raw_sql:{value:\"update foo set a=1 where id='6\"}`,
				"raw_sql_abbreviation:TRUNCATED").butNot("67890"),
			expect(queryStatsBundleTopic).count(4),
		})
}

func TestNotNormalizedNotError(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			// successful, normalized
			{sql: "select * from users where id = :vtg1", responseTime: 5 * time.Second, normalized: YES,
				rawSQL: "select * from users where id=7"},

			// successful, not normalized => statement is sent
			{sql: "begin", responseTime: 5 * time.Second, normalized: NO,
				rawSQL: "begin"},

			// successful, not normalized => statement is sent, downcased, tags are parsed
			{sql: "roLlBacK /*abc='xyz'*/", responseTime: 5 * time.Second, normalized: NO,
				rawSQL: "roLlBacK /*abc='xyz'*/"},

			// error, normalized => query is interesting, statement is sent
			{sql: "select * from orders", normalized: YES, error: "no such table",
				rawSQL: "select * from orders"},

			// error, not normalized => query is interesting, statement replaced with "<error>"
			{sql: "hello world", normalized: NO, error: "syntax error",
				rawSQL: "hello world"},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"select * from users where id = :vtg1\"}`,
				`raw_sql:{value:\"select * from users where id=7\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"select * from users where id = :vtg1\"}`),
			expect(queryTopic, `normalized_sql:{value:\"begin\"}`,
				`raw_sql:{value:\"begin\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"begin\"}`),
			expect(queryTopic, `normalized_sql:{value:\"rollback\"}`, "xyz",
				`raw_sql:{value:\"roLlBacK /*abc='xyz'*/\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"rollback\"}`).butNot("xyz"),
			expect(queryTopic, `normalized_sql:{value:\"select * from orders\"}`, "no such table",
				`raw_sql:{value:\"select * from orders\"}`).butNot("<error>"),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"select * from orders\"}`).butNot("no such table", "<error>"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, "syntax error",
				`raw_sql:{value:\"hello world\"}`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"<error>\"}`).butNot("hello", "syntax error"),
		})
}

func TestRawQueryShortening(t *testing.T) {
	testCases := []struct {
		input, output, errStr string
		limit                 uint
	}{
		{
			input:  "select * from users",
			output: "select * from users",
			limit:  64,
		},
		{
			input:  "select * from users",
			errStr: "raw SQL string is still too long",
			limit:  8,
		},
		{
			input:  "insert into users values (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
			output: "insert into users values (1, 'Alice')",
			limit:  64,
		},
		{
			input:  "insert into users values (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
			errStr: "raw SQL string is still too long",
			limit:  8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			op, err := shortenRawSQL(tc.input, tc.limit)
			assert.Equal(t, tc.output, op)
			if tc.errStr != "" {
				require.Error(t, err)
				assert.Equal(t, tc.errStr, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestErrorNormalization(t *testing.T) {
	testCases := []struct {
		name, input, output string
	}{
		{
			name:   "Normalizes elapsed time and query id",
			input:  "vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: 20.000937445s, killing query ID 135243 (CallerID: planetscale-admin)",
			output: "vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: <time>, killing query ID <id> (CallerID: planetscale-admin)",
		},
		{
			name:   "Normalizes transaction id and ended at time",
			input:  "vttablet: rpc error: code = Aborted desc = transaction 1656362196194291371: ended at 2022-06-30 20:34:09.614 UTC (unlocked closed connection) (CallerID: planetscale-admin)",
			output: "vttablet: rpc error: code = Aborted desc = transaction <transaction>: ended at <time> (unlocked closed connection) (CallerID: planetscale-admin)",
		},
		{
			name:   "Normalizes connection id",
			input:  "target: taobench-8.20-40.primary: vttablet: rpc error: code = Unavailable desc = conn 299286: Write(packet) failed: write unix @->/vt/socket/mysql.sock: write: broken pipe (errno 2006) (sqlstate HY000) (CallerID: planetscale-admin)",
			output: "target: taobench-8.20-40.primary: vttablet: rpc error: code = Unavailable desc = conn <conn>: Write(packet) failed: write unix @->/vt/socket/mysql.sock: write: broken pipe (errno 2006) (sqlstate HY000) (CallerID: planetscale-admin)",
		},
		{
			name:   "Normalizes the the table path",
			input:  "target: gomy_backend_production.-.primary: vttablet: rpc error: code = ResourceExhausted desc = The table '/vt/vtdataroot/vt_0955681468/tmp/#sql351_1df_2' is full (errno 1114) (sqlstate HY000) (CallerID: planetscale-admin)",
			output: "target: gomy_backend_production.-.primary: vttablet: rpc error: code = ResourceExhausted desc = The table <table> is full (errno 1114) (sqlstate HY000) (CallerID: planetscale-admin)",
		},
		{
			name:   "Normalizes the row number",
			input:  "transaction rolled back to reverse changes of partial DML execution: target: targ.a8-b0.primary: vttablet: rpc error: code = InvalidArgument desc = Data too long for column 'media_url' at row 2 (errno 1406) (sqlstate 22001)",
			output: "transaction rolled back to reverse changes of partial DML execution: target: targ.a8-b0.primary: vttablet: rpc error: code = InvalidArgument desc = Data too long for column 'media_url' at row <row> (errno 1406) (sqlstate 22001)",
		},
		{
			name:   "Normalizes syntax error position",
			input:  "syntax error at position 29",
			output: "syntax error at position <position>",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized := normalizeError(tc.input)
			assert.Equal(t, tc.output, normalized)
		})
	}
}

const (
	AUTO = iota
	NO
	YES
)

type insightsQuery struct {
	sql, error, rawSQL string
	responseTime       time.Duration
	rowsRead           int

	// insightsTestHelper sets ls.IsNormalized to false for errors, true otherwise.
	// Set normalized=YES or normalized=NO to override this, e.g., to simulate an error
	// that occurs after query parsing has succeeded, or to simulate a successful
	// statement that did not need to be normalized.
	normalized int
}

type insightsKafkaExpectation struct {
	patterns     []string
	antipatterns []string
	topic        string
	want, found  int
}

func expect(topic string, patterns ...string) insightsKafkaExpectation {
	return insightsKafkaExpectation{
		patterns: patterns,
		topic:    topic,
		want:     1,
	}
}

func (ike insightsKafkaExpectation) butNot(anti ...string) insightsKafkaExpectation {
	ike.antipatterns = append(ike.antipatterns, anti...)
	return ike
}

func (ike insightsKafkaExpectation) count(n int) insightsKafkaExpectation {
	ike.want = n
	return ike
}

func insightsTestHelper(t *testing.T, mockTimer bool, options setupOptions, queries []insightsQuery, expect []insightsKafkaExpectation) {
	t.Helper()
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", options)
	require.NoError(t, err)
	if options.maxRawQuerySize > 0 {
		insights.MaxRawQueryLength = options.maxRawQuerySize
	}
	insights.Sender = func(buf []byte, topic, key string) error {
		assert.Contains(t, string(buf), "mumblefoo", "database branch public ID not present in message body")
		assert.True(t, strings.HasPrefix(key, "mumblefoo"), "key has unexpected form %q", key)
		assert.Contains(t, string(buf), queryURLBase+"/"+topic, "expected key not present in message body")
		var found bool
		for i, ex := range expect {
			matchesAll := true
			if topic == ex.topic {
				for _, p := range ex.patterns {
					if !strings.Contains(string(buf), p) {
						matchesAll = false
						break
					}
				}
				if matchesAll {
					expect[i].found++
					found = true
					break
				}
			}
		}
		assert.True(t, found, "no pattern expects topic=%q buf=%q", topic, string(buf))
		return nil
	}
	now := time.Now()
	for _, q := range queries {
		ls := &logstats.LogStats{
			SQL:          q.sql,
			RawSQL:       q.rawSQL,
			IsNormalized: q.normalized == YES || (q.error == "" && q.normalized == AUTO),
			StartTime:    now.Add(-q.responseTime),
			EndTime:      now,
			RowsRead:     uint64(q.rowsRead),
			Ctx:          context.Background(),
			Table:        options.tableString,
		}
		if q.error != "" {
			ls.Error = errors.New(q.error)
		} else {
			ls.StmtType = sqlparser.Preview(q.sql).String()
		}
		logger.Send(ls)
	}
	if mockTimer {
		insights.MockTimer()
	}
	require.True(t, insights.Drain(), "did not drain")
	for _, ex := range expect {
		assert.Equal(t, ex.want, ex.found, "count for %+v was wrong", ex)
	}
}

var (
	lsSlowQuery = &logstats.LogStats{
		SQL:          "select sleep(:vtg1)",
		IsNormalized: true,
		StartTime:    time.Now().Add(-5 * time.Second),
		EndTime:      time.Now(),
		Ctx:          context.Background(),
	}
)
