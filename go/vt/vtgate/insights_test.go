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
		15*time.Second, true)
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
		assert.Contains(t, string(buf), "select sleep(5)")
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
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"syntax error at position 21\"}`, `statement_type:{value:\"ERROR\"}`).butNot("does"),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"<error>\"}`, `statement_type:\"ERROR\"`, "query_count:3", "error_count:3").butNot("does", "foo", "bogus"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"this is a fake error\"}`, `statement_type:{value:\"ERROR\"}`).butNot("foo", "BindVars"),
			expect(queryTopic, `normalized_sql:{value:\"<error>\"}`, `error:{value:\"another fake error\"}`, `statement_type:{value:\"ERROR\"}`).butNot("Sql", "bogus"),
		})
}

func TestInsightsSafeErrors(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select :vtg1", error: "target: commerce.0.primary: vttablet: rpc error: code = Canceled desc = (errno 2013) due to context deadline exceeded, elapsed time: 29.998788288s, killing query ID 58 (CallerID: userData1)"},
			{sql: "select :vtg1", error: `target: commerce.0.primary: vttablet: rpc error: code = Aborted desc = Row count exceeded 10000 (errno 10001) (sqlstate HY000) (CallerID: userData1): Sql: "select * from foo as f1 join foo as f2 join foo as f3 join foo as f4 join foo as f5 join foo as f6 where f1.id > :vtg1", BindVars: {#maxLimit: "type:INT64 value:\"10001\""vtg1: "type:INT64 value:\"0\"`},
			{sql: "select :vtg1", error: "target: commerce.0.primary: vttablet: rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (18345369 vs. 16777216)"},
			{sql: "select :vtg1", error: `target: commerce.0.primary: vttablet: rpc error: code = Canceled desc = EOF (errno 2013) (sqlstate HY000) (CallerID: userData1): Sql: "select :vtg1 from foo", BindVars: {#maxLimit: "type:INT64 value:\"10001\""vtg1: "type:INT64 value:\"1\"`},
			{sql: "select :vtg1", error: `target: sharded.-40.primary: vttablet: rpc error: code = Unavailable desc = error reading from server: EOF`},
		},
		[]insightsKafkaExpectation{
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = Canceled`).butNot("foo", "BindVars", "Sql").count(2),
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = Aborted`).butNot("foo", "BindVars", "Sql"),
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = ResourceExhausted`),
			expect(queryTopic, `normalized_sql:{value:\"select :vtg1`, `code = Unavailable`),
			expect(queryStatsBundleTopic, `normalized_sql:{value:\"<error>\"}`, `statement_type:\"ERROR\"`, "query_count:5", "error_count:5").butNot("foo", "BindVars", ":vtg1"),
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

		//-- VALUES compaction
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

		//-- SET compaction
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
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			out, err := normalizeSQL(tc.input)
			assert.NoError(t, err)
			assert.Equal(t, tc.output, out)
		})
	}
}

func TestErrorNormalization(t *testing.T) {
	testCases := []struct {
		name, input, output string
	}{
		{
			"no change",
			"nothing to normalize here",
			"nothing to normalize here",
		},
		{
			"truncates after 'code ='",
			`target: mars.-.primary: vttablet: rpc error: code = InvalidArgument desc = Unknown system variable 'query_response_time_stats' (errno 1193) (sqlstate HY000) (CallerID: planetscale-admin): Sql: \"select @@query_response_time_stats from dual\", BindVars: {}: 2760`,
			`target: mars.-.primary: vttablet: rpc error: code = InvalidArgument`,
		},
		{
			"truncates after 'Duplicate entry'",
			`target: keep3rv1.-.primary: vttablet: Duplicate entry '0' for key 'stats.id' (errno 1062) (sqlstate 23000) (CallerID: planetscale-admin): Sql: \"insert into stats(jobs, work_done, keepers, rewarded_kp3r, bonded_kp3r) values (:v1, :v2, :v3, :v4, :v5)\", BindVars: {v1: \"type:VARBINARY value:\\\"0\\\"\"v2: \"type:VARBINARY value:\\\"1\\\"\"v3: \"type:VARBINARY value:\\\"0\\\"\"v4: \"type:VARBINARY value:\\\"0\\\"\"v5: \"type:VARBINARY value:\\\"0\\\"\"}`,
			`target: keep3rv1.-.primary: vttablet: Duplicate entry`,
		},
		{
			"truncates after 'syntax error'",
			`syntax error at position 42 near 'WHERE'`,
			`syntax error at position 42`,
		},
		{
			"removes Sql after colon",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin): Sql: \"insert into User(id, firstName, lastName, password, phone, email, citizenship, dob, address_street1, address_city, address_state, address_zip, address_country, piiSsn, signupIpAddress, createdAt, updatedAt) values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12, :v13, :v14, :v15, :v16, :v17)\", BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin)`,
		},
		{
			"removes Sql after comma",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin): Junk: "whatever", Sql: \"insert into User(id, firstName, lastName, password, phone, email, citizenship, dob, address_street1, address_city, address_state, address_zip, address_country, piiSsn, signupIpAddress, createdAt, updatedAt) values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9, :v10, :v11, :v12, :v13, :v14, :v15, :v16, :v17)\", BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin): Junk: "whatever"`,
		},
		{
			"removes BindVars after colon",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin): BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin)`,
		},
		{
			"removes BindVars after comma",
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin): Junk: "whatever", BindVars: {v1: \"type:VARBINARY value:\\\"$guid\\\"\"v10: \"type:VARBINARY value:\\\"$city\\\"\"v11: \"type:VARBINARY value:\\\"$state\\\"\"v12: \"type:VARBINARY value:\\\"$zip\\\"\"v13: \"type:VARBINARY value:\\\"$country\\\"\"v14: \"type:VARBINARY value:\\\"$tok\\\"\"v15: \"type:VARBINARY value:\\\"$ips\\\"\"v16: \"type:VARCHAR value:\\\"$ts\\\"\"v17: \"type:VARCHAR value:\\\"$ts\\\"\"v2: \"type:VARBINARY value:\\\"$firstname\\\"\"v3: \"type:VARBINARY value:\\\"$lastname\\\"\"v4: \"type:VARBINARY value:\\\"$blob\\\"\"v5: \"type:VARBINARY value:\\\"$phone\\\"\"v6: \"type:VARBINARY value:\\\"$email\\\"\"v7: \"type:VARBINARY value:\\\"$country\\\"\"v8: \"type:VARBINARY value:\\\"$birthday\\\"\"v9: \"type:VARBINARY value:\\\"$address\\\"\"}: 1`,
			`target: workmade.-.primary: vttablet: Data too long for column 'signupIpAddress' at row 1 (errno 1406) (sqlstate 22001) (CallerID: planetscale-admin): Junk: "whatever"`,
		},
		{
			"truncates very long strings",
			"Doloribus quo ullam labore nostrum nihil dolore nemo. Ad molestiae ab at dolores et. Iusto adipisci tempora et quia blanditiis et.  Velit alias eos quia et velit. Impedit ipsa itaque facilis repellendus. Quidem fuga sit voluptas minus. Neque amet et necessitatibus voluptatum. Voluptatem eum consequatur et dolor. Nulla deserunt quia cum ea hic architecto.  Eum et sed quo et officia nostrum eos. Nam quisquam et dolor repellat. Ea aperiam iste et.  Sint commodi non ut non occaecati velit. Architecto et fuga alias blanditiis consequatur qui ipsa magnam. Ea velit mollitia sed eligendi dolor et. Commodi et non sint optio asperiores.  Et dolores id corrupti voluptatum quasi voluptatem ipsam voluptatem. Dolorum et natus fugit. Ad id ea laudantium adipisci molestiae ratione eum quisquam.",
			"Doloribus quo ullam labore nostrum nihil dolore nemo. Ad molestiae ab at dolores et. Iusto adipisci tempora et quia blanditiis et.  Velit alias eos quia et velit. Impedit ipsa itaque facilis repellendus. Quidem fuga sit voluptas minus. Neque amet et necess",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := normalizeError(tc.input)
			assert.Equal(t, tc.output, op)
		})
	}
}

type insightsQuery struct {
	sql, error   string
	responseTime time.Duration
	rowsRead     int
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
	insights.Sender = func(buf []byte, topic, key string) error {
		assert.Contains(t, string(buf), "mumblefoo", "database branch public ID not present in message body")
		assert.True(t, strings.HasPrefix(key, "mumblefoo/"), "key has unexpected form %q", key)
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
		ls := &LogStats{
			SQL:       q.sql,
			StartTime: now.Add(-q.responseTime),
			EndTime:   now,
			RowsRead:  uint64(q.rowsRead),
			Ctx:       context.Background(),
		}
		if q.error != "" {
			ls.Error = errors.New(q.error)
		} else {
			ls.StmtType = strings.ToUpper(strings.SplitN(q.sql, " ", 2)[0])
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
	lsSlowQuery = &LogStats{
		SQL:       "select sleep(5)",
		StartTime: time.Now().Add(-5 * time.Second),
		EndTime:   time.Now(),
		Ctx:       context.Background(),
	}
)
