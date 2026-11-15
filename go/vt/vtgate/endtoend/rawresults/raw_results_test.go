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

// Package rawresults runs vtcombo with and without
// --queryserver-config-raw-result-charset, and checks that a MySQL client of
// vtgate gets the same results from both.
package rawresults

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}

const schema = `create table rawtext (
	id int primary key,
	l1 varchar(10) character set latin1 collate latin1_german2_ci,
	l1t text character set latin1,
	gbk varchar(10) character set gbk,
	sjis varchar(10) character set sjis,
	ujis varchar(10) character set ujis,
	euckr varchar(10) character set euckr,
	gb18030 varchar(10) character set gb18030,
	big5 varchar(10) character set big5,
	tis varchar(10) character set tis620,
	u3 varchar(10) character set utf8mb3,
	u4b varchar(10) character set utf8mb4 collate utf8mb4_bin,
	u4 text character set utf8mb4,
	uc varchar(10) character set ucs2,
	u32 varchar(10) character set utf32,
	c char(5) character set latin1,
	lt longtext character set latin1,
	e enum('a', 'Ä') character set latin1,
	s set('a', 'Ä') character set latin1,
	j json,
	vb varbinary(10),
	n int,
	d decimal(10, 2),
	dt datetime,
	bt bit(8)
) Engine=InnoDB;
`

var vschema = &vschemapb.Keyspace{
	Sharded:  true,
	Vindexes: map[string]*vschemapb.Vindex{"hash": {Type: "hash"}},
	Tables: map[string]*vschemapb.Table{
		"rawtext": {ColumnVindexes: []*vschemapb.ColumnVindex{{Column: "id", Name: "hash"}}},
	},
}

var inserts = []string{
	"insert into rawtext (id, l1, l1t, gbk, sjis, ujis, euckr, gb18030, big5, tis, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt) values (1, 'Müller', 'Müller', '中文', '日本', '日本', '한국', '中文😀', '中文', 'ไทย', 'Ü日', 'Ü😀', 'é', 'Ü日', 'Ü😀', 'Ä', 'é', 'Ä', 'a,Ä', '{\"a\": \"Müller\"}', x'C3BC', 1, 1.5, '2024-02-29 12:00:00', b'1')",
	"insert into rawtext (id, l1, l1t, gbk, sjis, ujis, euckr, gb18030, big5, tis, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt) values (2, 'Ärger', 'abc', '汉字', 'カナ', 'カナ', '문자', '汉字', '漢字', 'ก', 'é', 'ü', 'ö', 'ß', 'ä', 'Ö', 'ü', 'a', 'a', '[1]', x'41', 2, 2.5, '2024-03-01 00:00:00', b'0')",
	"insert into rawtext (id, l1, l1t, gbk, sjis, ujis, euckr, gb18030, big5, tis, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt) values (3, 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'a', 'a', 'a', 'a', 'null', x'00', 3, 3.5, '2024-03-02 00:00:00', b'11')",
	"insert into rawtext (id) values (4)",
}

// queries are run with a MySQL client of vtgate. Some run on both shards and
// are merged and sorted by vtgate, which compares the text with the collation
// in the fields.
var queries = []string{
	"select id, l1, l1t, gbk, sjis, ujis, euckr, gb18030, big5, tis, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt from rawtext order by id",
	"select id, l1, l1t, gbk, sjis, ujis, euckr, gb18030, big5, tis, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt from rawtext where 1 != 1",
	"select id, l1, l1t, gbk, sjis, ujis, euckr, gb18030, big5, tis, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt from rawtext where id = 1",
	"select l1, gbk from rawtext order by l1",
	"select l1, count(*) from rawtext group by l1 order by l1",
	"select max(gbk), min(l1), group_concat(sjis order by id) from rawtext",
	"select concat(l1, 'x'), upper(gbk), convert(sjis using utf8mb4), cast(u4 as char(1)), json_object('a', l1), j->>'$.a', hex(l1) from rawtext where id = 1",
	"select distinct e from rawtext order by e",
	"select l1 from rawtext where l1 = 'müller'",
	"select 'Müller', _latin1 x'FC', concat(_gbk x'D6D0', 'x') from dual",
	"show full columns from rawtext",
	// select @@character_set_results is left out: vtgate sends it to a tablet,
	// and the session of a tablet with the flag has NULL.
}

// describe writes every part of the fields and the bytes of the values.
func describe(qr *sqltypes.Result) string {
	var s strings.Builder
	for _, f := range qr.Fields {
		fmt.Fprintf(&s, "field %q %q %q %q %q %v collation=%d length=%d flags=%d decimals=%d\n", f.Name, f.OrgName, f.Table, f.OrgTable, f.Database, f.Type, f.Charset, f.ColumnLength, f.Flags, f.Decimals)
	}
	for _, row := range qr.Rows {
		for _, v := range row {
			if v.IsNull() {
				s.WriteString("NULL ")
				continue
			}
			fmt.Fprintf(&s, "%v:%X ", v.Type(), v.Raw())
		}
		s.WriteString("\n")
	}
	return s.String()
}

// results starts vtcombo, with --queryserver-config-raw-result-charset when
// raw is true, and returns the result of every query, each with no
// transaction, in a transaction, on a reserved connection, with a system
// setting, and streamed.
func results(t *testing.T, charset string, raw bool) map[string]string {
	cfg := vttest.Config{
		Charset: charset,
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{{
				Name:   "ks",
				Shards: []*vttestpb.Shard{{Name: "-80"}, {Name: "80-"}},
			}},
		},
		EnableSystemSettings: true,
	}
	require.NoError(t, cfg.InitSchemas("ks", schema, vschema))
	defer os.RemoveAll(cfg.SchemaDir)

	queryServerArgs := vttest.QueryServerArgs
	if raw {
		vttest.QueryServerArgs = append(slices.Clone(queryServerArgs), "--queryserver-config-raw-result-charset")
	}
	cluster := &vttest.LocalCluster{Config: cfg}
	err := cluster.Setup()
	vttest.QueryServerArgs = queryServerArgs
	defer cluster.TearDown()
	require.NoError(t, err)

	params := mysql.ConnParams{Host: "localhost", Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", "")}
	conn, err := mysql.Connect(t.Context(), &params)
	require.NoError(t, err)
	defer conn.Close()
	for _, insert := range inserts {
		_, err := conn.ExecuteFetch(insert, 0, false)
		require.NoError(t, err)
	}

	out := map[string]string{}
	modes := map[string][]string{
		"plain":       nil,
		"transaction": {"begin"},
		"reserved":    {"select get_lock('rawresults', 1)"},
		"setting":     {"set sql_safe_updates = 1"},
		"olap":        {"set workload = olap"},
	}
	for mode, setup := range modes {
		conn, err := mysql.Connect(t.Context(), &params)
		require.NoError(t, err)
		for _, query := range setup {
			_, err := conn.ExecuteFetch(query, 1000, false)
			require.NoError(t, err, query)
		}
		for _, query := range queries {
			qr, err := conn.ExecuteFetch(query, 1000, true)
			if err != nil {
				out[mode+" "+query] = "error: " + err.Error()
				continue
			}
			out[mode+" "+query] = describe(qr)
		}
		conn.Close()
	}
	return out
}

// TestVtcomboRawResultCharset checks that a MySQL client of vtgate gets the
// same result, in every part of every field and in every byte, from a vtcombo
// with --queryserver-config-raw-result-charset as from one without it, for
// columns in a dozen character sets, sorted and merged by vtgate across two
// shards, in transactions, on reserved connections, with system settings and
// streamed. It does so for the default --db-charset utf8mb4, and for
// utf8mb4_general_ci, whose collation the fields of a tablet without the flag
// have.
func TestVtcomboRawResultCharset(t *testing.T) {
	for _, charset := range []string{"utf8mb4", "utf8mb4_general_ci"} {
		t.Run(charset, func(t *testing.T) {
			raw := results(t, charset, true)
			plain := results(t, charset, false)
			require.Len(t, raw, len(plain))
			for key, want := range plain {
				assert.Equal(t, want, raw[key], key)
			}
		})
	}
}

const sortedSchema = `create table sorted (
	id int primary key,
	l varchar(10) character set latin1 collate latin1_german2_ci,
	g varchar(10) character set gbk
) Engine=InnoDB;
`

// TestVtgateEvaluatesColumnCollations checks that a vtgate with
// --convert-result-charset sorts, groups, counts the distinct values of,
// filters and joins on a latin1 and a gbk column across two shards in the
// collation of the column, as MySQL does. It does so with tablets that have
// --queryserver-config-raw-result-charset, which send the text of a column
// in the character set of the column, and with tablets that do not, which
// send it in utf8mb4. The expected values are the ones of the same table and
// rows on one MySQL 8.0.46 and 8.4.13 server: in latin1_german2_ci 'é' sorts
// between 'd' and 'f' and 'Müller' equals 'MUELLER', where the utf8mb4 bytes
// of 'é' taken as latin1 sort before 'd' and those of 'Müller' differ from
// 'MUELLER'. The rows with 'Müller' and with 'MUELLER' are on different
// shards.
func TestVtgateEvaluatesColumnCollations(t *testing.T) {
	t.Run("tablets with the flag", func(t *testing.T) {
		vtgateEvaluatesColumnCollations(t, "--queryserver-config-raw-result-charset")
	})
	t.Run("tablets without the flag", func(t *testing.T) {
		vtgateEvaluatesColumnCollations(t)
	})
}

func vtgateEvaluatesColumnCollations(t *testing.T, tabletArgs ...string) {
	cfg := vttest.Config{
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{{
				Name:   "ks",
				Shards: []*vttestpb.Shard{{Name: "-80"}, {Name: "80-"}},
			}},
		},
	}
	require.NoError(t, cfg.InitSchemas("ks", sortedSchema, &vschemapb.Keyspace{
		Sharded:  true,
		Vindexes: map[string]*vschemapb.Vindex{"hash": {Type: "hash"}},
		Tables: map[string]*vschemapb.Table{
			"sorted": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{Column: "id", Name: "hash"}},
				// With the collations of the columns in the vschema, vtgate
				// compares their text itself. Without them it asks MySQL for
				// the weight string of each value and compares those.
				Columns: []*vschemapb.Column{
					{Name: "id", Type: sqltypes.Int32},
					{Name: "l", Type: sqltypes.VarChar, CollationName: "latin1_german2_ci"},
					{Name: "g", Type: sqltypes.VarChar, CollationName: "gbk_chinese_ci"},
				},
				ColumnListAuthoritative: true,
			},
		},
	}))
	t.Cleanup(func() { os.RemoveAll(cfg.SchemaDir) })

	queryServerArgs := vttest.QueryServerArgs
	vttest.QueryServerArgs = append(append(slices.Clone(queryServerArgs), "--convert-result-charset"), tabletArgs...)
	cluster := &vttest.LocalCluster{Config: cfg}
	err := cluster.Setup()
	vttest.QueryServerArgs = queryServerArgs
	t.Cleanup(func() { _ = cluster.TearDown() })
	require.NoError(t, err)

	params := mysql.ConnParams{Host: "localhost", Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", "")}
	conn, err := mysql.Connect(t.Context(), &params)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	_, err = conn.ExecuteFetch("insert into sorted (id, l, g) values (1, 'é', '中'), (2, 'd', '波'), (3, 'f', '啊'), (4, 'Müller', '中'), (5, 'MUELLER', 'abc'), (6, 'z', 'ABC')", 0, false)
	require.NoError(t, err)

	// column returns the values of one column of the result of query.
	column := func(query string, col int) []string {
		qr, err := conn.ExecuteFetch(query, 100, false)
		require.NoError(t, err, query)
		var values []string
		for _, row := range qr.Rows {
			values = append(values, row[col].ToString())
		}
		return values
	}

	for _, workload := range []string{"oltp", "olap"} {
		_, err := conn.ExecuteFetch("set workload = "+workload, 0, false)
		require.NoError(t, err)

		assert.Equal(t, []string{"2", "1", "3", "4", "5", "6"}, column("select id, l from sorted order by l, id", 0), workload)
		assert.Equal(t, []string{"5", "6", "3", "2", "1", "4"}, column("select id, g from sorted order by g, id", 0), workload)

		assert.Equal(t, []string{"5"}, column("select count(distinct l) from sorted", 0), workload)
		assert.Equal(t, []string{"4"}, column("select count(distinct g) from sorted", 0), workload)

		assert.Equal(t, []string{"1", "1", "1", "2", "1"}, column("select l, count(*) from sorted group by l order by l", 1), workload)
		assert.Equal(t, []string{"2", "1", "1", "2"}, column("select g, count(*) from sorted group by g order by g", 1), workload)

		assert.Len(t, column("select distinct l from sorted", 0), 5, workload)
		assert.Len(t, column("select distinct g from sorted", 0), 4, workload)

		// vtgate groups the rows of the shards, and evaluates what is done
		// with the groups: a HAVING that needs the count, and the
		// expressions of the query around the grouped one. 'é' is not less
		// than 'e', and its utf8mb4 bytes taken as latin1 are.
		assert.Equal(t, []string{"d"}, column("select l, count(*) from sorted group by l having l < 'e' or count(*) > 100", 0), workload)
		assert.Equal(t, []string{"2"}, column("select l, count(*) from sorted group by l having l = 'mueller' or count(*) > 100", 1), workload)
		assert.Equal(t, []string{"2"}, column("select g, count(*) from sorted group by g having g = 'abc' or count(*) > 100", 1), workload)
		assert.ElementsMatch(t, []string{"1", "0", "0", "0", "0"}, column("select t.l < 'e' from (select l, count(*) from sorted group by l) as t", 0), workload)
		assert.ElementsMatch(t, []string{"0", "0", "0", "1", "0"}, column("select t.l = 'mueller' from (select l, count(*) from sorted group by l) as t", 0), workload)

		// Both sides have a LIMIT, which vtgate applies, so it joins them
		// itself, with a hash of the column.
		assert.ElementsMatch(t,
			[]string{"1-1", "2-2", "3-3", "4-4", "4-5", "5-4", "5-5", "6-6"},
			column("select concat(a.id, '-', b.id) from (select id, l from sorted order by id limit 10) as a join (select id, l from sorted order by id limit 10) as b on a.l = b.l", 0), workload)
	}
}
