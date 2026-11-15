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
	l1 varchar(10) character set latin1 collate latin1_swedish_ci,
	l1t text character set latin1,
	gb2312 varchar(10) character set gb2312,
	sjis varchar(10) character set sjis,
	ujis varchar(10) character set ujis,
	euckr varchar(10) character set euckr,
	gb18030 varchar(10) character set gb18030,
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
	"insert into rawtext (id, l1, l1t, gb2312, sjis, ujis, euckr, gb18030, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt) values (1, 'Müller', 'Müller', '中文', '日本', '日本', '한국', '中文😀', 'Ü日', 'Ü😀', 'é', 'Ü日', 'Ü😀', 'Ä', 'é', 'Ä', 'a,Ä', '{\"a\": \"Müller\"}', x'C3BC', 1, 1.5, '2024-02-29 12:00:00', b'1')",
	"insert into rawtext (id, l1, l1t, gb2312, sjis, ujis, euckr, gb18030, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt) values (2, 'Ärger', 'abc', '汉字', 'カナ', 'カナ', '문자', '汉字', 'é', 'ü', 'ö', 'ß', 'ä', 'Ö', 'ü', 'a', 'a', '[1]', x'41', 2, 2.5, '2024-03-01 00:00:00', b'0')",
	"insert into rawtext (id, l1, l1t, gb2312, sjis, ujis, euckr, gb18030, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt) values (3, 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'a', 'a', 'a', 'a', 'null', x'00', 3, 3.5, '2024-03-02 00:00:00', b'11')",
	"insert into rawtext (id) values (4)",
}

// queries are run with a MySQL client of vtgate. Some run on both shards and
// are merged and sorted by vtgate, which compares the text with the collation
// in the fields.
var queries = []string{
	"select id, l1, l1t, gb2312, sjis, ujis, euckr, gb18030, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt from rawtext order by id",
	"select id, l1, l1t, gb2312, sjis, ujis, euckr, gb18030, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt from rawtext where 1 != 1",
	"select id, l1, l1t, gb2312, sjis, ujis, euckr, gb18030, u3, u4b, u4, uc, u32, c, lt, e, s, j, vb, n, d, dt, bt from rawtext where id = 1",
	"select l1, gb2312 from rawtext order by l1",
	"select l1, count(*) from rawtext group by l1 order by l1",
	"select max(gb2312), min(l1), group_concat(sjis order by id) from rawtext",
	"select concat(l1, 'x'), upper(gb2312), convert(sjis using utf8mb4), cast(u4 as char(1)), json_object('a', l1), j->>'$.a', hex(l1) from rawtext where id = 1",
	"select distinct e from rawtext order by e",
	"select l1 from rawtext where l1 = 'müller'",
	"select 'Müller', _latin1 x'FC', concat(_gb2312 x'D6D0', 'x') from dual",
	"show full columns from rawtext",
	// select @@character_set_results is left out: vtgate sends it to a tablet,
	// and the session of a tablet with the flag has NULL.
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
	return s
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
