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

// Package columncollation checks that vtgate sorts, groups, filters and joins
// the text of a column that is not utf8mb4 in the collation of the column.
package columncollation

import (
	"os"
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

const sortedSchema = `create table sorted (
	id int primary key,
	l varchar(10) character set latin1 collate latin1_german2_ci,
	g varchar(10) character set gbk
) Engine=InnoDB;
`

// TestVtgateEvaluatesColumnCollations checks that vtgate sorts, groups, counts
// the distinct values of, filters and joins on a latin1 and a gbk column
// across two shards in the collation of the column, as MySQL does. A tablet
// sends the text of every column in utf8mb4. The expected values are the ones
// of the same table and rows on one MySQL 8.0.46 server: in latin1_german2_ci
// 'é' sorts between 'd' and 'f' and 'Müller' equals 'MUELLER', where the
// utf8mb4 bytes of 'é' taken as latin1 sort before 'd' and those of 'Müller'
// differ from 'MUELLER'. The rows with 'Müller' and with 'MUELLER' are on
// different shards.
func TestVtgateEvaluatesColumnCollations(t *testing.T) {
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

	cluster := &vttest.LocalCluster{Config: cfg}
	err := cluster.Setup()
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

		// 'é' equals 'e', which is not less than 'd'.
		assert.Equal(t, []string{"d"}, column("select min(l), max(l) from sorted", 0), workload)
		assert.Equal(t, []string{"z"}, column("select min(l), max(l) from sorted", 1), workload)
		assert.Equal(t, []string{"é"}, column("select min(l), max(l) from sorted where id in (1, 2)", 1), workload)

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
