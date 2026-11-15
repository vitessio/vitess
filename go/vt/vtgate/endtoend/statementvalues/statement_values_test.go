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

// Package statementvalues runs vtcombo with --convert-result-charset and
// --queryserver-config-raw-result-charset, and checks the statements in which
// vtgate puts a value of a row that it read from a tablet: the other side of a
// join, a subquery, an INSERT ... SELECT, a lookup vindex, a DML with input, a
// recursive CTE, a foreign key cascade and a user-defined variable. With the
// two flags the text of a row is in the character set of its column inside
// vtgate, and every statement is in utf8mb4, so each such value has to be
// converted before it goes into the statement.
package statementvalues

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}

const latin1 = "character set latin1 collate latin1_swedish_ci"

var shardedSchema = strings.NewReplacer("LATIN1", latin1).Replace(`
create table a (id int primary key, l varchar(10) LATIN1, g varchar(10) character set gb2312) Engine=InnoDB;
create table b (id int primary key, l varchar(10) LATIN1, g varchar(10) character set gb2312, n varchar(10) LATIN1) Engine=InnoDB;
create table c (id int primary key, l varchar(10) LATIN1, g varchar(10) character set gb2312) Engine=InnoDB;
create table lk (id int primary key, l varchar(10) LATIN1) Engine=InnoDB;
create table p (l varchar(10) LATIN1 primary key, n int) Engine=InnoDB;
create table d (id int primary key, u varchar(10) character set utf8mb4, l varchar(10) LATIN1) Engine=InnoDB;
`)

var unshardedSchema = strings.NewReplacer("LATIN1", latin1).Replace(`
create table u (id int primary key, l varchar(10) LATIN1, g varchar(10) character set gb2312) Engine=InnoDB;
create table lk_l_lookup (l varchar(10) LATIN1 primary key, keyspace_id varbinary(10)) Engine=InnoDB;
create table fkp (id int primary key, l varchar(10) LATIN1, unique key (l)) Engine=InnoDB;
create table fkc (id int primary key, l varchar(10) LATIN1, foreign key (l) references fkp (l) on delete cascade on update cascade) Engine=InnoDB;
`)

var hashOnID = []*vschemapb.ColumnVindex{{Column: "id", Name: "hash"}}

// shardedVSchema shards every table by a hash of id, which puts the ids 1, 2,
// 3 and 5 on -80 and 4 and 6 on 80-, except p, which is sharded by an xxhash
// of its latin1 primary key. lk has an owned lookup vindex on its latin1
// column, with batch_lookup, so that the lookup matches the rows of its
// table to the values it looks up.
var shardedVSchema = &vschemapb.Keyspace{
	Sharded: true,
	Vindexes: map[string]*vschemapb.Vindex{
		"hash":   {Type: "hash"},
		"xxhash": {Type: "xxhash"},
		"lk_l": {
			Type:   "consistent_lookup_unique",
			Params: map[string]string{"table": "uks.lk_l_lookup", "from": "l", "to": "keyspace_id", "batch_lookup": "true"},
			Owner:  "lk",
		},
	},
	Tables: map[string]*vschemapb.Table{
		"a":  {ColumnVindexes: hashOnID},
		"b":  {ColumnVindexes: hashOnID},
		"c":  {ColumnVindexes: hashOnID},
		"d":  {ColumnVindexes: hashOnID},
		"lk": {ColumnVindexes: []*vschemapb.ColumnVindex{{Column: "id", Name: "hash"}, {Column: "l", Name: "lk_l"}}},
		"p":  {ColumnVindexes: []*vschemapb.ColumnVindex{{Column: "l", Name: "xxhash"}}},
	},
}

// unshardedVSchema has vtgate manage the foreign keys, so that it executes
// the cascades itself.
var unshardedVSchema = &vschemapb.Keyspace{
	ForeignKeyMode: vschemapb.Keyspace_managed,
	Tables:         map[string]*vschemapb.Table{"u": {}, "lk_l_lookup": {}, "fkp": {}, "fkc": {}},
}

var inserts = []string{
	"insert into a (id, l, g) values (1, 'Müller', '中文'), (2, 'é', '汉字'), (3, 'abc', 'abc'), (4, 'MYLLER', '中文'), (5, 'ß', '字'), (6, 'z', 'ABC')",
	"insert into b (id, l, g, n) values (1, 'é', '中文', 'ß'), (2, 'Müller', '汉字', 'é'), (4, 'zz', '中文', NULL), (5, 'SS', 'abc', NULL), (6, 'MYLLER', '字', NULL)",
	"insert into d (id, u, l) values (1, 'é', 'x'), (2, 'ß', 'é')",
	"insert into uks.u (id, l, g) values (1, 'é', '字'), (2, 'Müller', '中文')",
	"insert into uks.fkp (id, l) values (1, 'Müller'), (2, 'é'), (3, 'ß')",
	"insert into uks.fkc (id, l) values (1, 'Müller'), (2, 'é'), (3, 'ß')",
}

// TestStatementValues runs each statement on vtgate and compares what a
// utf8mb4 client reads back, the text and HEX() of each column, which shows
// what MySQL stored, with the result of the same statements on the same rows
// in one table each on one MySQL server, measured on MySQL 8.0.46 and 8.4.13,
// which agree. In latin1_swedish_ci 'Müller' equals 'MYLLER' and 'ß' equals
// 'SS', and in gb2312_chinese_ci 'abc' equals 'ABC', so that a statement that
// MySQL compares in the collation of the column finds those rows too.
//
// Without the conversion of the values of the rows, the statements get the
// latin1 and gb2312 bytes as utf8mb4: MySQL refuses most of them with error 1267,
// "Illegal mix of collations", and stores '?' or finds nothing for others.
func TestStatementValues(t *testing.T) {
	cfg := vttest.Config{
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{Name: "ks", Shards: []*vttestpb.Shard{{Name: "-80"}, {Name: "80-"}}},
				{Name: "uks", Shards: []*vttestpb.Shard{{Name: "0"}}},
			},
		},
	}
	require.NoError(t, cfg.InitSchemas("ks", shardedSchema, shardedVSchema))
	require.NoError(t, cfg.InitSchemas("uks", unshardedSchema, unshardedVSchema))
	t.Cleanup(func() { os.RemoveAll(cfg.SchemaDir) })

	queryServerArgs := vttest.QueryServerArgs
	vttest.QueryServerArgs = append(slices.Clone(queryServerArgs), "--convert-result-charset", "--queryserver-config-raw-result-charset")
	cluster := &vttest.LocalCluster{Config: cfg}
	err := cluster.Setup()
	vttest.QueryServerArgs = queryServerArgs
	t.Cleanup(func() { _ = cluster.TearDown() })
	require.NoError(t, err)

	params := mysql.ConnParams{Host: "localhost", Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", "")}
	conn, err := mysql.Connect(t.Context(), &params)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	// A multi-table DELETE needs the primary key of its table, which vtgate
	// learns from the schema tracker, and the managed foreign keys need the
	// foreign keys.
	assert.Eventually(t, func() bool {
		_, err := conn.ExecuteFetch("vexplain plan delete p from p join a on p.l = a.l", 1, false)
		return err == nil
	}, 60*time.Second, 100*time.Millisecond)

	for _, insert := range inserts {
		_, err := conn.ExecuteFetch(insert, 0, false)
		require.NoError(t, err, insert)
	}

	// rows returns the rows of the result of query, with the values of a row
	// separated by |.
	rows := func(t *testing.T, conn *mysql.Conn, query string) []string {
		t.Helper()
		qr, err := conn.ExecuteFetch(query, 1000, false)
		require.NoError(t, err, query)
		var out []string
		for _, row := range qr.Rows {
			values := make([]string, 0, len(row))
			for _, v := range row {
				if v.IsNull() {
					values = append(values, "NULL")
					continue
				}
				values = append(values, v.ToString())
			}
			out = append(out, strings.Join(values, "|"))
		}
		return out
	}
	exec := func(t *testing.T, conn *mysql.Conn, query string) uint64 {
		t.Helper()
		qr, err := conn.ExecuteFetch(query, 1000, false)
		require.NoError(t, err, query)
		return qr.RowsAffected
	}
	// plans checks that vtgate executes query with operator itself, and does
	// not send it to MySQL as one statement.
	plans := func(t *testing.T, query, operator string) {
		t.Helper()
		plan := rows(t, conn, "vexplain plan "+query)
		require.Len(t, plan, 1)
		assert.Contains(t, plan[0], fmt.Sprintf(`"OperatorType": %q`, operator), query)
	}

	cte := "with recursive r (id, l) as (select id, l from a where id = 1 union all select b.id, b.n from r join b on b.l = r.l) "
	reads := []struct {
		name     string
		query    string
		operator string
		want     []string
		// sorted sorts the rows, for a query that vtgate cannot order without
		// a projection.
		sorted bool
		// oltp leaves out the streaming execution.
		oltp bool
	}{{
		name:     "join on a latin1 column",
		query:    "select a.id, b.id, a.l, hex(a.l), b.l, hex(b.l) from a join b on a.l = b.l order by a.id, b.id",
		operator: "Join",
		want: []string{
			"1|2|Müller|4DFC6C6C6572|Müller|4DFC6C6C6572",
			"1|6|Müller|4DFC6C6C6572|MYLLER|4D55454C4C4552",
			"2|1|é|E9|é|E9",
			"4|2|MYLLER|4D55454C4C4552|Müller|4DFC6C6C6572",
			"4|6|MYLLER|4D55454C4C4552|MYLLER|4D55454C4C4552",
			"5|5|ß|DF|SS|5353",
		},
	}, {
		name:     "join on a gb2312 column",
		query:    "select a.id, b.id, a.g, hex(a.g), b.g, hex(b.g) from a join b on a.g = b.g order by a.id, b.id",
		operator: "Join",
		want: []string{
			"1|1|中文|D6D0CEC4|中文|D6D0CEC4",
			"1|4|中文|D6D0CEC4|中文|D6D0CEC4",
			"2|2|汉字|BABAD7D6|汉字|BABAD7D6",
			"3|5|abc|616263|abc|616263",
			"4|1|中文|D6D0CEC4|中文|D6D0CEC4",
			"4|4|中文|D6D0CEC4|中文|D6D0CEC4",
			"5|6|字|D7D6|字|D7D6",
			"6|5|ABC|414243|abc|616263",
		},
	}, {
		name:     "join across keyspaces",
		query:    "select a.id, u.id, u.l, hex(u.l), u.g, hex(u.g) from a join uks.u as u on a.l = u.l order by a.id, u.id",
		operator: "Join",
		want: []string{
			"1|2|Müller|4DFC6C6C6572|中文|D6D0CEC4",
			"2|1|é|E9|字|D7D6",
			"4|2|Müller|4DFC6C6C6572|中文|D6D0CEC4",
		},
	}, {
		name:     "in a subquery of a latin1 column",
		query:    "select id, l, hex(l) from a where l in (select l from b) order by id",
		operator: "UncorrelatedSubquery",
		want:     []string{"1|Müller|4DFC6C6C6572", "2|é|E9", "4|MYLLER|4D55454C4C4552", "5|ß|DF"},
	}, {
		name:     "in a subquery of a gb2312 column",
		query:    "select id, g, hex(g) from a where g in (select g from b where id < 3) order by id",
		operator: "UncorrelatedSubquery",
		want:     []string{"1|中文|D6D0CEC4", "2|汉字|BABAD7D6", "4|中文|D6D0CEC4"},
	}, {
		name:     "not in a subquery of a gb2312 column",
		query:    "select id, g, hex(g) from a where g not in (select g from b where id < 3) order by id",
		operator: "UncorrelatedSubquery",
		want:     []string{"3|abc|616263", "5|字|D7D6", "6|ABC|414243"},
	}, {
		name:     "equal to a subquery",
		query:    "select id, l, hex(l) from a where l = (select l from b where id = 2) order by id",
		operator: "UncorrelatedSubquery",
		want:     []string{"1|Müller|4DFC6C6C6572", "4|MYLLER|4D55454C4C4552"},
	}, {
		// A streamed semi-join whose subquery is a scatter with a LIMIT
		// fails with "stream send error: EOF" also for ASCII values and
		// without the flags, so it runs in oltp only.
		name:     "exists on a latin1 column",
		query:    "select id, l, hex(l) from a where exists (select 1 from b where b.l = a.l) order by id",
		operator: "SemiJoin",
		want:     []string{"1|Müller|4DFC6C6C6572", "2|é|E9", "4|MYLLER|4D55454C4C4552", "5|ß|DF"},
		oltp:     true,
	}, {
		name:     "exists on a gb2312 column",
		query:    "select id, g, hex(g) from a where exists (select 1 from b where b.g = a.g) order by id",
		operator: "SemiJoin",
		want:     []string{"1|中文|D6D0CEC4", "2|汉字|BABAD7D6", "3|abc|616263", "4|中文|D6D0CEC4", "5|字|D7D6", "6|ABC|414243"},
		oltp:     true,
	}, {
		// The recursion goes from 'Müller' of a on -80 to 'Müller' and
		// 'MYLLER' of b on -80 and 80-, to 'é' and then to 'SS', which
		// equals 'ß'. The text of l is read without a projection: vtgate
		// returns the text of a column that its projection passes through
		// as utf8mb4 without converting it, which is not what this test is
		// about.
		name:     "recursive CTE",
		query:    cte + "select id, l from r",
		operator: "RecurseCTE",
		want:     []string{"1|Müller", "1|ß", "2|é", "5|NULL", "6|NULL"},
		sorted:   true,
	}, {
		name:     "recursive CTE, the bytes",
		query:    cte + "select id, hex(l) from r",
		operator: "RecurseCTE",
		want:     []string{"1|4DFC6C6C6572", "1|DF", "2|E9", "5|NULL", "6|NULL"},
		sorted:   true,
	}, {
		// The columns of a recursive CTE are the ones of its seed: the 'ß'
		// that the recursion reads from the utf8mb4 column d.u is returned
		// in latin1, the character set of a.l.
		name:     "recursive CTE, a latin1 seed and a utf8mb4 recursion",
		query:    "with recursive r (id, l) as (select id, l from a where id = 2 union all select d.id, d.u from r join d on d.l = r.l) select id, l from r",
		operator: "RecurseCTE",
		want:     []string{"2|ß", "2|é"},
		sorted:   true,
	}, {
		name:     "recursive CTE, a latin1 seed and a utf8mb4 recursion, the bytes",
		query:    "with recursive r (id, l) as (select id, l from a where id = 2 union all select d.id, d.u from r join d on d.l = r.l) select id, hex(l) from r",
		operator: "RecurseCTE",
		want:     []string{"2|DF", "2|E9"},
		sorted:   true,
	}, {
		// The other way: the 'ß' of the latin1 column b.n is returned in
		// utf8mb4, the character set of d.u.
		name:     "recursive CTE, a utf8mb4 seed and a latin1 recursion",
		query:    "with recursive r as (select id, u as l from d where id = 1 union all select b.id, b.n from r join b on b.l = r.l) select id, l from r",
		operator: "RecurseCTE",
		want:     []string{"1|ß", "1|é", "5|NULL"},
		sorted:   true,
	}, {
		name:     "recursive CTE, a utf8mb4 seed and a latin1 recursion, the bytes",
		query:    "with recursive r as (select id, u as l from d where id = 1 union all select b.id, b.n from r join b on b.l = r.l) select id, hex(l) from r",
		operator: "RecurseCTE",
		want:     []string{"1|C39F", "1|C3A9", "5|NULL"},
		sorted:   true,
	}}
	for _, workload := range []string{"oltp", "olap"} {
		_, err := conn.ExecuteFetch("set workload = "+workload, 0, false)
		require.NoError(t, err)
		for _, tc := range reads {
			if tc.oltp && workload == "olap" {
				continue
			}
			t.Run(workload+" "+tc.name, func(t *testing.T) {
				plans(t, tc.query, tc.operator)
				got := rows(t, conn, tc.query)
				if tc.sorted {
					slices.Sort(got)
				}
				assert.Equal(t, tc.want, got)
			})
		}
	}

	// The INSERT ... SELECT runs once buffered and once streamed.
	t.Run("insert select", func(t *testing.T) {
		for _, workload := range []string{"oltp", "olap"} {
			_, err := conn.ExecuteFetch("set workload = "+workload, 0, false)
			require.NoError(t, err)
			offset := map[string]int{"oltp": 10, "olap": 20}[workload]
			query := fmt.Sprintf("insert into c (id, l, g) select id + %d, l, g from a", offset)
			plans(t, query, "Insert")
			assert.EqualValues(t, 6, exec(t, conn, query), workload)
		}
		_, err := conn.ExecuteFetch("set workload = oltp", 0, false)
		require.NoError(t, err)
		assert.Equal(t, []string{
			"11|Müller|4DFC6C6C6572|中文|D6D0CEC4",
			"12|é|E9|汉字|BABAD7D6",
			"13|abc|616263|abc|616263",
			"14|MYLLER|4D55454C4C4552|中文|D6D0CEC4",
			"15|ß|DF|字|D7D6",
			"16|z|7A|ABC|414243",
			"21|Müller|4DFC6C6C6572|中文|D6D0CEC4",
			"22|é|E9|汉字|BABAD7D6",
			"23|abc|616263|abc|616263",
			"24|MYLLER|4D55454C4C4552|中文|D6D0CEC4",
			"25|ß|DF|字|D7D6",
			"26|z|7A|ABC|414243",
		}, rows(t, conn, "select id, l, hex(l), g, hex(g) from c order by id"))
	})

	// The multi-table UPDATE reads the ids of c and the new value, the gb2312
	// '汉字' of b, which goes into the UPDATE of c as a bind variable.
	t.Run("multi-table update", func(t *testing.T) {
		query := "update c join b on c.l = b.l set c.g = b.g where b.id = 2"
		plans(t, query, "DMLWithInput")
		assert.EqualValues(t, 4, exec(t, conn, query))
		assert.Equal(t, []string{
			"11|Müller|4DFC6C6C6572|汉字|BABAD7D6",
			"12|é|E9|汉字|BABAD7D6",
			"13|abc|616263|abc|616263",
			"14|MYLLER|4D55454C4C4552|汉字|BABAD7D6",
			"15|ß|DF|字|D7D6",
			"16|z|7A|ABC|414243",
			"21|Müller|4DFC6C6C6572|汉字|BABAD7D6",
			"22|é|E9|汉字|BABAD7D6",
			"23|abc|616263|abc|616263",
			"24|MYLLER|4D55454C4C4552|汉字|BABAD7D6",
			"25|ß|DF|字|D7D6",
			"26|z|7A|ABC|414243",
		}, rows(t, conn, "select id, l, hex(l), g, hex(g) from c order by id"))
	})

	// The multi-table DELETE reads the latin1 primary keys of p, which go
	// into the DELETE of p as a list, and to its xxhash vindex, which routes
	// them to their shards.
	t.Run("multi-table delete", func(t *testing.T) {
		exec(t, conn, "insert into p (l, n) values ('Müller', 1), ('é', 2), ('abc', 3), ('ß', 4)")
		query := "delete p from p join a on p.l = a.l where a.id in (1, 5)"
		plans(t, query, "DMLWithInput")
		assert.EqualValues(t, 2, exec(t, conn, query))
		assert.Equal(t, []string{"é|E9|2", "abc|616263|3"}, rows(t, conn, "select l, hex(l), n from p order by n"))
	})

	// The rows of lk come from an INSERT and an INSERT ... SELECT, whose
	// values go into the lookup table. An UPDATE and a DELETE read the old
	// values and delete them from the lookup table. A SELECT, an UPDATE and a
	// DELETE by l look up the keyspace ID.
	t.Run("lookup vindex", func(t *testing.T) {
		assert.EqualValues(t, 3, exec(t, conn, "insert into lk (id, l) values (1, 'Müller'), (4, 'é'), (2, 'abc')"))
		assert.EqualValues(t, 2, exec(t, conn, "insert into lk (id, l) select id + 20, l from a where id in (5, 6)"))
		plans(t, "select id from lk where l = 'Müller'", "VindexLookup")
		assert.Equal(t, []string{"1"}, rows(t, conn, "select id from lk where l = 'Müller'"))
		assert.Equal(t, []string{"25"}, rows(t, conn, "select id from lk where l = 'ß'"))
		assert.Equal(t, []string{"4", "26"}, rows(t, conn, "select id from lk where l in ('é', 'z') order by id"))
		assert.EqualValues(t, 1, exec(t, conn, "update lk set l = 'Ärger' where id = 1"))
		assert.EqualValues(t, 1, exec(t, conn, "delete from lk where id = 4"))
		assert.EqualValues(t, 1, exec(t, conn, "update lk set l = 'Öl' where id = 25"))
		assert.EqualValues(t, 1, exec(t, conn, "update lk set l = 'Zahl' where l = 'Öl'"))
		assert.EqualValues(t, 1, exec(t, conn, "delete from lk where l = 'z'"))

		want := []string{"1|Ärger|C472676572", "2|abc|616263", "25|Zahl|5A61686C"}
		assert.Equal(t, want, rows(t, conn, "select id, l, hex(l) from lk order by id"))
		// The lookup table has a row for each row of lk, with its value and
		// the keyspace ID of its id.
		var lookup []string
		for _, row := range want {
			id, value, _ := strings.Cut(row, "|")
			lookup = append(lookup, fmt.Sprintf("%s|%X", value, keyspaceID(t, id)))
		}
		got := rows(t, conn, "select l, hex(l), hex(keyspace_id) from uks.lk_l_lookup")
		slices.Sort(got)
		slices.Sort(lookup)
		assert.Equal(t, lookup, got)
	})

	// vtgate reads the old values of fkp and puts them into the UPDATE and
	// DELETE of fkc, and for an UPDATE whose value is not a literal, the new
	// value that MySQL computed, 'ßä'.
	t.Run("foreign key cascade", func(t *testing.T) {
		for _, query := range []string{
			"update uks.fkp set l = 'Öl' where id = 1",
			"update uks.fkp set l = concat(l, 'ä') where id = 3",
			"delete from uks.fkp where id = 2",
		} {
			plans(t, query, "FkCascade")
			assert.EqualValues(t, 1, exec(t, conn, query), query)
		}
		want := []string{"1|Öl|D66C", "3|ßä|DFE4"}
		assert.Equal(t, want, rows(t, conn, "select id, l, hex(l) from uks.fkp order by id"))
		assert.Equal(t, want, rows(t, conn, "select id, l, hex(l) from uks.fkc order by id"))
	})

	// A shard evaluates the subquery of SET @x, and the value of @x becomes a
	// bind variable. The SET runs in the unsharded keyspace, which has the
	// row: in a sharded one vtgate sends it to any one shard.
	t.Run("user-defined variable", func(t *testing.T) {
		conn, err := mysql.Connect(t.Context(), &params)
		require.NoError(t, err)
		t.Cleanup(conn.Close)
		exec(t, conn, "use uks")
		exec(t, conn, "set @x = (select l from u where id = 2)")
		assert.Equal(t, []string{"Müller"}, rows(t, conn, "select @x"))
		assert.Equal(t, []string{"1|Müller|4DFC6C6C6572", "4|MYLLER|4D55454C4C4552"}, rows(t, conn, "select id, l, hex(l) from ks.a where l = @x order by id"))
	})
}

// keyspaceID returns the keyspace ID of the hash vindex for id.
func keyspaceID(t *testing.T, id string) []byte {
	vdx, err := vindexes.CreateVindex("hash", "hash", nil)
	require.NoError(t, err)
	dest, err := vdx.(vindexes.SingleColumn).Map(t.Context(), nil, []sqltypes.Value{sqltypes.NewVarChar(id)})
	require.NoError(t, err)
	require.Len(t, dest, 1)
	return dest[0].(key.DestinationKeyspaceID)
}
