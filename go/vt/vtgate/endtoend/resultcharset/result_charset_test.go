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

// Package resultcharset tests vtgate with --convert-result-charset and
// tablets with --queryserver-config-raw-result-charset against MySQL: MySQL
// protocol clients with utf8mb4 and utf8mb3 handshake collations and SET
// statements get the bytes, field collations and lengths that MySQL sends,
// and clients of other character sets are refused. The expected values were
// measured on MySQL 8.0.46 and 8.4.13 with the same table, and the two agree
// on all of them.
//
// The tablets read results with character_set_results = NULL, so vtgate knows
// the collation of each column, which character_set_results = NULL and binary
// need to send a latin1 or gb2312 column in its own character set.
package resultcharset

import (
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var vtParams mysql.ConnParams

const schema = `
create table rc (
	id int primary key,
	l varchar(10) character set latin1 collate latin1_swedish_ci,
	g varchar(10) character set gb2312,
	u4 varchar(10) character set utf8mb4,
	j json,
	vb varbinary(10),
	i int,
	unique key uk_l (l)
) default charset = utf8mb4;
`

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		vttest.QueryServerArgs = append(vttest.QueryServerArgs, "--convert-result-charset", "--queryserver-config-raw-result-charset")

		var cfg vttest.Config
		cfg.Topology = &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{{
				Name:   "ks",
				Shards: []*vttestpb.Shard{{Name: "0"}},
			}},
		}
		if err := cfg.InitSchemas("ks", schema, nil); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.RemoveAll(cfg.SchemaDir)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)

		cluster := &vttest.LocalCluster{Config: cfg}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			cluster.TearDown()
			return 1
		}
		defer cluster.TearDown()

		vtParams = mysql.ConnParams{
			Host:   "localhost",
			Port:   cluster.Env.PortForProtocol("vtcombo_mysql_port", ""),
			DbName: "ks",
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func connect(t *testing.T, handshake collations.ID) *mysql.Conn {
	params := vtParams
	params.Charset = handshake
	conn, err := mysql.Connect(t.Context(), &params)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

func exec(t *testing.T, conn *mysql.Conn, query string) {
	_, err := conn.ExecuteFetch(query, 1000, false)
	require.NoError(t, err, query)
}

func insertRow(t *testing.T) {
	conn := connect(t, collations.CollationUtf8mb4ID)
	exec(t, conn, "delete from rc")
	exec(t, conn, `insert into rc values (1, 'Müller', '中文', 'é€😀', '{"a": "é€😀"}', 0x00ff, 42)`)
}

// column is what MySQL sends for a column: the field's collation and length,
// and the value in hex.
type column struct {
	collation collations.ID
	length    uint32
	value     string
}

// The query selects id, l, g, u4, j, vb and i. MySQL sends id, vb and i the
// same way with every setting: 31 (cs 63, length 11), 00ff (cs 63, length
// 10) and 3432 (cs 63, length 11). It never converts a number, also not for
// ucs2, and keeps the binary collation and the length of a JSON column.
const query = "select id, l, g, u4, j, vb, i from rc where id = 1"

func TestResultCharset(t *testing.T) {
	insertRow(t)

	tests := []struct {
		name      string
		handshake collations.ID
		set       string
		// l, g and u4
		text [3]column
		// j
		json string
		// @@character_set_results, in hex, and "NULL"
		results string
	}{{
		name:      "utf8mb4 handshake",
		handshake: 255,
		text:      [3]column{{255, 40, "4dc3bc6c6c6572"}, {255, 40, "e4b8ade69687"}, {255, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		// The 4-byte character of u4 is a ?, as in every character set
		// other than utf8mb4.
		name:      "utf8mb3 handshake",
		handshake: 33,
		text:      [3]column{{33, 30, "4dc3bc6c6c6572"}, {33, 30, "e4b8ade69687"}, {33, 30, "c3a9e282ac3f"}},
		json:      "7b2261223a2022c3a9e282ac3f227d",
		results:   "757466386d6233",
	}, {
		name:      "utf8mb4_bin handshake",
		handshake: 46,
		text:      [3]column{{46, 40, "4dc3bc6c6c6572"}, {46, 40, "e4b8ade69687"}, {46, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		name:      "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
		handshake: 255,
		set:       "set names utf8mb4 collate utf8mb4_bin",
		text:      [3]column{{46, 40, "4dc3bc6c6c6572"}, {46, 40, "e4b8ade69687"}, {46, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		name:      "SET NAMES utf8mb3",
		handshake: 255,
		set:       "set names utf8mb3",
		text:      [3]column{{33, 30, "4dc3bc6c6c6572"}, {33, 30, "e4b8ade69687"}, {33, 30, "c3a9e282ac3f"}},
		json:      "7b2261223a2022c3a9e282ac3f227d",
		results:   "757466386d6233",
	}, {
		name:      "SET NAMES DEFAULT after a utf8mb3 handshake",
		handshake: 33,
		set:       "set names default",
		text:      [3]column{{255, 40, "4dc3bc6c6c6572"}, {255, 40, "e4b8ade69687"}, {255, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		name:      "SET character_set_results = latin1",
		handshake: 255,
		set:       "set character_set_results = latin1",
		text:      [3]column{{8, 10, "4dfc6c6c6572"}, {8, 10, "3f3f"}, {8, 10, "e9803f"}},
		json:      "7b2261223a2022e9803f227d",
		results:   "6c6174696e31",
	}, {
		name:      "SET character_set_results = gb2312",
		handshake: 255,
		set:       "set character_set_results = gb2312",
		text:      [3]column{{24, 20, "4da8b96c6c6572"}, {24, 20, "d6d0cec4"}, {24, 20, "a8a63f3f"}},
		json:      "7b2261223a2022a8a63f3f227d",
		results:   "676232333132",
	}, {
		name:      "SET character_set_results = sjis",
		handshake: 255,
		set:       "set character_set_results = sjis",
		text:      [3]column{{13, 20, "4d3f6c6c6572"}, {13, 20, "928695b6"}, {13, 20, "3f3f3f"}},
		json:      "7b2261223a20223f3f3f227d",
		results:   "736a6973",
	}, {
		name:      "SET character_set_results = ucs2",
		handshake: 255,
		set:       "set character_set_results = ucs2",
		text:      [3]column{{35, 20, "004d00fc006c006c00650072"}, {35, 20, "4e2d6587"}, {35, 20, "00e920ac003f"}},
		json:      "007b002200610022003a0020002200e920ac003f0022007d",
		results:   "0075006300730032",
	}, {
		// MySQL sends each column in its own character set, with the
		// binary collation and one byte for each character of its length.
		name:      "SET character_set_results = binary",
		handshake: 255,
		set:       "set character_set_results = binary",
		text:      [3]column{{63, 10, "4dfc6c6c6572"}, {63, 10, "d6d0cec4"}, {63, 10, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "62696e617279",
	}, {
		// MySQL sends each column in its own character set, with its
		// own collation and length: l with latin1_swedish_ci and 10, g
		// with gb2312_chinese_ci and 20.
		name:      "SET character_set_results = NULL",
		handshake: 255,
		set:       "set character_set_results = null",
		text:      [3]column{{8, 10, "4dfc6c6c6572"}, {24, 20, "d6d0cec4"}, {255, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "NULL",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := connect(t, tc.handshake)
			if tc.set != "" {
				exec(t, conn, tc.set)
			}
			for _, workload := range []string{"oltp", "olap"} {
				exec(t, conn, "set workload = "+workload)
				qr, err := conn.ExecuteFetch(query, 10, true)
				require.NoError(t, err)
				require.Len(t, qr.Rows, 1)
				require.Len(t, qr.Fields, 7)
				row := qr.Rows[0]

				for i, want := range tc.text {
					f := qr.Fields[1+i]
					assert.Equal(t, uint32(want.collation), f.Charset, "%s: field %s", workload, f.Name)
					assert.Equal(t, want.length, f.ColumnLength, "%s: field %s", workload, f.Name)
					assert.Equal(t, want.value, hex.EncodeToString(row[1+i].Raw()), "%s: value of %s", workload, f.Name)
				}
				for _, i := range []int{0, 4, 5, 6} {
					assert.Equal(t, uint32(collations.CollationBinaryID), qr.Fields[i].Charset, "%s: field %d", workload, i)
				}
				assert.Equal(t, uint32(4294967295), qr.Fields[4].ColumnLength, workload)
				assert.Equal(t, "31", hex.EncodeToString(row[0].Raw()), workload)
				assert.Equal(t, tc.json, hex.EncodeToString(row[4].Raw()), workload)
				assert.Equal(t, "00ff", hex.EncodeToString(row[5].Raw()), workload)
				assert.Equal(t, "3432", hex.EncodeToString(row[6].Raw()), workload)

				qr, err = conn.ExecuteFetch("select @@character_set_results", 10, true)
				require.NoError(t, err)
				got := "NULL"
				if !qr.Rows[0][0].IsNull() {
					got = hex.EncodeToString(qr.Rows[0][0].Raw())
				}
				assert.Equal(t, tc.results, got, workload)
			}
		})
	}
}

// TestHandshakeRefused covers the clients that vtgate refuses with
// --convert-result-charset, which reads every query as utf8mb4: a client
// whose handshake collation belongs to another character set than utf8mb4
// and utf8mb3. MySQL accepts latin1, gb2312 and binary, and refuses ucs2 with
// the same error.
func TestHandshakeRefused(t *testing.T) {
	tests := []struct {
		handshake collations.ID
		charset   string
	}{
		{8, "latin1"},
		{24, "gb2312"},
		{63, "binary"},
		{35, "ucs2"},
	}
	for _, tc := range tests {
		t.Run(tc.charset, func(t *testing.T) {
			params := vtParams
			params.Charset = tc.handshake
			_, err := mysql.Connect(t.Context(), &params)
			require.Error(t, err)
			var sqlErr *sqlerror.SQLError
			require.ErrorAs(t, err, &sqlErr)
			assert.Equal(t, sqlerror.ERWrongValueForVar, sqlErr.Num)
			assert.Equal(t, sqlerror.SSClientError, sqlErr.State)
			assert.Equal(t, "Variable 'character_set_client' can't be set to the value of '"+tc.charset+"': vtgate with --convert-result-charset accepts utf8mb4 and utf8mb3 clients, and SET character_set_results gives results in another character set", sqlErr.Message)
		})
	}
}

// TestResultCharsetNames covers the names in the fields, which MySQL sends in
// character_set_results too: with gb2312, the alias ü is A8 B9 and the column
// u4 is 75 34, and with ucs2 the column u4 is 00 75 00 34.
func TestResultCharsetNames(t *testing.T) {
	insertRow(t)
	conn := connect(t, collations.CollationUtf8mb4ID)

	exec(t, conn, "set character_set_results = gb2312")
	qr, err := conn.ExecuteFetch("select u4 as `ü`, u4 from rc where id = 1", 10, true)
	require.NoError(t, err)
	assert.Equal(t, "a8b9", hex.EncodeToString([]byte(qr.Fields[0].Name)))
	assert.Equal(t, "7534", hex.EncodeToString([]byte(qr.Fields[1].Name)))

	exec(t, conn, "set character_set_results = ucs2")
	qr, err = conn.ExecuteFetch("select u4 from rc where id = 1", 10, true)
	require.NoError(t, err)
	assert.Equal(t, "00750034", hex.EncodeToString([]byte(qr.Fields[0].Name)))
}

// TestResultCharsetErrors covers what MySQL refuses, what vtgate refuses
// where MySQL accepts it, and the error messages, which MySQL sends in
// character_set_results: with gb2312, the duplicate entry 'Müller' is
// 4D A8 B9 6C 6C 65 72, and with NULL it is in utf8.
func TestResultCharsetErrors(t *testing.T) {
	insertRow(t)
	conn := connect(t, collations.CollationUtf8mb4ID)

	_, err := conn.ExecuteFetch("set character_set_results = nosuch", 10, false)
	require.Error(t, err)
	assert.Equal(t, sqlerror.ERUnknownCharacterSet, err.(*sqlerror.SQLError).Num)
	assert.Contains(t, err.Error(), "Unknown character set: 'nosuch'")

	_, err = conn.ExecuteFetch("set names utf8 collate utf8mb4_bin", 10, false)
	require.Error(t, err)
	assert.Equal(t, sqlerror.ERCollationCharsetMismatch, err.(*sqlerror.SQLError).Num)
	assert.Contains(t, err.Error(), "COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'utf8mb3'")

	// vtgate reads every query as utf8mb4, and refuses SET NAMES and SET
	// CHARACTER SET with another character set.
	for _, set := range []string{"set names latin1", "set names binary", "set character set gb2312"} {
		_, err = conn.ExecuteFetch(set, 10, false)
		require.Error(t, err, set)
		assert.Equal(t, sqlerror.ERNotSupportedYet, err.(*sqlerror.SQLError).Num, set)
		assert.Contains(t, err.Error(), "accepts SET NAMES and SET CHARACTER SET with utf8mb4 and utf8mb3", set)
	}
	qr, err := conn.ExecuteFetch("select @@character_set_results", 10, false)
	require.NoError(t, err)
	assert.Equal(t, "utf8mb4", qr.Rows[0][0].ToString())

	const duplicate = "insert into rc (id, l) values (2, 'Müller')"
	exec(t, conn, "set character_set_results = gb2312")
	_, err = conn.ExecuteFetch(duplicate, 10, false)
	require.Error(t, err)
	message := err.(*sqlerror.SQLError).Message
	assert.Contains(t, message, "Duplicate entry 'M\xA8\xB9ller'")
	assert.NotContains(t, message, "Müller")

	exec(t, conn, "set character_set_results = null")
	_, err = conn.ExecuteFetch(duplicate, 10, false)
	require.Error(t, err)
	assert.Contains(t, err.(*sqlerror.SQLError).Message, "Duplicate entry 'Müller'")
}

// TestPreparedStatement covers prepared statements over the binary protocol
// (COM_STMT_PREPARE and COM_STMT_EXECUTE): with character_set_results = gb2312,
// they return the gb2312 bytes that the text protocol returns. MySQL 8.0.46
// and 8.4.13 send 4D A8 B9 6C 6C 65 72, D6 D0 CE C4 and A8 A6 3F 3F for l, g
// and u4 with both. The driver prepares on the server: Prepare always sends
// COM_STMT_PREPARE, and interpolateParams is off.
func TestPreparedStatement(t *testing.T) {
	insertRow(t)

	cfg := gomysql.NewConfig()
	cfg.User = vtParams.Uname
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", vtParams.Host, vtParams.Port)
	cfg.DBName = vtParams.DbName
	cfg.InterpolateParams = false
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ctx := t.Context()
	// SET character_set_results and the statements must share a session.
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	_, err = conn.ExecContext(ctx, "set character_set_results = gb2312")
	require.NoError(t, err)

	want := []string{"4da8b96c6c6572", "d6d0cec4", "a8a63f3f"}
	scan := func(t *testing.T, rows *sql.Rows) {
		t.Helper()
		defer rows.Close()
		require.True(t, rows.Next(), "no row")
		var l, g, u4 []byte
		require.NoError(t, rows.Scan(&l, &g, &u4))
		assert.Equal(t, want, []string{hex.EncodeToString(l), hex.EncodeToString(g), hex.EncodeToString(u4)})
		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())
	}

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "set workload = "+workload)
			require.NoError(t, err)

			// A query without arguments goes over the text protocol.
			rows, err := conn.QueryContext(ctx, "select l, g, u4 from rc where id = 1")
			require.NoError(t, err)
			scan(t, rows)

			stmt, err := conn.PrepareContext(ctx, "select l, g, u4 from rc where id = ?")
			require.NoError(t, err)
			defer stmt.Close()
			rows, err = stmt.QueryContext(ctx, 1)
			require.NoError(t, err)
			scan(t, rows)
		})
	}
}
