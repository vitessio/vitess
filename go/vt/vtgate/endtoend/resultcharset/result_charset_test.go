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
// protocol clients with different handshake collations and SET statements get
// the bytes, field collations and lengths that MySQL sends. The expected
// values were measured on MySQL 8.0.46 and 8.4.13 with the same table, and the
// two agree on all of them.
//
// The tablets read results with character_set_results = NULL, so vtgate knows
// the collation of each column, which character_set_results = NULL and binary
// need to send a latin1 or gbk column in its own character set.
package resultcharset

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

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
	l varchar(10) character set latin1 collate latin1_german2_ci,
	g varchar(10) character set gbk,
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
		name:      "latin1 handshake",
		handshake: 8,
		text:      [3]column{{8, 10, "4dfc6c6c6572"}, {8, 10, "3f3f"}, {8, 10, "e9803f"}},
		json:      "7b2261223a2022e9803f227d",
		results:   "6c6174696e31",
	}, {
		name:      "gbk handshake",
		handshake: 28,
		text:      [3]column{{28, 20, "4da8b96c6c6572"}, {28, 20, "d6d0cec4"}, {28, 20, "a8a63f3f"}},
		json:      "7b2261223a2022a8a63f3f227d",
		results:   "67626b",
	}, {
		name:      "utf8mb4_bin handshake",
		handshake: 46,
		text:      [3]column{{46, 40, "4dc3bc6c6c6572"}, {46, 40, "e4b8ade69687"}, {46, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		// MySQL sends each column in its own character set, with the
		// binary collation and one byte for each character of its length.
		name:      "binary handshake",
		handshake: 63,
		text:      [3]column{{63, 10, "4dfc6c6c6572"}, {63, 10, "d6d0cec4"}, {63, 10, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "62696e617279",
	}, {
		name:      "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
		handshake: 255,
		set:       "set names utf8mb4 collate utf8mb4_bin",
		text:      [3]column{{46, 40, "4dc3bc6c6c6572"}, {46, 40, "e4b8ade69687"}, {46, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		name:      "SET NAMES DEFAULT after a latin1 handshake",
		handshake: 8,
		set:       "set names default",
		text:      [3]column{{255, 40, "4dc3bc6c6c6572"}, {255, 40, "e4b8ade69687"}, {255, 40, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "757466386d6234",
	}, {
		name:      "SET character_set_results = gbk",
		handshake: 255,
		set:       "set character_set_results = gbk",
		text:      [3]column{{28, 20, "4da8b96c6c6572"}, {28, 20, "d6d0cec4"}, {28, 20, "a8a63f3f"}},
		json:      "7b2261223a2022a8a63f3f227d",
		results:   "67626b",
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
		name:      "SET character_set_results = binary",
		handshake: 255,
		set:       "set character_set_results = binary",
		text:      [3]column{{63, 10, "4dfc6c6c6572"}, {63, 10, "d6d0cec4"}, {63, 10, "c3a9e282acf09f9880"}},
		json:      "7b2261223a2022c3a9e282acf09f9880227d",
		results:   "62696e617279",
	}, {
		// MySQL sends each column in its own character set, with its
		// own collation and length: l with latin1_german2_ci and 10, g
		// with gbk_chinese_ci and 20.
		name:      "SET character_set_results = NULL",
		handshake: 255,
		set:       "set character_set_results = null",
		text:      [3]column{{31, 10, "4dfc6c6c6572"}, {28, 20, "d6d0cec4"}, {255, 40, "c3a9e282acf09f9880"}},
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

// TestResultCharsetNames covers the names in the fields, which MySQL sends in
// character_set_results too: with gbk, the alias ü is A8 B9 and the column
// u4 is 75 34, and with ucs2 the column u4 is 00 75 00 34.
func TestResultCharsetNames(t *testing.T) {
	insertRow(t)
	conn := connect(t, collations.CollationUtf8mb4ID)

	exec(t, conn, "set character_set_results = gbk")
	qr, err := conn.ExecuteFetch("select u4 as `ü`, u4 from rc where id = 1", 10, true)
	require.NoError(t, err)
	assert.Equal(t, "a8b9", hex.EncodeToString([]byte(qr.Fields[0].Name)))
	assert.Equal(t, "7534", hex.EncodeToString([]byte(qr.Fields[1].Name)))

	exec(t, conn, "set character_set_results = ucs2")
	qr, err = conn.ExecuteFetch("select u4 from rc where id = 1", 10, true)
	require.NoError(t, err)
	assert.Equal(t, "00750034", hex.EncodeToString([]byte(qr.Fields[0].Name)))
}

// TestResultCharsetErrors covers what MySQL refuses, and the error messages,
// which MySQL sends in character_set_results: with gbk, the duplicate entry
// 'Müller' is 4D A8 B9 6C 6C 65 72, and with NULL it is in utf8.
func TestResultCharsetErrors(t *testing.T) {
	insertRow(t)
	conn := connect(t, collations.CollationUtf8mb4ID)

	_, err := conn.ExecuteFetch("set character_set_results = nosuch", 10, false)
	require.Error(t, err)
	assert.Equal(t, sqlerror.ERUnknownCharacterSet, err.(*sqlerror.SQLError).Num)
	assert.Contains(t, err.Error(), "Unknown character set: 'nosuch'")

	_, err = conn.ExecuteFetch("set names latin1 collate utf8mb4_bin", 10, false)
	require.Error(t, err)
	assert.Equal(t, sqlerror.ERCollationCharsetMismatch, err.(*sqlerror.SQLError).Num)

	const duplicate = "insert into rc (id, l) values (2, 'Müller')"
	exec(t, conn, "set character_set_results = gbk")
	_, err = conn.ExecuteFetch(duplicate, 10, false)
	require.Error(t, err)
	message := err.(*sqlerror.SQLError).Message
	assert.Contains(t, message, "Duplicate entry 'M\xA8\xB9ller'")
	assert.NotContains(t, message, "Müller")

	exec(t, conn, "set character_set_results = null")
	_, err = conn.ExecuteFetch(duplicate, 10, false)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.(*sqlerror.SQLError).Message, "Duplicate entry 'Müller'"), err.Error())
}
