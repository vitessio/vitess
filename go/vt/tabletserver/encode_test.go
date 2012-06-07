// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/mysql"
	"testing"
)

func assertTrue(tf bool, msg string, t *testing.T) {
	if !tf {
		t.Error(msg)
	}
}

func TestQuery(t *testing.T) {
	bv := make(map[string]interface{})
	bv["foo"] = int64(20)
	in := &Query{"abcd", bv, 24, 0, 0}
	encoded := bytes.NewBuffer(make([]byte, 0, 8))
	in.MarshalBson(encoded)
	expected, _ := bson.Marshal(in)
	compare(t, encoded.Bytes(), expected)

	var ret Query
	ret.UnmarshalBson(encoded)
	assertTrue(ret.Sql == in.Sql, "Sql", t)
	assertTrue(ret.BindVariables["foo"] == in.BindVariables["foo"], "bind vars", t)
}

func TestQueryResult(t *testing.T) {
	fields := make([]mysql.Field, 2)
	fields[0] = mysql.Field{"name0", 0}
	fields[1] = mysql.Field{"name1", 1}

	rows := make([][]interface{}, 1)
	rows[0] = make([]interface{}, 2)
	rows[0][0] = "val0"
	rows[0][1] = "val1"
	in := &QueryResult{fields, 5, 0, rows}
	encoded := bytes.NewBuffer(make([]byte, 0, 8))
	in.MarshalBson(encoded)
	expected, _ := bson.Marshal(in)
	compare(t, encoded.Bytes(), expected)

	var ret QueryResult
	ret.UnmarshalBson(encoded)
	assertTrue(ret.Fields[1].Name == in.Fields[1].Name, "fields", t)
	assertTrue(ret.Rows[0][1] == in.Rows[0][1], "rows", t)
}

func BenchmarkMarshal(b *testing.B) {
	b.StopTimer()
	fields := make([]mysql.Field, 4)
	fields[0] = mysql.Field{"name0", 0}
	fields[1] = mysql.Field{"name1", 1}
	fields[2] = mysql.Field{"name1", 1}
	fields[3] = mysql.Field{"name1", 1}
	rows := make([][]interface{}, 1)
	rows[0] = make([]interface{}, 4)
	rows[0][0] = "val0"
	rows[0][1] = "val1"
	rows[0][2] = "val1"
	rows[0][3] = "val1"
	in := &QueryResult{fields, 1, 0, rows}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		encoded := bytes.NewBuffer(make([]byte, 0, 8))
		in.MarshalBson(encoded)
	}
}

func compare(t *testing.T, encoded []byte, expected []byte) {
	if len(encoded) != len(expected) {
		t.Errorf("encoding mismatch:\n%#v\n%#v\n", string(encoded), string(expected))
	} else {
		for i := range encoded {
			if encoded[i] != expected[i] {
				t.Errorf("encoding mismatch:\n%#v\n%#v\n%#v\n%#v\n", string(encoded), string(expected), string(encoded[i:]), string(expected[i:]))
				break
			}
		}
	}
}
