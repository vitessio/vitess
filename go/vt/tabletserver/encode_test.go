/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
