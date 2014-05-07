// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

func TestParsedQuery(t *testing.T) {
	tcases := []struct {
		desc     string
		query    string
		bindVars map[string]interface{}
		listVars []sqltypes.Value
		output   string
	}{
		{
			"no subs",
			"select * from a where id = 2",
			map[string]interface{}{
				"id": 1,
			},
			nil,
			"select * from a where id = 2",
		}, {
			"simple bindvar sub",
			"select * from a where id1 = :id1 and id2 = :id2",
			map[string]interface{}{
				"id1": 1,
				"id2": nil,
			},
			nil,
			"select * from a where id1 = 1 and id2 = null",
		}, {
			"missing bind var",
			"select * from a where id1 = :id1 and id2 = :id2",
			map[string]interface{}{
				"id1": 1,
			},
			nil,
			"missing bind var id2",
		}, {
			"unencodable bind var",
			"select * from a where id1 = :id",
			map[string]interface{}{
				"id": make([]int, 1),
			},
			nil,
			"unsupported bind variable type []int: [0]",
		}, {
			"list var sub",
			"select * from a where id = :0 and name = :1",
			nil,
			[]sqltypes.Value{
				sqltypes.MakeNumeric([]byte("1")),
				sqltypes.MakeString([]byte("aa")),
			},
			"select * from a where id = 1 and name = 'aa'",
		}, {
			"list inside bind vars",
			"select * from a where id in (:vals)",
			map[string]interface{}{
				"vals": []sqltypes.Value{
					sqltypes.MakeNumeric([]byte("1")),
					sqltypes.MakeString([]byte("aa")),
				},
			},
			nil,
			"select * from a where id in (1, 'aa')",
		}, {
			"two lists inside bind vars",
			"select * from a where id in (:vals)",
			map[string]interface{}{
				"vals": [][]sqltypes.Value{
					[]sqltypes.Value{
						sqltypes.MakeNumeric([]byte("1")),
						sqltypes.MakeString([]byte("aa")),
					},
					[]sqltypes.Value{
						sqltypes.Value{},
						sqltypes.MakeString([]byte("bb")),
					},
				},
			},
			nil,
			"select * from a where id in ((1, 'aa'), (null, 'bb'))",
		}, {
			"illega list var name",
			"select * from a where id = :0a",
			nil,
			[]sqltypes.Value{
				sqltypes.MakeNumeric([]byte("1")),
				sqltypes.MakeString([]byte("aa")),
			},
			`unexpected: strconv.ParseInt: parsing "0a": invalid syntax for 0a`,
		}, {
			"out of range list var index",
			"select * from a where id = :10",
			nil,
			[]sqltypes.Value{
				sqltypes.MakeNumeric([]byte("1")),
				sqltypes.MakeString([]byte("aa")),
			},
			"index out of range: 10",
		},
	}

	for _, tcase := range tcases {
		tree, err := Parse(tcase.query)
		if err != nil {
			t.Errorf("parse failed for %s: %v", tcase.desc, err)
			continue
		}
		buf := NewTrackedBuffer(nil)
		buf.Fprintf("%v", tree)
		pq := buf.ParsedQuery()
		bytes, err := pq.GenerateQuery(tcase.bindVars, tcase.listVars)
		var got string
		if err != nil {
			got = err.Error()
		} else {
			got = string(bytes)
		}
		if got != tcase.output {
			t.Errorf("for test case: %s, got: '%s', want '%s'", tcase.desc, got, tcase.output)
		}
	}
}

func TestStarParam(t *testing.T) {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("select * from a where id in (%a)", "*")
	pq := buf.ParsedQuery()
	listvars := []sqltypes.Value{
		sqltypes.MakeNumeric([]byte("1")),
		sqltypes.MakeString([]byte("aa")),
	}
	bytes, err := pq.GenerateQuery(nil, listvars)
	if err != nil {
		t.Errorf("generate failed: %v", err)
		return
	}
	got := string(bytes)
	want := "select * from a where id in (1, 'aa')"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
