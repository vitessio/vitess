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
		output   string
	}{
		{
			"no subs",
			"select * from a where id = 2",
			map[string]interface{}{
				"id": 1,
			},
			"select * from a where id = 2",
		}, {
			"simple bindvar sub",
			"select * from a where id1 = :id1 and id2 = :id2",
			map[string]interface{}{
				"id1": 1,
				"id2": nil,
			},
			"select * from a where id1 = 1 and id2 = null",
		}, {
			"missing bind var",
			"select * from a where id1 = :id1 and id2 = :id2",
			map[string]interface{}{
				"id1": 1,
			},
			"missing bind var id2",
		}, {
			"unencodable bind var",
			"select * from a where id1 = :id",
			map[string]interface{}{
				"id": make([]int, 1),
			},
			"unsupported bind variable type []int: [0]",
		}, {
			"list inside bind vars",
			"select * from a where id in (:vals)",
			map[string]interface{}{
				"vals": []sqltypes.Value{
					sqltypes.MakeNumeric([]byte("1")),
					sqltypes.MakeString([]byte("aa")),
				},
			},
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
			"select * from a where id in ((1, 'aa'), (null, 'bb'))",
		}, {
			"list bind vars",
			"select * from a where id in ::vals",
			map[string]interface{}{
				"vals": []interface{}{
					1,
					"aa",
				},
			},
			"select * from a where id in (1, 'aa')",
		}, {
			"list bind vars single argument",
			"select * from a where id in ::vals",
			map[string]interface{}{
				"vals": []interface{}{
					1,
				},
			},
			"select * from a where id in (1)",
		}, {
			"list bind vars 0 arguments",
			"select * from a where id in ::vals",
			map[string]interface{}{
				"vals": []interface{}{},
			},
			"empty list supplied for vals",
		}, {
			"non-list bind var supplied",
			"select * from a where id in ::vals",
			map[string]interface{}{
				"vals": 1,
			},
			"unexpected list arg type int for key vals",
		}, {
			"list bind var for non-list",
			"select * from a where id = :vals",
			map[string]interface{}{
				"vals": []interface{}{1},
			},
			"unexpected arg type []interface {} for key vals",
		}, {
			"single column tuple equality",
			// We have to use an incorrect construct to get around the parser.
			"select * from a where b = :equality",
			map[string]interface{}{
				"equality": TupleEqualityList{
					Columns: []string{"pk"},
					Rows: [][]sqltypes.Value{
						[]sqltypes.Value{sqltypes.MakeNumeric([]byte("1"))},
						[]sqltypes.Value{sqltypes.MakeString([]byte("aa"))},
					},
				},
			},
			"select * from a where b = pk in (1, 'aa')",
		}, {
			"multi column tuple equality",
			"select * from a where b = :equality",
			map[string]interface{}{
				"equality": TupleEqualityList{
					Columns: []string{"pk1", "pk2"},
					Rows: [][]sqltypes.Value{
						[]sqltypes.Value{
							sqltypes.MakeNumeric([]byte("1")),
							sqltypes.MakeString([]byte("aa")),
						},
						[]sqltypes.Value{
							sqltypes.MakeNumeric([]byte("2")),
							sqltypes.MakeString([]byte("bb")),
						},
					},
				},
			},
			"select * from a where b = (pk1, pk2) = (1, 'aa') or (pk1, pk2) = (2, 'bb')",
		}, {
			"0 rows",
			"select * from a where b = :equality",
			map[string]interface{}{
				"equality": TupleEqualityList{
					Columns: []string{"pk"},
					Rows:    [][]sqltypes.Value{},
				},
			},
			"cannot encode with 0 rows",
		}, {
			"values don't match column count",
			"select * from a where b = :equality",
			map[string]interface{}{
				"equality": TupleEqualityList{
					Columns: []string{"pk"},
					Rows: [][]sqltypes.Value{
						[]sqltypes.Value{
							sqltypes.MakeNumeric([]byte("1")),
							sqltypes.MakeString([]byte("aa")),
						},
					},
				},
			},
			"values don't match column count",
		},
	}

	for _, tcase := range tcases {
		tree, err := Parse(tcase.query)
		if err != nil {
			t.Errorf("parse failed for %s: %v", tcase.desc, err)
			continue
		}
		buf := NewTrackedBuffer(nil)
		buf.Myprintf("%v", tree)
		pq := buf.ParsedQuery()
		bytes, err := pq.GenerateQuery(tcase.bindVars)
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
