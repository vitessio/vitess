/*
Copyright 2021 The Vitess Authors.
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

package nativefuzzing

import (
	fuzz "github.com/AdaLogics/go-fuzz-headers"

	"testing"
	"vitess.io/vitess/go/vt/sqlparser"
)

func FuzzEqualsSQLNode(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 10 {
			return
		}
		f := fuzz.NewConsumer(data)
		query1, err := f.GetSQLString()
		if err != nil {
			return
		}
		query2, err := f.GetSQLString()
		if err != nil {
			return
		}
		inA, err := sqlparser.Parse(query1)
		if err != nil {
			return
		}
		inB, err := sqlparser.Parse(query2)
		if err != nil {
			return
		}

		// There are 3 targets in this fuzzer:
		// 1) sqlparser.EqualsSQLNode
		// 2) sqlparser.CloneSQLNode
		// 3) sqlparser.VisitSQLNode

		// Target 1:
		identical := sqlparser.EqualsSQLNode(inA, inA)
		if !identical {
			panic("Should be identical")
		}
		identical = sqlparser.EqualsSQLNode(inB, inB)
		if !identical {
			panic("Should be identical")
		}

		// Target 2:
		newSQLNode := sqlparser.CloneSQLNode(inA)
		if !sqlparser.EqualsSQLNode(inA, newSQLNode) {
			panic("These two nodes should be identical")
		}

		// Target 3:
		_ = sqlparser.VisitSQLNode(inA, func(node sqlparser.SQLNode) (bool, error) { return false, nil })
	})
}
