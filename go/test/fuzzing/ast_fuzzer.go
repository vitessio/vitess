//go:build gofuzz
// +build gofuzz

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

package fuzzing

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// FuzzEqualsSQLNode implements the fuzzer
func FuzzEqualsSQLNode(data []byte) int {
	if len(data) < 10 {
		return 0
	}
	if (len(data) % 2) != 0 {
		return 0
	}
	firstHalf := string(data[:len(data)/2])
	secondHalf := string(data[(len(data)/2)+1:])
	inA, err := sqlparser.Parse(firstHalf)
	if err != nil {
		return 0
	}
	inB, err := sqlparser.Parse(secondHalf)
	if err != nil {
		return 0
	}

	// There are 3 targets in this fuzzer:
	// 1) sqlparser.EqualsSQLNode
	// 2) sqlparser.CloneSQLNode
	// 3) sqlparser.VisitSQLNode

	// Target 1:
	_ = sqlparser.EqualsSQLNode(inA, inB)

	// Target 2:
	newSQLNode := sqlparser.CloneSQLNode(inA)
	if !sqlparser.EqualsSQLNode(inA, newSQLNode) {
		panic("These two nodes should be identical")
	}

	// Target 3:
	_ = sqlparser.VisitSQLNode(inA, func(node sqlparser.SQLNode) (bool, error) { return false, nil })
	return 1
}
