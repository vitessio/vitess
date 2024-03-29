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
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
)

func FuzzIsDML(data []byte) int {
	_ = sqlparser.IsDML(string(data))
	return 1
}

func FuzzNormalizer(data []byte) int {
	stmt, reservedVars, err := sqlparser.Parse2(string(data))
	if err != nil {
		return -1
	}
	bv := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(stmt, sqlparser.NewReservedVars("bv", reservedVars), bv)
	return 1
}

func FuzzParser(data []byte) int {
	_, err := sqlparser.NewTestParser().Parse(string(data))
	if err != nil {
		return 0
	}
	return 1
}

func FuzzNodeFormat(data []byte) int {
	f := fuzz.NewConsumer(data)
	query, err := f.GetSQLString()
	if err != nil {
		return 0
	}
	node, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		return 0
	}
	buf := &sqlparser.TrackedBuffer{}
	err = f.GenerateStruct(buf)
	if err != nil {
		return 0
	}
	node.Format(buf)
	return 1
}

func FuzzSplitStatementToPieces(data []byte) int {
	_, _ = sqlparser.NewTestParser().SplitStatementToPieces(string(data))
	return 1
}
