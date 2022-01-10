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
	"testing"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
)

func FuzzIsDML(f *testing.F) {
	f.Fuzz(func(t *testing.T, data string) {
		_ = sqlparser.IsDML(data)
	})
}

func FuzzNormalizer(f *testing.F) {
	f.Fuzz(func(t *testing.T, data string) {
		stmt, reservedVars, err := sqlparser.Parse2(data)
		if err != nil {
			return
		}
		bv := make(map[string]*querypb.BindVariable)
		sqlparser.Normalize(stmt, sqlparser.NewReservedVars("bv", reservedVars), bv)
	})
}

func FuzzParser(f *testing.F) {
	f.Fuzz(func(t *testing.T, data string) {
		_, _ = sqlparser.Parse(data)
	})
}

func FuzzNodeFormat(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		f := fuzz.NewConsumer(data)
		query, err := f.GetSQLString()
		if err != nil {
			return
		}
		node, err := sqlparser.Parse(query)
		if err != nil {
			return
		}
		buf := &sqlparser.TrackedBuffer{}
		err = f.GenerateStruct(buf)
		if err != nil {
			return
		}
		node.Format(buf)
	})
}

func FuzzSplitStatementToPieces(f *testing.F) {
	f.Fuzz(func(t *testing.T, data string) {
		_, _ = sqlparser.SplitStatementToPieces(data)
	})
}
