/*
Copyright 2019 The Vitess Authors.

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

package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	context2 "golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func BenchmarkJoin_Execute(b *testing.B) {
	leftPrim := &benchmarkSource{}
	rightPrim := &benchmarkSource{}
	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(10),
	}

	// Normal join
	jn := &Join{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}
	for i := 0; i < 10000; i++ {
		_, err := jn.Execute(context.Background(), noopVCursor{}, bv, false)
		assert.NoError(b, err)
	}
}

var _ Primitive = (*benchmarkSource)(nil)

type benchmarkSource struct {
	result *sqltypes.Result
}

func (bs *benchmarkSource) RouteType() string {
	panic("implement me")
}

func (bs *benchmarkSource) GetKeyspaceName() string {
	panic("implement me")
}

func (bs *benchmarkSource) GetTableName() string {
	panic("implement me")
}

const resultSize = 10

func (bs *benchmarkSource) Execute(ctx context2.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if bs.result == nil {
		lotsOfRows := make([]string, resultSize)
		for i := 0; i < resultSize; i++ {
			lotsOfRows[i] = strconv.Itoa(i) + "|a|aa"
		}

		bs.result = sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"col1|col2|col3",
				"int64|varchar|varchar",
			),
			lotsOfRows...,
		)
	}
	return bs.result, nil
}

func (bs *benchmarkSource) StreamExecute(ctx context2.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (bs *benchmarkSource) GetFields(ctx context2.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}
