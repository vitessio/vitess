/*
Copyright 2018 The Vitess Authors.

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
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestExplainExecute(t *testing.T) {
	leftPrim := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	rightPrim := NewRoute(
		SelectScatter,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"some other query",
		"dummy_select_field",
	)

	jn := &Join{
		Opcode: NormalJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		Vars: map[string]int{
			"bv": 1,
		},
	}

	explain := &Explain{
		Input: jn,
	}
	r, err := explain.Execute(noopVCursor{}, make(map[string]*querypb.BindVariable), true)
	assert.NoError(t, err)
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|4|d",
		"3|c|5|e",
		"3|c|6|f",
		"3|c|7|g",
	))
}
