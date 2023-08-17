/*
Copyright 2023 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestDeleteCascade(t *testing.T) {
	fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"), "1|a", "2|b")

	inputP := &fakePrimitive{results: []*sqltypes.Result{fakeRes}}
	childP := &fakePrimitive{results: []*sqltypes.Result{fakeRes}}
	parentP := &fakePrimitive{results: []*sqltypes.Result{fakeRes}}
	fkc := &FK_Cascade{
		Input:  inputP,
		Child:  []Child{{BVName: "__vals", Cols: []int{0, 1}, P: childP}},
		Parent: parentP,
	}

	vc := &loggingVCursor{}
	_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{`Execute  true`}, inputP.log)
	assert.Equal(t, []string{`Execute __vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}} values:{type:TUPLE values:{type:INT64 value:"2"} values:{type:VARCHAR value:"b"}} true`}, childP.log)
	assert.Equal(t, []string{`Execute  true`}, parentP.log)
}
