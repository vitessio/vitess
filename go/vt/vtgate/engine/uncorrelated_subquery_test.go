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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func TestPulloutSubqueryValueGood(t *testing.T) {
	// Test one case with actual bind vars.
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}

	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	underlyingResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col",
			"int64",
		),
		"0",
	)
	ufp := &fakePrimitive{
		results: []*sqltypes.Result{underlyingResult},
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
		Outer:          ufp,
	}

	result, err := ps.TryExecute(context.Background(), &noopVCursor{}, bindVars, false)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute aa: type:INT64 value:"1" false`})
	ufp.ExpectLog(t, []string{`Execute aa: type:INT64 value:"1" sq: type:INT64 value:"1" false`})
	expectResult(t, result, underlyingResult)
}

func TestPulloutSubqueryValueNone(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
		Outer:          ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute sq:  false`})
}

func TestPulloutSubqueryValueBadRows(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
		"2",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
	}

	_, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "subquery returned more than one row")
}

func TestPulloutSubqueryInNotinGood(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
		"2",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutIn,
		SubqueryResult: "sq",
		HasValues:      "has_values",
		Subquery:       sfp,
		Outer:          ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute has_values: type:INT64 value:"1" sq: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} false`})

	// Test the NOT IN case just once even though it's common code.
	sfp.rewind()
	ufp.rewind()
	ps.Opcode = PulloutNotIn
	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute has_values: type:INT64 value:"1" sq: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} false`})
}

func TestPulloutSubqueryInNone(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutIn,
		SubqueryResult: "sq",
		HasValues:      "has_values",
		Subquery:       sfp,
		Outer:          ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute has_values: type:INT64 value:"0" sq: type:TUPLE values:{type:INT64 value:"0"} false`})
}

func TestPulloutSubqueryExists(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:    PulloutExists,
		HasValues: "has_values",
		Subquery:  sfp,
		Outer:     ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute has_values: type:INT64 value:"1" false`})
}

func TestPulloutSubqueryExistsNone(t *testing.T) {
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:    PulloutExists,
		HasValues: "has_values",
		Subquery:  sfp,
		Outer:     ufp,
	}

	if _, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false); err != nil {
		t.Error(err)
	}
	sfp.ExpectLog(t, []string{`Execute  false`})
	ufp.ExpectLog(t, []string{`Execute has_values: type:INT64 value:"0" false`})
}

func TestPulloutSubqueryError(t *testing.T) {
	sfp := &fakePrimitive{
		sendErr: errors.New("err"),
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutExists,
		SubqueryResult: "sq",
		Subquery:       sfp,
	}

	_, err := ps.TryExecute(context.Background(), &noopVCursor{}, make(map[string]*querypb.BindVariable), false)
	require.EqualError(t, err, "err")
}

func TestPulloutSubqueryStream(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	sqResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1",
			"int64",
		),
		"1",
	)
	sfp := &fakePrimitive{
		results: []*sqltypes.Result{sqResult},
	}
	underlyingResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col",
			"int64",
		),
		"0",
	)
	ufp := &fakePrimitive{
		results: []*sqltypes.Result{underlyingResult},
	}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		Subquery:       sfp,
		Outer:          ufp,
	}

	result, err := wrapStreamExecute(ps, &noopVCursor{}, bindVars, true)
	require.NoError(t, err)
	sfp.ExpectLog(t, []string{`Execute aa: type:INT64 value:"1" false`})
	ufp.ExpectLog(t, []string{`StreamExecute aa: type:INT64 value:"1" sq: type:INT64 value:"1" true`})
	expectResult(t, result, underlyingResult)
}

func TestPulloutSubqueryGetFields(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"aa": sqltypes.Int64BindVariable(1),
	}
	ufp := &fakePrimitive{}
	ps := &UncorrelatedSubquery{
		Opcode:         PulloutValue,
		SubqueryResult: "sq",
		HasValues:      "has_values",
		Outer:          ufp,
	}

	if _, err := ps.GetFields(context.Background(), nil, bindVars); err != nil {
		t.Error(err)
	}
	ufp.ExpectLog(t, []string{
		`GetFields aa: type:INT64 value:"1" sq: `,
		`Execute aa: type:INT64 value:"1" sq:  true`,
	})

	ufp.rewind()
	ps.Opcode = PulloutIn
	if _, err := ps.GetFields(context.Background(), nil, bindVars); err != nil {
		t.Error(err)
	}
	ufp.ExpectLog(t, []string{
		`GetFields aa: type:INT64 value:"1" has_values: type:INT64 value:"0" sq: type:TUPLE values:{type:INT64 value:"0"}`,
		`Execute aa: type:INT64 value:"1" has_values: type:INT64 value:"0" sq: type:TUPLE values:{type:INT64 value:"0"} true`,
	})

	ufp.rewind()
	ps.Opcode = PulloutNotIn
	if _, err := ps.GetFields(context.Background(), nil, bindVars); err != nil {
		t.Error(err)
	}
	ufp.ExpectLog(t, []string{
		`GetFields aa: type:INT64 value:"1" has_values: type:INT64 value:"0" sq: type:TUPLE values:{type:INT64 value:"0"}`,
		`Execute aa: type:INT64 value:"1" has_values: type:INT64 value:"0" sq: type:TUPLE values:{type:INT64 value:"0"} true`,
	})

	ufp.rewind()
	ps.Opcode = PulloutExists
	if _, err := ps.GetFields(context.Background(), nil, bindVars); err != nil {
		t.Error(err)
	}
	ufp.ExpectLog(t, []string{
		`GetFields aa: type:INT64 value:"1" has_values: type:INT64 value:"0"`,
		`Execute aa: type:INT64 value:"1" has_values: type:INT64 value:"0" true`,
	})
}
