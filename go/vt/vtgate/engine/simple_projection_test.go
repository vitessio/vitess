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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestSubqueryExecute(t *testing.T) {
	prim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
			),
		},
	}

	sq := &SimpleProjection{
		Cols:     []int{0, 2},
		ColNames: []string{"", ""},
		Input:    prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := sq.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.NoError(t, err)
	prim.ExpectLog(t, []string{
		fmt.Sprintf(`Execute a: %v true`, sqltypes.Int64BindVariable(1)),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col3",
			"int64|varchar",
		),
		"1|aa",
		"2|bb",
		"3|cc",
	))

	// Error case.
	sq.Input = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = sq.TryExecute(t.Context(), &noopVCursor{}, bv, true)
	require.EqualError(t, err, `err`)
}

func TestSubqueryStreamExecute(t *testing.T) {
	prim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
			),
		},
	}

	sq := &SimpleProjection{
		Cols:     []int{0, 2},
		ColNames: []string{"", ""},
		Input:    prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := wrapStreamExecute(sq, &noopVCursor{}, bv, true)
	require.NoError(t, err)
	prim.ExpectLog(t, []string{
		fmt.Sprintf(`StreamExecute a: %v true`, sqltypes.Int64BindVariable(1)),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col3",
			"int64|varchar",
		),
		"1|aa",
		"2|bb",
		"3|cc",
	))

	// Error case.
	sq.Input = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = wrapStreamExecute(sq, &noopVCursor{}, bv, true)
	require.EqualError(t, err, `err`)
}

func TestSubqueryGetFields(t *testing.T) {
	prim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
			),
		},
	}

	sq := &SimpleProjection{
		Cols:     []int{0, 2},
		ColNames: []string{"", ""},
		Input:    prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := sq.GetFields(t.Context(), nil, bv)
	require.NoError(t, err)
	prim.ExpectLog(t, []string{
		fmt.Sprintf(`GetFields a: %v`, sqltypes.Int64BindVariable(1)),
		fmt.Sprintf(`Execute a: %v true`, sqltypes.Int64BindVariable(1)),
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col3",
			"int64|varchar",
		),
	))

	// Error case.
	sq.Input = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = sq.GetFields(t.Context(), nil, bv)
	require.EqualError(t, err, `err`)
}

func TestSubqueryGetFieldsNamesOnly(t *testing.T) {
	prim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
				"3|c|cc",
			),
		},
	}

	sq := &SimpleProjection{
		ColNames: []string{"col1alias", "", "col3alias"},
		Input:    prim,
	}

	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(1),
	}

	r, err := sq.GetFields(t.Context(), nil, bv)
	require.NoError(t, err)
	prim.ExpectLog(t, []string{
		`GetFields a: type:INT64 value:"1"`,
		`Execute a: type:INT64 value:"1" true`,
	})
	expectResult(t, r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1alias|col2|col3alias",
			"int64|varchar|varchar",
		),
	))

	// Error case.
	sq.Input = &fakePrimitive{
		sendErr: errors.New("err"),
	}
	_, err = sq.GetFields(t.Context(), nil, bv)
	require.EqualError(t, err, `err`)
}

// concatStreamSource mimics how Concatenate.parallelStreamExec feeds this
// primitive in OLAP mode: it sends the fields only on the first chunk and
// clears them on every later chunk, and it keeps delivering chunks even after
// the callback has returned an error, because a scatter route only stops
// streaming once all of its shard streams have finished - so other arms keep
// streaming from inside the errgroup goroutines (which have no recover) while
// the error is being propagated.
type concatStreamSource struct {
	fakePrimitive
	chunks []*sqltypes.Result
	// errs records the error returned by the callback for each delivered chunk.
	errs []error
}

func (c *concatStreamSource) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var retErr error
	for _, chunk := range c.chunks {
		err := callback(chunk)
		if err != nil && retErr == nil {
			retErr = err
		}
		c.errs = append(c.errs, err)
	}
	return retErr
}

// assertColsPastInputError asserts that err is the MySQL errno 1222 error
// (ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, SQL state 21000) that main returns
// for these queries at execution time. The guard in SimpleProjection.checkColumns
// must return it instead of panicking with an index out of range when a
// projection above a union whose wider inputs were trimmed reads at or past
// the end of the input (vitessio/vitess#22059).
func assertColsPastInputError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.Equal(t, vterrors.WrongNumberOfColumnsInSelect, vterrors.ErrState(err))
	require.EqualError(t, err, "The used SELECT statements have a different number of columns")
}

// TestSimpleProjectionColsPastInputWidth covers the checkColumns guard: a
// SimpleProjection whose Cols read at or past the end of the input must fail
// with the 1222 error on every path instead of panicking in buildResult.
func TestSimpleProjectionColsPastInputWidth(t *testing.T) {
	t.Run("TryExecute", func(t *testing.T) {
		sq := &SimpleProjection{
			Cols:     []int{0, 2},
			ColNames: []string{"", ""},
			Input: &fakePrimitive{
				results: []*sqltypes.Result{
					sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"col1|col2",
							"int64|varchar",
						),
						"1|a",
					),
				},
			},
		}
		_, err := sq.TryExecute(t.Context(), &noopVCursor{}, nil, true)
		assertColsPastInputError(t, err)
	})

	t.Run("GetFields", func(t *testing.T) {
		sq := &SimpleProjection{
			Cols:     []int{0, 2},
			ColNames: []string{"", ""},
			Input: &fakePrimitive{
				results: []*sqltypes.Result{
					sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"col1|col2",
							"int64|varchar",
						),
					),
				},
			},
		}
		_, err := sq.GetFields(t.Context(), nil, nil)
		assertColsPastInputError(t, err)
	})

	t.Run("TryStreamExecute fields chunk", func(t *testing.T) {
		// The first streamed chunk carries the field info, so the fields are
		// too narrow for the projection.
		sq := &SimpleProjection{
			Cols:     []int{0, 2},
			ColNames: []string{"", ""},
			Input: &fakePrimitive{
				results: []*sqltypes.Result{
					sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"col1|col2",
							"int64|varchar",
						),
						"1|a",
					),
				},
			},
		}
		_, err := wrapStreamExecute(sq, &noopVCursor{}, nil, true)
		assertColsPastInputError(t, err)
	})

	t.Run("TryStreamExecute rows-only chunk", func(t *testing.T) {
		// This reproduces the streaming shape from the #22059 review: the
		// fields chunk fails with 1222, but a scatter route keeps its shard
		// streams running until they finish, so Concatenate's errgroup
		// goroutines keep delivering row chunks that carry no field info.
		// Those chunks are only caught by the row-width check; without it,
		// this test panics with "index out of range [2] with length 2" in
		// buildResult.
		fields := sqltypes.MakeTestFields("col1|col2", "int64|varchar")
		src := &concatStreamSource{
			chunks: []*sqltypes.Result{
				// The first chunk carries the fields, as Concatenate sends it.
				{Fields: fields},
				// Later chunks have the fields cleared and carry rows only.
				{Rows: sqltypes.MakeTestResult(fields, "1|a", "2|b").Rows},
			},
		}
		sq := &SimpleProjection{
			Cols:     []int{0, 2},
			ColNames: []string{"", ""},
			Input:    src,
		}
		_, err := wrapStreamExecute(sq, &noopVCursor{}, nil, true)
		// The overall error is the one from the fields chunk.
		assertColsPastInputError(t, err)
		// The rows-only chunk must also fail with 1222 instead of panicking.
		require.Len(t, src.errs, 2)
		assertColsPastInputError(t, src.errs[1])
	})
}
