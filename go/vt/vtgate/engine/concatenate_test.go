/*
Copyright 2020 The Vitess Authors.

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
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func r(names, types string, rows ...string) *sqltypes.Result {
	fields := sqltypes.MakeTestFields(names, types)
	for _, f := range fields {
		if sqltypes.IsText(f.Type) {
			f.Charset = collations.CollationUtf8mb4ID
		} else {
			f.Charset = collations.CollationBinaryID
		}
		_, flags := sqltypes.TypeToMySQL(f.Type)
		f.Flags = uint32(flags)
	}
	return sqltypes.MakeTestResult(fields, rows...)
}

func TestConcatenate_NoErrors(t *testing.T) {
	type testCase struct {
		testName       string
		inputs         []*sqltypes.Result
		expectedResult *sqltypes.Result
		expectedError  string
		ignoreTypes    []int
	}

	r1 := r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2")
	r2 := r("id|col1|col2", "int32|varbinary|varbinary", "1|a1|b1", "2|a2|b2")
	combinedResult := r1
	combinedResult.Rows = append(combinedResult.Rows, r2.Rows...)

	testCases := []*testCase{{
		testName: "empty results",
		inputs: []*sqltypes.Result{
			r("id1|col11|col12", "int64|varbinary|varbinary"),
			r("id2|col21|col22", "int64|varbinary|varbinary"),
			r("id3|col31|col32", "int64|varbinary|varbinary"),
		},
		expectedResult: r("id1|col11|col12", "int64|varbinary|varbinary"),
	}, {
		testName: "2 non empty result",
		inputs: []*sqltypes.Result{
			r("myid|mycol1|mycol2", "int64|varchar|varbinary", "11|m1|n1", "22|m2|n2"),
			r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id2|col2|col3", "int64|varchar|varbinary", "3|a3|b3"),
			r("id2|col2|col3", "int64|varchar|varbinary", "4|a4|b4"),
		},
		expectedResult: r("myid|mycol1|mycol2", "int64|varchar|varbinary", "11|m1|n1", "22|m2|n2", "1|a1|b1", "2|a2|b2", "3|a3|b3", "4|a4|b4"),
	}, {
		testName: "mismatch field type",
		inputs: []*sqltypes.Result{
			r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id|col3|col4", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2", "1|a1|b1", "2|a2|b2", "1|a1|b1", "2|a2|b2"),
	}, {
		testName: "ignored field types - ignored",
		inputs: []*sqltypes.Result{
			r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
			r("id|col1|col2", "int32|varbinary|varbinary", "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: combinedResult,
		ignoreTypes:    []int{0},
	}, {
		testName: "input source has different column count",
		inputs: []*sqltypes.Result{
			r("id|col1|col2", "int64|varchar|varchar", "1|a1|b1", "2|a2|b2"),
			r("id|col1|col2", "int64|varchar|varchar", "1|a1|b1", "2|a2|b2"),
			r("id|col3|col4|col5", "int64|varchar|varchar|int32", "1|a1|b1|5", "2|a2|b2|6"),
		},
		expectedError: "The used SELECT statements have a different number of columns",
	}, {
		testName: "1 empty result and 1 non empty result",
		inputs: []*sqltypes.Result{
			r("myid|mycol1|mycol2", "int64|varchar|varbinary"),
			r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
		},
		expectedResult: r("myid|mycol1|mycol2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"),
	}}

	for _, tc := range testCases {
		for _, tx := range []bool{false, true} {
			var sources []Primitive
			for _, input := range tc.inputs {
				// input is added twice, since the first one is used by execute and the next by stream execute
				sources = append(sources, &fakePrimitive{results: []*sqltypes.Result{input, input}})
			}

			concatenate := NewConcatenate(sources, tc.ignoreTypes)
			vcursor := &noopVCursor{inTx: tx}
			txStr := "InTx"
			if !tx {
				txStr = "NotInTx"
			}
			checkResult := func(t *testing.T, qr *sqltypes.Result, err error) {
				if tc.expectedError == "" {
					require.NoError(t, err)
					utils.MustMatch(t, tc.expectedResult.Fields, qr.Fields, "fields")
					require.NoError(t, sqltypes.RowsEquals(tc.expectedResult.Rows, qr.Rows))
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.expectedError)
				}
			}
			t.Run(fmt.Sprintf("%s-%s-Exec", txStr, tc.testName), func(t *testing.T) {
				qr, err := concatenate.TryExecute(t.Context(), vcursor, nil, true)
				checkResult(t, qr, err)
			})

			t.Run(fmt.Sprintf("%s-%s-StreamExec", txStr, tc.testName), func(t *testing.T) {
				qr, err := wrapStreamExecute(concatenate, vcursor, nil, true)
				checkResult(t, qr, err)
			})
		}
	}
}

func TestConcatenate_WithErrors(t *testing.T) {
	strFailed := "failed"

	fake := r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2")
	concatenate := NewConcatenate(
		[]Primitive{
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
			&fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)},
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
		}, nil,
	)
	_, err := concatenate.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)

	_, err = wrapStreamExecute(concatenate, &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)

	concatenate = NewConcatenate(
		[]Primitive{
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
			&fakePrimitive{results: []*sqltypes.Result{nil, nil}, sendErr: errors.New(strFailed)},
			&fakePrimitive{results: []*sqltypes.Result{fake, fake}},
		}, nil)

	_, err = concatenate.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)
	_, err = wrapStreamExecute(concatenate, &noopVCursor{}, nil, true)
	require.EqualError(t, err, strFailed)
}

func TestConcatenateStreamExecuteCopiesBindVars(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"input": sqltypes.StringBindVariable("value"),
	}
	concatenate := NewConcatenate([]Primitive{
		&bindVarMutatingPrimitive{},
	}, nil)

	_, err := wrapStreamExecute(concatenate, &noopVCursor{}, bindVars, true)

	require.NoError(t, err)
	require.NotContains(t, bindVars, "mutated")
}

func TestConcatenateStreamExecutePreservesSourceOrder(t *testing.T) {
	leftStarted := make(chan struct{})
	rightRowsSent := make(chan struct{})

	left := &orderedStreamPrimitive{
		result:             r("id|name", "int64|varchar", "1|left"),
		signalBeforeFields: leftStarted,
		waitBeforeRows:     rightRowsSent,
	}
	right := &orderedStreamPrimitive{
		result:          r("id|name", "int64|varchar", "2|right"),
		waitBeforeStart: leftStarted,
		signalAfterRows: rightRowsSent,
	}
	concatenate := NewConcatenate([]Primitive{left, right}, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	var result sqltypes.Result
	err := concatenate.TryStreamExecute(ctx, &noopVCursor{}, nil, true, func(r *sqltypes.Result) error {
		if result.Fields == nil {
			result.Fields = r.Fields
		}
		result.Rows = append(result.Rows, r.Rows...)
		return nil
	})

	require.NoError(t, err)
	require.NoError(t, sqltypes.RowsEquals(r("id|name", "int64|varchar", "1|left", "2|right").Rows, result.Rows))
}

func TestConcatenateStreamExecuteErrorsIfSourceFinishesWithoutFields(t *testing.T) {
	concatenate := NewConcatenate([]Primitive{
		&streamRowsWithoutFieldsPrimitive{},
		&fakePrimitive{results: []*sqltypes.Result{r("id", "int64", "2")}},
	}, nil)

	err := concatenate.TryStreamExecute(t.Context(), &noopVCursor{}, nil, true, func(*sqltypes.Result) error {
		return nil
	})

	require.ErrorContains(t, err, "finished without sending fields")
}

type bindVarMutatingPrimitive struct {
	fakePrimitive
}

func (p *bindVarMutatingPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	bindVars["mutated"] = sqltypes.StringBindVariable("value")
	return callback(r("id", "int64"))
}

type orderedStreamPrimitive struct {
	fakePrimitive
	result             *sqltypes.Result
	waitBeforeStart    <-chan struct{}
	waitBeforeRows     <-chan struct{}
	signalBeforeFields chan<- struct{}
	signalAfterRows    chan<- struct{}
}

func (p *orderedStreamPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if p.waitBeforeStart != nil {
		select {
		case <-p.waitBeforeStart:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.signalBeforeFields != nil {
		close(p.signalBeforeFields)
	}
	if wantfields {
		if err := callback(&sqltypes.Result{Fields: p.result.Fields}); err != nil {
			return err
		}
	}
	if p.waitBeforeRows != nil {
		select {
		case <-p.waitBeforeRows:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	rows := make([][]sqltypes.Value, 0, len(p.result.Rows))
	for _, row := range p.result.Rows {
		rows = append(rows, sqltypes.CopyRow(row))
	}
	if err := callback(&sqltypes.Result{Rows: rows}); err != nil {
		return err
	}
	if p.signalAfterRows != nil {
		close(p.signalAfterRows)
	}
	return nil
}

type streamRowsWithoutFieldsPrimitive struct {
	fakePrimitive
}

func (p *streamRowsWithoutFieldsPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return callback(&sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}}})
}

func TestConcatenateTypes(t *testing.T) {
	tests := []struct {
		t1, t2   string
		expected []*querypb.Field
	}{
		{t1: "int32", t2: "int64", expected: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64, Charset: collations.CollationBinaryID}}},
		{t1: "int32", t2: "int32", expected: []*querypb.Field{{Name: "id", Type: querypb.Type_INT32, Charset: collations.CollationBinaryID}}},
		{t1: "int32", t2: "varchar", expected: []*querypb.Field{{Name: "id", Type: querypb.Type_VARCHAR, Charset: collations.CollationUtf8mb4ID}}},
		{t1: "int32", t2: "decimal", expected: []*querypb.Field{{Name: "id", Type: querypb.Type_DECIMAL, Charset: collations.CollationBinaryID}}},
		{t1: "hexval", t2: "uint64", expected: []*querypb.Field{{Name: "id", Type: querypb.Type_VARCHAR, Charset: collations.CollationUtf8mb4ID}}},
		{t1: "varchar", t2: "varbinary", expected: []*querypb.Field{{Name: "id", Type: querypb.Type_VARBINARY, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)}}},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%s - %s", test.t1, test.t2)
		t.Run(name, func(t *testing.T) {
			in1 := r("id", test.t1, "1")
			in2 := r("id", test.t2, "1")
			concatenate := NewConcatenate(
				[]Primitive{
					&fakePrimitive{results: []*sqltypes.Result{in1}},
					&fakePrimitive{results: []*sqltypes.Result{in2}},
				}, nil,
			)

			res, err := concatenate.GetFields(t.Context(), &noopVCursor{}, nil)
			require.NoError(t, err)

			require.Equal(t, test.expected, res.Fields)
		})
	}
}
