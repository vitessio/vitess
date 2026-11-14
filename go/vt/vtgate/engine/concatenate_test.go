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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"
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

// Hold the first arm open to distinguish sequential execution from parallel
// execution, including the ordinary-session path that must remain parallel.
func TestConcatenateSessionConcurrency(t *testing.T) {
	for _, session := range []string{"ordinary", "reserved", "transaction"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", session, stream), func(t *testing.T) {
				result := r("id", "int64", "1")
				sources := []Primitive{
					&fakePrimitive{results: []*sqltypes.Result{result}},
					&fakePrimitive{results: []*sqltypes.Result{result}},
				}
				var first, second atomic.Bool
				release, done := make(chan struct{}), make(chan struct{})
				var once sync.Once
				cursor := &concatenateConcurrencyVCursor{
					before: func(p Primitive) {
						if p == sources[0] {
							first.Store(true)
							<-release
						} else {
							second.Store(true)
						}
					},
				}
				cursor.inTx = session == "transaction"
				cursor.inReserved = session == "reserved"
				concatenate := NewConcatenate(sources, nil)
				var execErr error
				var rows atomic.Int32
				t.Cleanup(func() { once.Do(func() { close(release) }); <-done })
				go func() {
					defer close(done)
					if stream {
						execErr = concatenate.TryStreamExecute(t.Context(), cursor, nil, true, func(qr *sqltypes.Result) error {
							rows.Add(int32(len(qr.Rows)))
							return nil
						})
					} else {
						qr, err := concatenate.TryExecute(t.Context(), cursor, nil, true)
						execErr = err
						if err == nil {
							rows.Add(int32(len(qr.Rows)))
						}
					}
				}()
				require.Eventually(t, first.Load, 30*time.Second, time.Millisecond)
				if session == "ordinary" {
					assert.Eventually(t, second.Load, 30*time.Second, time.Millisecond, "ordinary sessions retain parallel arms")
				} else {
					assert.Never(t, second.Load, 100*time.Millisecond, time.Millisecond, "arms must not share a session connection concurrently")
				}
				once.Do(func() { close(release) })
				<-done
				require.NoError(t, execErr)
				require.Equal(t, int32(2), rows.Load())
			})
		}
	}
}

type concatenateConcurrencyVCursor struct {
	noopVCursor
	before func(Primitive)
}

func (c *concatenateConcurrencyVCursor) ExecutePrimitive(ctx context.Context, p Primitive, vars map[string]*querypb.BindVariable, fields bool) (*sqltypes.Result, error) {
	c.before(p)
	return c.noopVCursor.ExecutePrimitive(ctx, p, vars, fields)
}

func (c *concatenateConcurrencyVCursor) StreamExecutePrimitive(ctx context.Context, p Primitive, vars map[string]*querypb.BindVariable, fields bool, callback func(*sqltypes.Result) error) error {
	c.before(p)
	return c.noopVCursor.StreamExecutePrimitive(ctx, p, vars, fields, callback)
}

// Serialized streams retain rows until all arm types are known. Their shared
// buffer must respect the row budget, including concurrent source callbacks.
func TestConcatenateSequentialStreamMemoryLimit(t *testing.T) {
	for _, session := range []string{"reserved", "transaction"} {
		for _, tc := range []struct {
			name                                        string
			limit                                       int
			ignore, wrapErrors, async, empty, wantError bool
		}{
			{name: "boundary", limit: 4},
			{name: "across-arms", limit: 3, wantError: true},
			{name: "oversized-chunk", limit: 1, wantError: true},
			{name: "aggregated-error", limit: 3, wrapErrors: true, wantError: true},
			{name: "concurrent-callbacks", limit: 3, async: true, wantError: true},
			{name: "empty", limit: 0, empty: true},
			{name: "explicit-override", limit: 1, ignore: true},
		} {
			t.Run(session+"/"+tc.name, func(t *testing.T) {
				first := r("id", "int32", "1", "2")
				second := r("id", "int64", "3", "4")
				if tc.empty {
					first.Rows, second.Rows = nil, nil
				}
				left := &fakePrimitive{results: []*sqltypes.Result{first}, async: tc.async}
				if tc.async {
					left.results = append(left.results, first)
				}
				right := &fakePrimitive{results: []*sqltypes.Result{second}}
				concatenate := NewConcatenate([]Primitive{left, right}, nil)
				cursor := &concatenateMemoryVCursor{limit: tc.limit, ignore: tc.ignore, wrapErrors: tc.wrapErrors}
				cursor.inTx = session == "transaction"
				cursor.inReserved = session == "reserved"
				var rows [][]sqltypes.Value
				var fields []*querypb.Field
				err := concatenate.TryStreamExecute(t.Context(), cursor, nil, true, func(qr *sqltypes.Result) error {
					if qr.Fields != nil {
						fields = qr.Fields
					}
					rows = append(rows, qr.Rows...)
					return nil
				})
				if tc.wantError {
					require.ErrorContains(t, err, "in-memory row count exceeded allowed limit")
					require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
					require.Equal(t, sqlerror.ERNetPacketTooLarge, sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError).Number())
					require.Empty(t, rows, "a failed buffered union must not deliver partial results")
					require.False(t, cursor.rollback.Load(), "a local row buffer error must not roll back the caller's transaction")
					if tc.limit == 1 || tc.async {
						require.Zero(t, right.curResult, "stop before starting another arm")
					}
					return
				}
				require.NoError(t, err)
				require.Len(t, fields, 1)
				require.Equal(t, sqltypes.Int64, fields[0].Type)
				if tc.empty {
					require.Empty(t, rows)
				} else {
					require.Len(t, rows, 4)
					for _, row := range rows {
						require.Equal(t, sqltypes.Int64, row[0].Type())
					}
				}
			})
		}
	}
}

type concatenateMemoryVCursor struct {
	noopVCursor
	limit              int
	ignore, wrapErrors bool
	rollback           atomic.Bool
}

func (c *concatenateMemoryVCursor) MaxMemoryRows() int { return c.limit }
func (c *concatenateMemoryVCursor) ExceedsMaxMemoryRows(rows int) bool {
	return !c.ignore && rows > c.limit
}

func (c *concatenateMemoryVCursor) StreamExecutePrimitive(ctx context.Context, p Primitive, vars map[string]*querypb.BindVariable, fields bool, callback func(*sqltypes.Result) error) error {
	err := c.noopVCursor.StreamExecutePrimitive(ctx, p, vars, fields, func(result *sqltypes.Result) error {
		err := callback(result)
		// ScatterConn.endAction marks these shard errors for rollback.
		if code := vterrors.Code(err); code == vtrpcpb.Code_RESOURCE_EXHAUSTED || code == vtrpcpb.Code_ABORTED {
			c.rollback.Store(true)
		}
		return err
	})
	if err != nil && c.wrapErrors {
		return fmt.Errorf("aggregated stream error: %v", err)
	}
	return err
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

// TestConcatenateStreamBindVarIsolation ensures the streaming execution paths
// hand each source its own copy of the bindVars map, matching the buffered
// paths. Sources such as the information_schema route mutate bindVars in place
// while routing; without an isolated copy, concurrent sources race on the
// shared map (and can crash vtgate with a concurrent map write).
func TestConcatenateStreamBindVarIsolation(t *testing.T) {
	newSources := func() []Primitive {
		fake := r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2")
		var sources []Primitive
		for i := range 8 {
			sources = append(sources, &fakePrimitive{
				results:      []*sqltypes.Result{fake},
				noLog:        true,
				writeBindVar: fmt.Sprintf("src%d", i),
			})
		}
		return sources
	}

	t.Run("parallel", func(t *testing.T) {
		concatenate := NewConcatenate(newSources(), nil)
		bindVars := map[string]*querypb.BindVariable{"orig": sqltypes.Int64BindVariable(1)}
		_, err := wrapStreamExecute(concatenate, &noopVCursor{}, bindVars, true)
		require.NoError(t, err)
		require.Equal(t, map[string]*querypb.BindVariable{"orig": sqltypes.Int64BindVariable(1)}, bindVars)
	})

	t.Run("sequential", func(t *testing.T) {
		concatenate := NewConcatenate(newSources(), nil)
		bindVars := map[string]*querypb.BindVariable{"orig": sqltypes.Int64BindVariable(1)}
		_, err := wrapStreamExecute(concatenate, &noopVCursor{inTx: true}, bindVars, true)
		require.NoError(t, err)
		require.Equal(t, map[string]*querypb.BindVariable{"orig": sqltypes.Int64BindVariable(1)}, bindVars)
	})
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

func TestConcatenateCoercesBitValues(t *testing.T) {
	bitResult := &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Bit, Charset: collations.CollationBinaryID}},
		Rows:   [][]sqltypes.Value{{sqltypes.MakeTrusted(sqltypes.Bit, []byte{1, 2})}},
	}
	intResult := r("id", "int32", "3")
	concatenate := NewConcatenate([]Primitive{
		&fakePrimitive{results: []*sqltypes.Result{bitResult, bitResult}},
		&fakePrimitive{results: []*sqltypes.Result{intResult, intResult}},
	}, nil)

	result, err := concatenate.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.NoError(t, err)
	require.Equal(t, sqltypes.Int64, result.Fields[0].Type)
	require.Equal(t, "258", result.Rows[0][0].ToString())
	require.Equal(t, "3", result.Rows[1][0].ToString())
}

func TestConcatenatePreservesGeometryValues(t *testing.T) {
	geometry := []byte{0, 1, 2, 3}
	geometryResult := &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "value", Type: sqltypes.Geometry, Charset: collations.CollationBinaryID}},
		Rows:   [][]sqltypes.Value{{sqltypes.MakeTrusted(sqltypes.Geometry, geometry)}},
	}
	stringResult := r("value", "varchar", "shape")
	concatenate := NewConcatenate([]Primitive{
		&fakePrimitive{results: []*sqltypes.Result{geometryResult, geometryResult}},
		&fakePrimitive{results: []*sqltypes.Result{stringResult, stringResult}},
	}, nil)

	result, err := concatenate.TryExecute(t.Context(), &noopVCursor{}, nil, true)
	require.NoError(t, err)
	require.Equal(t, sqltypes.VarChar, result.Fields[0].Type)
	require.Equal(t, geometry, result.Rows[0][0].Raw())
	require.Equal(t, []byte("shape"), result.Rows[1][0].Raw())
}
