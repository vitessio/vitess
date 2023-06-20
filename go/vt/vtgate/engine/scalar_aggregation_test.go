/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

func TestEmptyRows(outer *testing.T) {
	testCases := []struct {
		opcode      AggregateOpcode
		origOpcode  AggregateOpcode
		expectedVal string
		expectedTyp string
	}{{
		opcode:      AggregateCountDistinct,
		expectedVal: "0",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateCount,
		expectedVal: "0",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateSumDistinct,
		expectedVal: "null",
		expectedTyp: "decimal",
	}, {
		opcode:      AggregateSum,
		expectedVal: "null",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateSum,
		expectedVal: "0",
		expectedTyp: "int64",
		origOpcode:  AggregateCount,
	}, {
		opcode:      AggregateMax,
		expectedVal: "null",
		expectedTyp: "int64",
	}, {
		opcode:      AggregateMin,
		expectedVal: "null",
		expectedTyp: "int64",
	}}

	for _, test := range testCases {
		outer.Run(test.opcode.String(), func(t *testing.T) {
			assert := assert.New(t)
			fp := &fakePrimitive{
				results: []*sqltypes.Result{sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						test.opcode.String(),
						"int64",
					),
					// Empty input table
				)},
			}

			oa := &ScalarAggregate{
				PreProcess: true,
				Aggregates: []*AggregateParams{{
					Opcode:     test.opcode,
					Col:        0,
					Alias:      test.opcode.String(),
					OrigOpcode: test.origOpcode,
				}},
				Input: fp,
			}

			result, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			assert.NoError(err)

			wantResult := sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					test.opcode.String(),
					test.expectedTyp,
				),
				test.expectedVal,
			)
			assert.Equal(wantResult, result)
		})
	}
}

func TestScalarAggregateStreamExecute(t *testing.T) {
	assert := assert.New(t)
	fields := sqltypes.MakeTestFields(
		"col|weight_string(col)",
		"uint64|varbinary",
	)
	fp := &fakePrimitive{
		allResultsInOneCall: true,
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields,
				"1|null",
			), sqltypes.MakeTestResult(fields,
				"3|null",
			)},
	}

	oa := &ScalarAggregate{
		Aggregates: []*AggregateParams{{
			Opcode: AggregateSum,
			Col:    0,
		}},
		Input:               fp,
		TruncateColumnCount: 1,
		PreProcess:          true,
	}

	var results []*sqltypes.Result
	err := oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	assert.NoError(err)
	// one for the fields, and one for the actual aggregation result
	require.EqualValues(t, 2, len(results), "number of results")

	got := fmt.Sprintf("%v", results[1].Rows)
	assert.Equal("[[UINT64(4)]]", got)
}

// TestScalarAggregateExecuteTruncate checks if truncate works
func TestScalarAggregateExecuteTruncate(t *testing.T) {
	assert := assert.New(t)
	fields := sqltypes.MakeTestFields(
		"col|weight_string(col)",
		"uint64|varbinary",
	)

	fp := &fakePrimitive{
		allResultsInOneCall: true,
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields,
				"1|null", "3|null",
			)},
	}

	oa := &ScalarAggregate{
		Aggregates: []*AggregateParams{{
			Opcode: AggregateSum,
			Col:    0,
		}},
		Input:               fp,
		TruncateColumnCount: 1,
		PreProcess:          true,
	}

	qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, true)
	assert.NoError(err)
	assert.Equal("[[UINT64(4)]]", fmt.Sprintf("%v", qr.Rows))
}

// TestScalarGroupConcatWithAggrOnEngine tests group_concat with full aggregation on engine.
func TestScalarGroupConcatWithAggrOnEngine(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c2",
		"varchar",
	)

	varbinaryFields := sqltypes.MakeTestFields(
		"c2",
		"varbinary",
	)

	textOutFields := sqltypes.MakeTestFields(
		"group_concat(c2)",
		"text",
	)

	var tcases = []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "ending with null",
		inputResult: sqltypes.MakeTestResult(fields,
			"a", "a", "b", "null", "null"),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`a,a,b`),
	}, {
		name:        "empty result",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`null`),
	}, {
		name: "only null value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", "null", "null"),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`null`),
	}, {
		name: "empty string value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"", "", ""),
		expResult: sqltypes.MakeTestResult(textOutFields,
			`,,`),
	}, {
		name: "varbinary column",
		inputResult: sqltypes.MakeTestResult(varbinaryFields,
			"foo", "null", "bar"),
		expResult: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"group_concat(c2)",
			"blob",
		),
			`foo,bar`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateGroupConcat,
					Col:    0,
					Alias:  "group_concat(c2)",
				}},
				Input:        fp,
				AggrOnEngine: true,
				PreProcess:   true,
			}
			qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
				if qr.Fields != nil {
					results.Fields = qr.Fields
				}
				results.Rows = append(results.Rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, results)
		})
	}
}

// TestScalarGroupConcat tests group_concat with partial aggregation on engine.
func TestScalarGroupConcat(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"group_concat(c2)",
		"text",
	)

	varbinaryFields := sqltypes.MakeTestFields(
		"group_concat(c2)",
		"blob",
	)

	var tcases = []struct {
		name        string
		inputResult *sqltypes.Result
		expResult   *sqltypes.Result
	}{{
		name: "ending with null",
		inputResult: sqltypes.MakeTestResult(fields,
			"a", "a", "b", "null", "null"),
		expResult: sqltypes.MakeTestResult(fields,
			`a,a,b`),
	}, {
		name:        "empty result",
		inputResult: sqltypes.MakeTestResult(fields),
		expResult: sqltypes.MakeTestResult(fields,
			`null`),
	}, {
		name: "only null value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"null", "null", "null"),
		expResult: sqltypes.MakeTestResult(fields,
			`null`),
	}, {
		name: "empty string value for concat",
		inputResult: sqltypes.MakeTestResult(fields,
			"", "", ""),
		expResult: sqltypes.MakeTestResult(fields,
			`,,`),
	}, {
		name: "varbinary column",
		inputResult: sqltypes.MakeTestResult(varbinaryFields,
			"foo", "null", "bar"),
		expResult: sqltypes.MakeTestResult(varbinaryFields,
			`foo,bar`),
	}}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fp := &fakePrimitive{results: []*sqltypes.Result{tcase.inputResult}}
			oa := &ScalarAggregate{
				Aggregates: []*AggregateParams{{
					Opcode: AggregateGroupConcat,
					Col:    0,
				}},
				Input: fp,
			}
			qr, err := oa.TryExecute(context.Background(), &noopVCursor{}, nil, false)
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, qr)

			fp.rewind()
			results := &sqltypes.Result{}
			err = oa.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
				if qr.Fields != nil {
					results.Fields = qr.Fields
				}
				results.Rows = append(results.Rows, qr.Rows...)
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, tcase.expResult, results)
		})
	}
}
