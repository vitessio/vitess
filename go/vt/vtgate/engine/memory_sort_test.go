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
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestMemorySortExecute(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"g|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input: fp,
	}

	result, err := ms.Execute(nil, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"g|2",
		"c|3",
		"c|4",
	)
	utils.MustMatch(t, wantResult, result)

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewArgument(":__upper_limit"))
	require.NoError(t, err)
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.Execute(nil, bv, false)
	require.NoError(t, err)

	wantResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"g|2",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestMemorySortStreamExecuteWeightString(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"weightString|normal",
		"varbinary|varchar",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"null|x",
			"g|d",
			"a|a",
			"c|t",
			"f|p",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: 0,
			Col:             1,
		}},
		Input: fp,
	}
	var results []*sqltypes.Result

	t.Run("order by weight string", func(t *testing.T) {

		err := ms.StreamExecute(&noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		})
		require.NoError(t, err)

		wantResults := sqltypes.MakeTestStreamingResults(
			fields,
			"null|x",
			"a|a",
			"c|t",
			"f|p",
			"g|d",
		)
		utils.MustMatch(t, wantResults, results)
	})

	t.Run("Limit test", func(t *testing.T) {
		fp.rewind()
		upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewArgument(":__upper_limit"))
		require.NoError(t, err)
		ms.UpperLimit = upperlimit
		bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

		results = nil
		err = ms.StreamExecute(&noopVCursor{}, bv, false, func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		})
		require.NoError(t, err)

		wantResults := sqltypes.MakeTestStreamingResults(
			fields,
			"null|x",
			"a|a",
			"c|t",
		)
		utils.MustMatch(t, wantResults, results)
	})
}

func TestMemorySortExecuteWeightString(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varchar|varbinary",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"g|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: 1,
			Col:             0,
		}},
		Input: fp,
	}

	result, err := ms.Execute(nil, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"g|2",
		"c|3",
		"c|4",
	)
	utils.MustMatch(t, wantResult, result)

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewArgument(":__upper_limit"))
	require.NoError(t, err)
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.Execute(nil, bv, false)
	require.NoError(t, err)

	wantResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"g|2",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestMemorySortStreamExecute(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"g|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input: fp,
	}

	var results []*sqltypes.Result
	err := ms.StreamExecute(&noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	wantResults := sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"a|1",
		"g|2",
		"c|3",
		"c|4",
	)
	utils.MustMatch(t, wantResults, results)

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewArgument(":__upper_limit"))
	require.NoError(t, err)
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	results = nil
	err = ms.StreamExecute(&noopVCursor{}, bv, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	wantResults = sqltypes.MakeTestStreamingResults(
		fields,
		"a|1",
		"a|1",
		"g|2",
	)
	utils.MustMatch(t, wantResults, results)
}

func TestMemorySortGetFields(t *testing.T) {
	result := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2",
			"int64|varchar",
		),
	)
	fp := &fakePrimitive{results: []*sqltypes.Result{result}}

	ms := &MemorySort{Input: fp}

	got, err := ms.GetFields(nil, nil)
	require.NoError(t, err)
	utils.MustMatch(t, result, got)
}

func TestMemorySortExecuteTruncate(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2|c3",
		"varbinary|decimal|int64",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1|1",
			"g|2|1",
			"a|1|1",
			"c|4|1",
			"c|3|1",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	result, err := ms.Execute(nil, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields[:2],
		"a|1",
		"a|1",
		"g|2",
		"c|3",
		"c|4",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestMemorySortStreamExecuteTruncate(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2|c3",
		"varbinary|decimal|int64",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1|1",
			"g|2|1",
			"a|1|1",
			"c|4|1",
			"c|3|1",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	var results []*sqltypes.Result
	err := ms.StreamExecute(&noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		results = append(results, qr)
		return nil
	})
	require.NoError(t, err)

	wantResults := sqltypes.MakeTestStreamingResults(
		fields[:2],
		"a|1",
		"a|1",
		"g|2",
		"c|3",
		"c|4",
	)
	utils.MustMatch(t, wantResults, results)
}

func TestMemorySortMultiColumn(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"b|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			Col:             1,
			WeightStringCol: -1,
		}, {
			Col:             0,
			WeightStringCol: -1,
			Desc:            true,
		}},
		Input: fp,
	}

	result, err := ms.Execute(nil, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"b|1",
		"a|1",
		"b|2",
		"c|3",
		"c|4",
	)
	utils.MustMatch(t, wantResult, result)

	fp.rewind()
	upperlimit, err := sqlparser.NewPlanValue(sqlparser.NewArgument(":__upper_limit"))
	require.NoError(t, err)
	ms.UpperLimit = upperlimit
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.Execute(nil, bv, false)
	require.NoError(t, err)

	wantResult = sqltypes.MakeTestResult(
		fields,
		"b|1",
		"a|1",
		"b|2",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestMemorySortMaxMemoryRows(t *testing.T) {
	saveMax := testMaxMemoryRows
	saveIgnore := testIgnoreMaxMemoryRows
	testMaxMemoryRows = 3
	defer func() {
		testMaxMemoryRows = saveMax
		testIgnoreMaxMemoryRows = saveIgnore
	}()

	testCases := []struct {
		ignoreMaxMemoryRows bool
		err                 string
	}{
		{true, ""},
		{false, "in-memory row count exceeded allowed limit of 3"},
	}
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varbinary|decimal",
	)
	for _, test := range testCases {
		fp := &fakePrimitive{
			results: []*sqltypes.Result{sqltypes.MakeTestResult(
				fields,
				"a|1",
				"b|2",
				"a|1",
				"c|4",
				"c|3",
			)},
		}

		ms := &MemorySort{
			OrderBy: []OrderbyParams{{
				WeightStringCol: -1,
				Col:             1,
			}},
			Input: fp,
		}

		testIgnoreMaxMemoryRows = test.ignoreMaxMemoryRows
		err := ms.StreamExecute(&noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
			return nil
		})
		if testIgnoreMaxMemoryRows {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, test.err)
		}
	}
}

func TestMemorySortExecuteNoVarChar(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1|c2",
		"varchar|decimal",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"a|1",
			"b|2",
			"a|1",
			"c|4",
			"c|3",
		)},
	}

	ms := &MemorySort{
		OrderBy: []OrderbyParams{{
			WeightStringCol: -1,
			Col:             0,
		}},
		Input: fp,
	}

	_, err := ms.Execute(nil, nil, false)
	want := "types are not comparable: VARCHAR vs VARCHAR"
	if err == nil || err.Error() != want {
		t.Errorf("Execute err: %v, want %v", err, want)
	}

	fp.rewind()
	err = ms.StreamExecute(&noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		return nil
	})
	if err == nil || err.Error() != want {
		t.Errorf("StreamExecute err: %v, want %v", err, want)
	}
}
