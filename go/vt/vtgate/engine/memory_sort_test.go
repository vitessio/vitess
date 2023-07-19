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
	"testing"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func init() {
	// We require MySQL 8.0 collations for the comparisons in the tests
	mySQLVersion := "8.0.0"
	servenv.SetMySQLServerVersionForTest(mySQLVersion)
	collationEnv = collations.NewEnvironment(mySQLVersion)
}

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
		OrderBy: []OrderByParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input: fp,
	}

	result, err := ms.TryExecute(context.Background(), &noopVCursor{}, nil, false)
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
	ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.TryExecute(context.Background(), &noopVCursor{}, bv, false)
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
		OrderBy: []OrderByParams{{
			WeightStringCol: 0,
			Col:             1,
		}},
		Input: fp,
	}
	var results []*sqltypes.Result

	t.Run("order by weight string", func(t *testing.T) {

		err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
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
		ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
		bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

		results = nil
		err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, bv, true, func(qr *sqltypes.Result) error {
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
		OrderBy: []OrderByParams{{
			WeightStringCol: 1,
			Col:             0,
		}},
		Input: fp,
	}

	result, err := ms.TryExecute(context.Background(), &noopVCursor{}, nil, false)
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
	ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.TryExecute(context.Background(), &noopVCursor{}, bv, false)
	require.NoError(t, err)

	wantResult = sqltypes.MakeTestResult(
		fields,
		"a|1",
		"a|1",
		"g|2",
	)
	utils.MustMatch(t, wantResult, result)
}

func TestMemorySortStreamExecuteCollation(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"normal",
		"varchar",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"c",
			"d",
			"cs",
			"cs",
			"c",
		)},
	}

	collationID, _ := collations.Local().LookupID("utf8mb4_hu_0900_ai_ci")
	ms := &MemorySort{
		OrderBy: []OrderByParams{{
			Col:         0,
			Type:        sqltypes.VarChar,
			CollationID: collationID,
		}},
		Input: fp,
	}

	var results []*sqltypes.Result
	t.Run("order by collation", func(t *testing.T) {
		results = nil
		err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		})
		require.NoError(t, err)

		wantResults := sqltypes.MakeTestStreamingResults(
			fields,
			"c",
			"c",
			"cs",
			"cs",
			"d",
		)
		utils.MustMatch(t, wantResults, results)
	})

	t.Run("Descending order by collation", func(t *testing.T) {
		ms.OrderBy[0].Desc = true
		fp.rewind()
		results = nil
		err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		})
		require.NoError(t, err)

		wantResults := sqltypes.MakeTestStreamingResults(
			fields,
			"d",
			"cs",
			"cs",
			"c",
			"c",
		)
		utils.MustMatch(t, wantResults, results)
	})

	t.Run("Limit test", func(t *testing.T) {
		fp.rewind()
		ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
		bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

		results = nil
		err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, bv, true, func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		})
		require.NoError(t, err)

		wantResults := sqltypes.MakeTestStreamingResults(
			fields,
			"d",
			"cs",
			"cs",
		)
		utils.MustMatch(t, wantResults, results)
	})
}

func TestMemorySortExecuteCollation(t *testing.T) {
	fields := sqltypes.MakeTestFields(
		"c1",
		"varchar",
	)
	fp := &fakePrimitive{
		results: []*sqltypes.Result{sqltypes.MakeTestResult(
			fields,
			"c",
			"d",
			"cs",
			"cs",
			"c",
		)},
	}

	collationID, _ := collations.Local().LookupID("utf8mb4_hu_0900_ai_ci")
	ms := &MemorySort{
		OrderBy: []OrderByParams{{
			Col:         0,
			Type:        sqltypes.VarChar,
			CollationID: collationID,
		}},
		Input: fp,
	}

	result, err := ms.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		fields,
		"c",
		"c",
		"cs",
		"cs",
		"d",
	)
	utils.MustMatch(t, wantResult, result)

	fp.rewind()
	ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.TryExecute(context.Background(), &noopVCursor{}, bv, false)
	require.NoError(t, err)

	wantResult = sqltypes.MakeTestResult(
		fields,
		"c",
		"c",
		"cs",
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
		OrderBy: []OrderByParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input: fp,
	}

	var results []*sqltypes.Result
	err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
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
	ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	results = nil
	err = ms.TryStreamExecute(context.Background(), &noopVCursor{}, bv, true, func(qr *sqltypes.Result) error {
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

	got, err := ms.GetFields(context.Background(), nil, nil)
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
		OrderBy: []OrderByParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	result, err := ms.TryExecute(context.Background(), &noopVCursor{}, nil, false)
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
		OrderBy: []OrderByParams{{
			WeightStringCol: -1,
			Col:             1,
		}},
		Input:               fp,
		TruncateColumnCount: 2,
	}

	var results []*sqltypes.Result
	err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, true, func(qr *sqltypes.Result) error {
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
		OrderBy: []OrderByParams{{
			Col:             1,
			WeightStringCol: -1,
		}, {
			Col:             0,
			WeightStringCol: -1,
			Desc:            true,
		}},
		Input: fp,
	}

	result, err := ms.TryExecute(context.Background(), &noopVCursor{}, nil, false)
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
	ms.UpperLimit = evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
	bv := map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)}

	result, err = ms.TryExecute(context.Background(), &noopVCursor{}, bv, false)
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
			OrderBy: []OrderByParams{{
				WeightStringCol: -1,
				Col:             1,
			}},
			Input: fp,
		}

		testIgnoreMaxMemoryRows = test.ignoreMaxMemoryRows
		err := ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
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
		OrderBy: []OrderByParams{{
			WeightStringCol: -1,
			Col:             0,
		}},
		Input: fp,
	}

	_, err := ms.TryExecute(context.Background(), &noopVCursor{}, nil, false)
	want := "cannot compare strings, collation is unknown or unsupported (collation ID: 0)"
	if err == nil || err.Error() != want {
		t.Errorf("Execute err: %v, want %v", err, want)
	}

	fp.rewind()
	err = ms.TryStreamExecute(context.Background(), &noopVCursor{}, nil, false, func(qr *sqltypes.Result) error {
		return nil
	})
	if err == nil || err.Error() != want {
		t.Errorf("StreamExecute err: %v, want %v", err, want)
	}
}
