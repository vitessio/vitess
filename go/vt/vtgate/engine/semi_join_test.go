package engine

import (
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSemiJoinExecute(t *testing.T) {
	leftPrim := &fakePrimitive{
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
	rightFields := sqltypes.MakeTestFields(
		"col4|col5|col6",
		"int64|varchar|varchar",
	)
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				rightFields,
				"4|d|dd",
			),
			sqltypes.MakeTestResult(
				rightFields,
			),
			sqltypes.MakeTestResult(
				rightFields,
				"5|e|ee",
				"6|f|ff",
				"7|g|gg",
			),
		},
	}
	bv := map[string]*querypb.BindVariable{
		"a": sqltypes.Int64BindVariable(10),
	}

	jn := &SemiJoin{
		Left:  leftPrim,
		Right: rightPrim,
		Vars: map[string]int{
			"bv": 1,
		},
		Cols: []int{-1, -2, -3},
	}
	r, err := jn.TryExecute(&noopVCursor{}, bv, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"10" true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"a" false`,
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"b" false`,
		`Execute a: type:INT64 value:"10" bv: type:VARCHAR value:"c" false`,
	})
	utils.MustMatch(t, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col3",
			"int64|varchar|varchar",
		),
		"1|a|aa",
		"3|c|cc",
	), r)
}

func TestSemiJoinStreamExecute(t *testing.T) {
	leftPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"1|a|aa",
				"2|b|bb",
			), sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col1|col2|col3",
					"int64|varchar|varchar",
				),
				"3|c|cc",
				"4|d|dd",
			),
		},
		allResultsInOneCall: true,
	}
	rightFields := sqltypes.MakeTestFields(
		"col4|col5|col6",
		"int64|varchar|varchar",
	)
	rightPrim := &fakePrimitive{
		// we'll return non-empty results for rows 2 and 4
		results: sqltypes.MakeTestStreamingResults(rightFields,
			"4|d|dd",
			"---",
			"---",
			"5|e|ee",
			"6|f|ff",
			"7|g|gg",
		),
	}

	jn := &SemiJoin{
		Left:  leftPrim,
		Right: rightPrim,
		Vars: map[string]int{
			"bv": 1,
		},
		Cols: []int{-1, -2, -3},
	}
	r, err := wrapStreamExecute(jn, &noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`StreamExecute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`StreamExecute bv: type:VARCHAR value:"a" false`,
		`StreamExecute bv: type:VARCHAR value:"b" false`,
		`StreamExecute bv: type:VARCHAR value:"c" false`,
		`StreamExecute bv: type:VARCHAR value:"d" false`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col3",
			"int64|varchar|varchar",
		),
		"2|b|bb",
		"4|d|dd",
	))
}
