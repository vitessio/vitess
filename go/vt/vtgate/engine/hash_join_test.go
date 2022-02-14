package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestHashJoinExecuteSameType(t *testing.T) {
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
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col4|col5|col6",
					"int64|varchar|varchar",
				),
				"1|d|dd",
				"3|e|ee",
				"4|f|ff",
				"3|g|gg",
			),
		},
	}

	// Normal join
	jn := &HashJoin{
		Opcode: InnerJoin,
		Left:   leftPrim,
		Right:  rightPrim,
		Cols:   []int{-1, -2, 1, 2},
		LHSKey: 0,
		RHSKey: 0,
	}
	r, err := jn.TryExecute(&noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|int64|varchar",
		),
		"1|a|1|d",
		"3|c|3|e",
		"3|c|3|g",
	))
}

func TestHashJoinExecuteDifferentType(t *testing.T) {
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
				"5|c|cc",
			),
		},
	}
	rightPrim := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"col4|col5|col6",
					"varchar|varchar|varchar",
				),
				"1.00|d|dd",
				"3|e|ee",
				"2.89|z|zz",
				"4|f|ff",
				"3|g|gg",
				" 5.0toto|g|gg",
				"w|ww|www",
			),
		},
	}

	// Normal join
	jn := &HashJoin{
		Opcode:         InnerJoin,
		Left:           leftPrim,
		Right:          rightPrim,
		Cols:           []int{-1, -2, 1, 2},
		LHSKey:         0,
		RHSKey:         0,
		ComparisonType: querypb.Type_FLOAT64,
	}
	r, err := jn.TryExecute(&noopVCursor{}, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	leftPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	rightPrim.ExpectLog(t, []string{
		`Execute  true`,
	})
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"col1|col2|col4|col5",
			"int64|varchar|varchar|varchar",
		),
		"1|a|1.00|d",
		"3|c|3|e",
		"3|c|3|g",
		"5|c| 5.0toto|g",
	))
}
