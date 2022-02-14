package evalengine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
)

// more tests in go/sqlparser/expressions_test.go

func TestBinaryOpTypes(t *testing.T) {
	type testcase struct {
		l, r, e sqltypes.Type
	}
	type ops struct {
		op        ArithmeticOp
		testcases []testcase
	}

	tests := []ops{
		{
			op: &OpAddition{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		}, {
			op: &OpSubstraction{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		}, {
			op: &OpMultiplication{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Int64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Uint64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		}, {
			op: &OpDivision{},
			testcases: []testcase{
				{sqltypes.Int64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Uint64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Float64, sqltypes.Int64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Uint64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Float64, sqltypes.Uint64, sqltypes.Float64},
				{sqltypes.Int64, sqltypes.Float64, sqltypes.Float64},
				{sqltypes.Uint64, sqltypes.Float64, sqltypes.Float64},
				{sqltypes.Float64, sqltypes.Float64, sqltypes.Float64},
			},
		},
	}

	for _, op := range tests {
		for _, tc := range op.testcases {
			name := fmt.Sprintf("%s %s %s", tc.l.String(), reflect.TypeOf(op.op).String(), tc.r.String())
			t.Run(name, func(t *testing.T) {
				result := op.op.typeof(tc.l)
				assert.Equal(t, tc.e, result)
			})
		}
	}
}
