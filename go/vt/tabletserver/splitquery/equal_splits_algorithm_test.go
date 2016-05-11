package splitquery

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/splitquery/splitquery_testing"
)

// Table-driven test for equal-splits algorithm.
// Fields are exported so that "%v" would print them using their String() method.
type testCaseType struct {
	SplitColumn        string
	SplitCount         int64
	MinValue           sqltypes.Value
	MaxValue           sqltypes.Value
	ExpectedBoundaries []tuple
}

var testCases = []testCaseType{
	{ // Split the interval [10, 60] into 5 parts.
		SplitColumn: "int64_col",
		SplitCount:  5,
		MinValue:    Int64Value(10),
		MaxValue:    Int64Value(60),
		ExpectedBoundaries: []tuple{
			{Int64Value(20)},
			{Int64Value(30)},
			{Int64Value(40)},
			{Int64Value(50)},
		},
	},
	{ // Split the interval [10, 60] into 4 parts.
		SplitColumn: "int64_col",
		SplitCount:  4,
		MinValue:    Int64Value(10),
		MaxValue:    Int64Value(60),
		ExpectedBoundaries: []tuple{
			{Int64Value(22)},
			{Int64Value(35)},
			{Int64Value(47)},
		},
	},
	{ // Split the interval [-30, 60] into 4 parts.
		SplitColumn: "int64_col",
		SplitCount:  4,
		MinValue:    Int64Value(-30),
		MaxValue:    Int64Value(60),
		ExpectedBoundaries: []tuple{
			{Int64Value(-7)},
			{Int64Value(15)},
			{Int64Value(37)},
		},
	},
	{ // Split the interval [18446744073709551610,18446744073709551615] into 4 parts.
		SplitColumn: "uint64_col",
		SplitCount:  4,
		MinValue:    Uint64Value(18446744073709551610),
		MaxValue:    Uint64Value(18446744073709551615),
		ExpectedBoundaries: []tuple{
			{Uint64Value(18446744073709551611)},
			{Uint64Value(18446744073709551612)},
			{Uint64Value(18446744073709551613)},
		},
	},
	{ // Split the interval [0,95] into 10 parts.
		SplitColumn: "uint64_col",
		SplitCount:  10,
		MinValue:    Uint64Value(0),
		MaxValue:    Uint64Value(95),
		ExpectedBoundaries: []tuple{
			{Uint64Value(9)},
			{Uint64Value(19)},
			{Uint64Value(28)},
			{Uint64Value(38)},
			{Uint64Value(47)},
			{Uint64Value(57)},
			{Uint64Value(66)},
			{Uint64Value(76)},
			{Uint64Value(85)},
		},
	},
	{ // Split the interval [-30.25, 60.25] into 4 parts.
		SplitColumn: "float64_col",
		SplitCount:  4,
		MinValue:    Float64Value(-30.25),
		MaxValue:    Float64Value(60.25),
		ExpectedBoundaries: []tuple{
			{Float64Value(-7.625)},
			{Float64Value(15)},
			{Float64Value(37.625)},
		},
	},
	{ // Split the interval [-30, -30] into 4 parts.
		// (should return an empty boundary list).
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           Int64Value(-30),
		MaxValue:           Int64Value(-30),
		ExpectedBoundaries: []tuple{},
	},
}

func TestEqualSplitsAlgorithm(t *testing.T) {
	// singleTest is a function that executes a single-test.
	singleTest := func(testCase *testCaseType) {
		splitParams, err := NewSplitParamsGivenSplitCount(
			"select * from test_table where int_col > 5",
			/* bindVariables */ nil,
			[]sqlparser.ColIdent{sqlparser.NewColIdent(testCase.SplitColumn)},
			testCase.SplitCount,
			GetSchema(),
		)
		if err != nil {
			t.Errorf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
			return
		}
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
		expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
			fmt.Sprintf(
				"select min(%v), max(%v) from test_table",
				testCase.SplitColumn, testCase.SplitColumn),
			nil /* Bind Variables */)
		expectedCall1.Return(
			&sqltypes.Result{
				Rows: [][]sqltypes.Value{
					{testCase.MinValue, testCase.MaxValue},
				},
			},
			nil)
		algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
		if err != nil {
			t.Errorf("NewEqualSplitsAlgorithm() failed with: %v", err)
			return
		}
		boundaries, err := algorithm.generateBoundaries()
		if err != nil {
			t.Errorf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
			return
		}
		if !reflect.DeepEqual(boundaries, testCase.ExpectedBoundaries) {
			t.Errorf("EqualSplitsAlgorith.generateBoundaries()=%+v, expected: %+v. testCase: %+v",
				boundaries, testCase.ExpectedBoundaries, testCase)
		}
	} // singleTest()
	for _, testCase := range testCases {
		singleTest(&testCase)
	}
}

//TODO(erez): Add test that checks we return an error if column is not numeric (and maybe also
// for other assumptions).
