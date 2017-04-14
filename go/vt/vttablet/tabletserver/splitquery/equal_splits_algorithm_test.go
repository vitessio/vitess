package splitquery

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/splitquery/splitquery_testing"
)

// Table-driven test for equal-splits algorithm.
// Fields are exported so that "%v" would print them using their String() method.
type equalSplitsAlgorithmTestCaseType struct {
	SplitColumn        string
	SplitCount         int64
	MinValue           sqltypes.Value
	MaxValue           sqltypes.Value
	ExpectedBoundaries []tuple
}

var equalSplitsAlgorithmTestCases = []equalSplitsAlgorithmTestCaseType{
	{ // Split the interval [10, 60] into 5 parts.
		SplitColumn: "int64_col",
		SplitCount:  5,
		MinValue:    int64Value(10),
		MaxValue:    int64Value(60),
		ExpectedBoundaries: []tuple{
			{int64Value(20)},
			{int64Value(30)},
			{int64Value(40)},
			{int64Value(50)},
		},
	},
	{ // Split the interval [10, 60] into 4 parts.
		SplitColumn: "int64_col",
		SplitCount:  4,
		MinValue:    int64Value(10),
		MaxValue:    int64Value(60),
		ExpectedBoundaries: []tuple{
			{int64Value(22)},
			{int64Value(35)},
			{int64Value(47)},
		},
	},
	{ // Split the interval [-30, 60] into 4 parts.
		SplitColumn: "int64_col",
		SplitCount:  4,
		MinValue:    int64Value(-30),
		MaxValue:    int64Value(60),
		ExpectedBoundaries: []tuple{
			{int64Value(-7)},
			{int64Value(15)},
			{int64Value(37)},
		},
	},
	{ // Split the interval [18446744073709551610,18446744073709551615] into 4 parts.
		SplitColumn: "uint64_col",
		SplitCount:  4,
		MinValue:    uint64Value(18446744073709551610),
		MaxValue:    uint64Value(18446744073709551615),
		ExpectedBoundaries: []tuple{
			{uint64Value(18446744073709551611)},
			{uint64Value(18446744073709551612)},
			{uint64Value(18446744073709551613)},
		},
	},
	{ // Split the interval [0,95] into 10 parts.
		SplitColumn: "uint64_col",
		SplitCount:  10,
		MinValue:    uint64Value(0),
		MaxValue:    uint64Value(95),
		ExpectedBoundaries: []tuple{
			{uint64Value(9)},
			{uint64Value(19)},
			{uint64Value(28)},
			{uint64Value(38)},
			{uint64Value(47)},
			{uint64Value(57)},
			{uint64Value(66)},
			{uint64Value(76)},
			{uint64Value(85)},
		},
	},
	{ // Split the interval [-30.25, 60.25] into 4 parts.
		SplitColumn: "float64_col",
		SplitCount:  4,
		MinValue:    float64Value(-30.25),
		MaxValue:    float64Value(60.25),
		ExpectedBoundaries: []tuple{
			{float64Value(-7.625)},
			{float64Value(15)},
			{float64Value(37.625)},
		},
	},
	{ // Split the interval [-30, -30] into 4 parts.
		// (should return an empty boundary list).
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           int64Value(-30),
		MaxValue:           int64Value(-30),
		ExpectedBoundaries: []tuple{},
	},
	{ // Split the interval [-30, -29] into 4 parts.
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           int64Value(-30),
		MaxValue:           int64Value(-29),
		ExpectedBoundaries: []tuple{},
	},
	{ // Split the interval [-30, -28] into 4 parts.
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           int64Value(-30),
		MaxValue:           int64Value(-28),
		ExpectedBoundaries: []tuple{{int64Value(-29)}},
	},
	{ // Split the  interval [-30, -28] into 4 parts with a floating-point split-column.
		SplitColumn: "float64_col",
		SplitCount:  4,
		MinValue:    float64Value(-30),
		MaxValue:    float64Value(-28),
		ExpectedBoundaries: []tuple{
			{float64Value(-29.5)},
			{float64Value(-29.0)},
			{float64Value(-28.5)},
		},
	},
}

func TestEqualSplitsAlgorithm(t *testing.T) {
	// singleTest is a function that executes a single-test.
	singleTest := func(testCase *equalSplitsAlgorithmTestCaseType) {
		splitParams, err := NewSplitParamsGivenSplitCount(
			querytypes.BoundQuery{Sql: "select * from test_table where int_col > 5"},
			[]sqlparser.ColIdent{sqlparser.NewColIdent(testCase.SplitColumn)},
			testCase.SplitCount,
			getTestSchema(),
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
	for _, testCase := range equalSplitsAlgorithmTestCases {
		singleTest(&testCase)
	}
}

//TODO(erez): Add test that checks we return an error if column is not numeric (and maybe also
// for other assumptions).
