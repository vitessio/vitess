/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package splitquery

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlparser"
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
		MinValue:    sqltypes.NewInt64(10),
		MaxValue:    sqltypes.NewInt64(60),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewInt64(20)},
			{sqltypes.NewInt64(30)},
			{sqltypes.NewInt64(40)},
			{sqltypes.NewInt64(50)},
		},
	},
	{ // Split the interval [10, 60] into 4 parts.
		SplitColumn: "int64_col",
		SplitCount:  4,
		MinValue:    sqltypes.NewInt64(10),
		MaxValue:    sqltypes.NewInt64(60),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewInt64(22)},
			{sqltypes.NewInt64(35)},
			{sqltypes.NewInt64(47)},
		},
	},
	{ // Split the interval [-30, 60] into 4 parts.
		SplitColumn: "int64_col",
		SplitCount:  4,
		MinValue:    sqltypes.NewInt64(-30),
		MaxValue:    sqltypes.NewInt64(60),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewInt64(-7)},
			{sqltypes.NewInt64(15)},
			{sqltypes.NewInt64(37)},
		},
	},
	{ // Split the interval [18446744073709551610,18446744073709551615] into 4 parts.
		SplitColumn: "uint64_col",
		SplitCount:  4,
		MinValue:    sqltypes.NewUint64(18446744073709551610),
		MaxValue:    sqltypes.NewUint64(18446744073709551615),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewUint64(18446744073709551611)},
			{sqltypes.NewUint64(18446744073709551612)},
			{sqltypes.NewUint64(18446744073709551613)},
		},
	},
	{ // Split the interval [0,95] into 10 parts.
		SplitColumn: "uint64_col",
		SplitCount:  10,
		MinValue:    sqltypes.NewUint64(0),
		MaxValue:    sqltypes.NewUint64(95),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewUint64(9)},
			{sqltypes.NewUint64(19)},
			{sqltypes.NewUint64(28)},
			{sqltypes.NewUint64(38)},
			{sqltypes.NewUint64(47)},
			{sqltypes.NewUint64(57)},
			{sqltypes.NewUint64(66)},
			{sqltypes.NewUint64(76)},
			{sqltypes.NewUint64(85)},
		},
	},
	{ // Split the interval [-30.25, 60.25] into 4 parts.
		SplitColumn: "float64_col",
		SplitCount:  4,
		MinValue:    sqltypes.NewFloat64(-30.25),
		MaxValue:    sqltypes.NewFloat64(60.25),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewFloat64(-7.625)},
			{sqltypes.NewFloat64(15)},
			{sqltypes.NewFloat64(37.625)},
		},
	},
	{ // Split the interval [-30, -30] into 4 parts.
		// (should return an empty boundary list).
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           sqltypes.NewInt64(-30),
		MaxValue:           sqltypes.NewInt64(-30),
		ExpectedBoundaries: []tuple{},
	},
	{ // Split the interval [-30, -29] into 4 parts.
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           sqltypes.NewInt64(-30),
		MaxValue:           sqltypes.NewInt64(-29),
		ExpectedBoundaries: []tuple{},
	},
	{ // Split the interval [-30, -28] into 4 parts.
		SplitColumn:        "int64_col",
		SplitCount:         4,
		MinValue:           sqltypes.NewInt64(-30),
		MaxValue:           sqltypes.NewInt64(-28),
		ExpectedBoundaries: []tuple{{sqltypes.NewInt64(-29)}},
	},
	{ // Split the  interval [-30, -28] into 4 parts with a floating-point split-column.
		SplitColumn: "float64_col",
		SplitCount:  4,
		MinValue:    sqltypes.NewFloat64(-30),
		MaxValue:    sqltypes.NewFloat64(-28),
		ExpectedBoundaries: []tuple{
			{sqltypes.NewFloat64(-29.5)},
			{sqltypes.NewFloat64(-29.0)},
			{sqltypes.NewFloat64(-28.5)},
		},
	},
}

func TestEqualSplitsAlgorithm(t *testing.T) {
	// singleTest is a function that executes a single-test.
	singleTest := func(testCase *equalSplitsAlgorithmTestCaseType) {
		splitParams, err := NewSplitParamsGivenSplitCount(
			&querypb.BoundQuery{Sql: "select * from test_table where int_col > 5"},
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
