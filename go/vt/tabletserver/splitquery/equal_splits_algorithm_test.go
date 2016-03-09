package splitquery

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/splitquery/splitquery_testing"
)

func TestMultipleBoundariesInt64Col(t *testing.T) {
	// Split the interval [10,60] into 5 parts.
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"int64_col"}, /* splitColumns */
		1000,
		GetSchema(),
	)
	// TODO(erez): Move the setting of splitCount into the constructor of 'splitParams'.
	splitParams.splitCount = 5
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(int64_col), max(int64_col) from test_table", nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{Int64Value(10), Int64Value(60)},
			},
		},
		nil)
	algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	if err != nil {
		t.Fatalf("NewEqualSplitsAlgorithm() failed with: %v", err)
	}
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{
		{Int64Value(20)},
		{Int64Value(30)},
		{Int64Value(40)},
		{Int64Value(50)},
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestMultipleBoundariesInt64ColWithRounding(t *testing.T) {
	// Split the interval [10, 60] into 4 parts.
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"int64_col"}, /* splitColumns */
		1000,
		GetSchema(),
	)
	// TODO(erez): Move the setting of splitCount into the constructor of 'splitParams'.
	splitParams.splitCount = 4
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(int64_col), max(int64_col) from test_table", nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{Int64Value(10), Int64Value(60)},
			},
		},
		nil)
	algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	if err != nil {
		t.Fatalf("NewEqualSplitsAlgorithm() failed with: %v", err)
	}
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{
		{Int64Value(22)},
		{Int64Value(35)},
		{Int64Value(47)},
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestMultipleBoundariesInt64ColWithRoundingAndNegativeNumbers(t *testing.T) {
	// Split the interval [-30, 60] into 4 parts.
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"int64_col"}, /* splitColumns */
		1000,
		GetSchema(),
	)
	// TODO(erez): Move the setting of splitCount into the constructor of 'splitParams'.
	splitParams.splitCount = 4
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(int64_col), max(int64_col) from test_table", nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{Int64Value(-30), Int64Value(60)},
			},
		},
		nil)
	algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	if err != nil {
		t.Fatalf("NewEqualSplitsAlgorithm() failed with: %v", err)
	}
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{
		{Int64Value(-7)},
		{Int64Value(15)},
		{Int64Value(37)},
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestMultipleBoundariesUint64ColWithRounding(t *testing.T) {
	// Split the interval [18446744073709551610,18446744073709551615] into 4 parts.
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"uint64_col"}, /* splitColumns */
		1000,
		GetSchema(),
	)
	// TODO(erez): Move the setting of splitCount into the constructor of 'splitParams'.
	splitParams.splitCount = 4
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(uint64_col), max(uint64_col) from test_table", nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{Uint64Value(18446744073709551610),
					Uint64Value(18446744073709551615)},
			},
		},
		nil)
	algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	if err != nil {
		t.Fatalf("NewEqualSplitsAlgorithm() failed with: %v", err)
	}
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{
		{Uint64Value(18446744073709551611)},
		{Uint64Value(18446744073709551612)},
		{Uint64Value(18446744073709551613)},
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestMultipleBoundariesUint64ColWithRounding2(t *testing.T) {
	// Split the interval [0,95] into 10 parts.
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"uint64_col"}, /* splitColumns */
		1000,
		GetSchema(),
	)
	// TODO(erez): Move the setting of splitCount into the constructor of 'splitParams'.
	splitParams.splitCount = 10
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(uint64_col), max(uint64_col) from test_table", nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{Uint64Value(0),
					Uint64Value(95)},
			},
		},
		nil)
	algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	if err != nil {
		t.Fatalf("NewEqualSplitsAlgorithm() failed with: %v", err)
	}
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := make([]tuple, 0, 10)
	for i := 1; i < 10; i++ {
		expectedBoundaries = append(expectedBoundaries,
			tuple{Uint64Value(uint64(float64(i) * 9.5))})
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestMultipleBoundariesFloat64Col(t *testing.T) {
	// Split the interval [-30.25, 60.25] into 4 parts.
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"float64_col"}, /* splitColumns */
		1000,
		GetSchema(),
	)
	// TODO(erez): Move the setting of splitCount into the constructor of 'splitParams'.
	splitParams.splitCount = 4
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(float64_col), max(float64_col) from test_table", nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{Float64Value(-30.25), Float64Value(60.25)},
			},
		},
		nil)
	algorithm, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	if err != nil {
		t.Fatalf("NewEqualSplitsAlgorithm() failed with: %v", err)
	}
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("EqualSplitsAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{
		{Float64Value(-7.625)},
		{Float64Value(15)},
		{Float64Value(37.625)},
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}
