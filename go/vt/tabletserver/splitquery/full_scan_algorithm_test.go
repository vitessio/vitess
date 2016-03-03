package splitquery

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/splitquery/mock_splitquery"
)

func TestMultipleBoundaries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"id", "user_id"}, /* splitColumns */
		1000,
		getSchema(),
	)
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := mock_splitquery.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select id, user_id from test_table"+
			" where int_col > 5"+
			" order by id asc, user_id asc"+
			" limit 1000, 1",
		map[string]interface{}{})
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{Int64Value(1), Int64Value(1)}},
		},
		nil)
	expectedCall2 := mockSQLExecuter.EXPECT().SQLExecute(
		"select id, user_id from test_table"+
			" where (int_col > 5) and"+
			" (:_splitquery_prev_id < id or"+
			" (:_splitquery_prev_id = id and :_splitquery_prev_user_id <= user_id))"+
			" order by id asc, user_id asc"+
			" limit 1000, 1",
		map[string]interface{}{
			"_splitquery_prev_id":      int64(1),
			"_splitquery_prev_user_id": int64(1),
		})
	expectedCall2.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{Int64Value(2), Int64Value(10)}},
		},
		nil)
	expectedCall2.After(expectedCall1)
	expectedCall3 := mockSQLExecuter.EXPECT().SQLExecute(
		"select id, user_id from test_table"+
			" where (int_col > 5) and"+
			" (:_splitquery_prev_id < id or"+
			" (:_splitquery_prev_id = id and :_splitquery_prev_user_id <= user_id))"+
			" order by id asc, user_id asc"+
			" limit 1000, 1",
		map[string]interface{}{
			"_splitquery_prev_id":      int64(2),
			"_splitquery_prev_user_id": int64(10),
		})
	expectedCall3.Return(
		&sqltypes.Result{Rows: [][]sqltypes.Value{}}, nil)
	expectedCall3.After(expectedCall2)

	algorithm := NewFullScanAlgorithm(splitParams, mockSQLExecuter)
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("FullScanAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{
		{Int64Value(1), Int64Value(1)},
		{Int64Value(2), Int64Value(10)},
	}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestSmallNumberOfRows(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"id", "user_id"}, /* splitColumns */
		1000,
		getSchema(),
	)
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := mock_splitquery.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select id, user_id from test_table"+
			" where int_col > 5"+
			" order by id asc, user_id asc"+
			" limit 1000, 1",
		map[string]interface{}{})
	expectedCall1.Return(
		&sqltypes.Result{Rows: [][]sqltypes.Value{}}, nil)

	algorithm := NewFullScanAlgorithm(splitParams, mockSQLExecuter)
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("FullScanAlgorithm.generateBoundaries() failed with: %v", err)
	}
	expectedBoundaries := []tuple{}
	if !reflect.DeepEqual(expectedBoundaries, boundaries) {
		t.Fatalf("expected: %v, got: %v", expectedBoundaries, boundaries)
	}
}

func TestSQLExecuterReturnsError(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where int_col > 5",
		nil, /* bindVariables */
		[]string{"id", "user_id"}, /* splitColumns */
		1000,
		getSchema(),
	)
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := mock_splitquery.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select id, user_id from test_table"+
			" where int_col > 5"+
			" order by id asc, user_id asc"+
			" limit 1000, 1",
		map[string]interface{}{})
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{{Int64Value(1), Int64Value(1)}},
		},
		nil)
	expectedCall2 := mockSQLExecuter.EXPECT().SQLExecute(
		"select id, user_id from test_table"+
			" where (int_col > 5) and"+
			" (:_splitquery_prev_id < id or"+
			" (:_splitquery_prev_id = id and :_splitquery_prev_user_id <= user_id))"+
			" order by id asc, user_id asc"+
			" limit 1000, 1",
		map[string]interface{}{
			"_splitquery_prev_id":      int64(1),
			"_splitquery_prev_user_id": int64(1),
		})
	expectedErr := fmt.Errorf("Error accessing database!")
	expectedCall2.Return(nil, expectedErr)
	algorithm := NewFullScanAlgorithm(splitParams, mockSQLExecuter)
	boundaries, err := algorithm.generateBoundaries()
	if err != expectedErr {
		t.Fatalf("FullScanAlgorithm.generateBoundaries() did not fail as expected. err: %v", err)
	}
	if boundaries != nil {
		t.Fatalf("boundaries: %v, expected: nil", boundaries)
	}
}
