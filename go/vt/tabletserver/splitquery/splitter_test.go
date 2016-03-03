package splitquery

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/splitquery/splitquery_testing"
)

type FakeSplitAlgorithm struct {
	boundaries []tuple
}

func (algorithm *FakeSplitAlgorithm) generateBoundaries() ([]tuple, error) {
	return algorithm.boundaries, nil
}

func verifyQueryPartsEqual(t *testing.T, expected, got []querytypes.QuerySplit) {
	if reflect.DeepEqual(expected, got) {
		return
	}
	message := fmt.Sprintf("\nexpected: %v\ngot: %v\n", expected, got)
	if len(expected) != len(got) {
		message += fmt.Sprintf("len is different: expected: %v vs got:%v\n", len(expected), len(got))
		return
	}
	for i := range expected {
		if expected[i].Sql != got[i].Sql {
			message += fmt.Sprintf("expected[%v].Sql:\n%v\n!=\ngot[%v].Sql:\n%v\n",
				i, expected[i].Sql, i, got[i].Sql)
		}
		if expected[i].RowCount != got[i].RowCount {
			message += fmt.Sprintf("expected[%v].RowCount: %v != got[%v].RowCount: %v\n",
				i, expected[i].RowCount, i, got[i].RowCount)
		}
		if !reflect.DeepEqual(expected[i].BindVariables, got[i].BindVariables) {
			message += fmt.Sprintf("expected[%v].BindVariables:\n%v\n!=\ngot[%v].BindVariables:\n%v\n",
				i, expected[i].BindVariables, i, got[i].BindVariables)
		}
	}
	t.Errorf("%s", message)
}

func TestSplit1SplitColumn(t *testing.T) {
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table",
		map[string]interface{}{},
		[]string{"id"},
		1000, // numRowsPerQueryPart
		splitquery_testing.GetSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{splitquery_testing.Int64Value(1)},
				{splitquery_testing.Int64Value(10)},
				{splitquery_testing.Int64Value(50)},
			},
		})
	var queryParts []querytypes.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []querytypes.QuerySplit{
		{
			Sql: "select * from test_table where id < :_splitquery_end_id",
			BindVariables: map[string]interface{}{
				"_splitquery_end_id": int64(1),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id <= id)" +
				" and" +
				" (id < :_splitquery_end_id)",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id": int64(1),
				"_splitquery_end_id":   int64(10),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id <= id)" +
				" and" +
				" (id < :_splitquery_end_id)",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id": int64(10),
				"_splitquery_end_id":   int64(50),
			},
		},
		{
			Sql: "select * from test_table where" +
				" :_splitquery_start_id <= id",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id": int64(50),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplit2SplitColumns(t *testing.T) {
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table",
		map[string]interface{}{},
		[]string{"id", "user_id"},
		1000, // numRowsPerQueryPart
		splitquery_testing.GetSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{splitquery_testing.Int64Value(1), splitquery_testing.Int64Value(2)},
				{splitquery_testing.Int64Value(1), splitquery_testing.Int64Value(3)},
				{splitquery_testing.Int64Value(5), splitquery_testing.Int64Value(1)},
			},
		})
	var queryParts []querytypes.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []querytypes.QuerySplit{
		{
			Sql: "select * from test_table where" +
				" id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)",
			BindVariables: map[string]interface{}{
				"_splitquery_end_id":      int64(1),
				"_splitquery_end_user_id": int64(2),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(2),
				"_splitquery_end_id":        int64(1),
				"_splitquery_end_user_id":   int64(3),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(3),
				"_splitquery_end_id":        int64(5),
				"_splitquery_end_user_id":   int64(1),
			},
		},
		{
			Sql: "select * from test_table where" +
				" :_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id)",
			BindVariables: map[string]interface{}{
				"_splitquery_start_user_id": int64(1),
				"_splitquery_start_id":      int64(5),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplit3SplitColumns(t *testing.T) {
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table",
		map[string]interface{}{},
		[]string{"id", "user_id", "user_id2"},
		1000, // numRowsPerQueryPart
		splitquery_testing.GetSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{
					splitquery_testing.Int64Value(1),
					splitquery_testing.Int64Value(2),
					splitquery_testing.Int64Value(2),
				},
				{
					splitquery_testing.Int64Value(2),
					splitquery_testing.Int64Value(1),
					splitquery_testing.Int64Value(1),
				},
			},
		})
	var queryParts []querytypes.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []querytypes.QuerySplit{
		{
			Sql: "select * from test_table where" +
				" id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and" +
				" (user_id < :_splitquery_end_user_id or" +
				" (user_id = :_splitquery_end_user_id and user_id2 < :_splitquery_end_user_id2)))",
			BindVariables: map[string]interface{}{
				"_splitquery_end_id":       int64(1),
				"_splitquery_end_user_id":  int64(2),
				"_splitquery_end_user_id2": int64(2),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and" +
				" (:_splitquery_start_user_id < user_id or" +
				" (:_splitquery_start_user_id = user_id and :_splitquery_start_user_id2 <= user_id2))))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and" +
				" (user_id < :_splitquery_end_user_id or" +
				" (user_id = :_splitquery_end_user_id and user_id2 < :_splitquery_end_user_id2))))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":       int64(1),
				"_splitquery_start_user_id":  int64(2),
				"_splitquery_start_user_id2": int64(2),
				"_splitquery_end_id":         int64(2),
				"_splitquery_end_user_id":    int64(1),
				"_splitquery_end_user_id2":   int64(1),
			},
		},
		{
			Sql: "select * from test_table where" +
				" :_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and" +
				" (:_splitquery_start_user_id < user_id or" +
				" (:_splitquery_start_user_id = user_id and :_splitquery_start_user_id2 <= user_id2)))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":       int64(2),
				"_splitquery_start_user_id":  int64(1),
				"_splitquery_start_user_id2": int64(1),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithWhereClause(t *testing.T) {
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table where name!='foo'",
		map[string]interface{}{},
		[]string{"id", "user_id"},
		1000, // numRowsPerQueryPart
		splitquery_testing.GetSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{splitquery_testing.Int64Value(1), splitquery_testing.Int64Value(2)},
				{splitquery_testing.Int64Value(1), splitquery_testing.Int64Value(3)},
				{splitquery_testing.Int64Value(5), splitquery_testing.Int64Value(1)},
			},
		})
	var queryParts []querytypes.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []querytypes.QuerySplit{
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"_splitquery_end_id":      int64(1),
				"_splitquery_end_user_id": int64(2),
			},
		},
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" ((:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(2),
				"_splitquery_end_id":        int64(1),
				"_splitquery_end_user_id":   int64(3),
			},
		},
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" ((:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(3),
				"_splitquery_end_id":        int64(5),
				"_splitquery_end_user_id":   int64(1),
			},
		},
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" (:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_user_id": int64(1),
				"_splitquery_start_id":      int64(5),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithExistingBindVariables(t *testing.T) {
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table",
		map[string]interface{}{"foo": int64(100)},
		[]string{"id", "user_id"},
		1000, // numRowsPerQueryPart
		splitquery_testing.GetSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{splitquery_testing.Int64Value(1), splitquery_testing.Int64Value(2)},
				{splitquery_testing.Int64Value(1), splitquery_testing.Int64Value(3)},
				{splitquery_testing.Int64Value(5), splitquery_testing.Int64Value(1)},
			},
		})
	var queryParts []querytypes.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []querytypes.QuerySplit{
		{
			Sql: "select * from test_table where" +
				" id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)",
			BindVariables: map[string]interface{}{
				"foo":                     int64(100),
				"_splitquery_end_id":      int64(1),
				"_splitquery_end_user_id": int64(2),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"foo": int64(100),
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(2),
				"_splitquery_end_id":        int64(1),
				"_splitquery_end_user_id":   int64(3),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
				" and" +
				" (id < :_splitquery_end_id or" +
				" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"foo": int64(100),
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(3),
				"_splitquery_end_id":        int64(5),
				"_splitquery_end_user_id":   int64(1),
			},
		},
		{
			Sql: "select * from test_table where" +
				" :_splitquery_start_id < id or" +
				" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id)",
			BindVariables: map[string]interface{}{
				"foo": int64(100),
				"_splitquery_start_user_id": int64(1),
				"_splitquery_start_id":      int64(5),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithEmptyBoundaryList(t *testing.T) {
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"select * from test_table",
		map[string]interface{}{"foo": int64(100)},
		[]string{"id", "user_id"},
		1000,
		splitquery_testing.GetSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{},
		})
	var queryParts []querytypes.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []querytypes.QuerySplit{
		{
			Sql: "select * from test_table",
			BindVariables: map[string]interface{}{
				"foo": int64(100),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}
