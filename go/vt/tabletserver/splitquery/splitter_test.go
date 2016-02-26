package splitquery

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

func getSchema() map[string]*schema.Table {
	table := schema.Table{
		Name: "test_table",
	}
	zero, _ := sqltypes.BuildValue(0)
	table.AddColumn("id", sqltypes.Int64, zero, "")
	table.AddColumn("id2", sqltypes.Int64, zero, "")
	table.AddColumn("count", sqltypes.Int64, zero, "")
	table.PKColumns = []int{0}
	primaryIndex := table.AddIndex("PRIMARY")
	primaryIndex.AddColumn("id", 12345)

	id2Index := table.AddIndex("idx_id2")
	id2Index.AddColumn("id2", 1234)

	result := make(map[string]*schema.Table)
	result["test_table"] = &table

	tableNoPK := schema.Table{
		Name: "test_table_no_pk",
	}
	tableNoPK.AddColumn("id", sqltypes.Int64, zero, "")
	tableNoPK.PKColumns = []int{}
	result["test_table_no_pk"] = &tableNoPK

	return result
}

func Int64Value(value int64) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt([]byte{}, value, 10))
}

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

func TestSplit(t *testing.T) {
	splitParams, err := NewSplitParams("select * from test_table",
		map[string]interface{}{}, []string{"id", "user_id"}, getSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{Int64Value(1), Int64Value(2)},
				{Int64Value(1), Int64Value(3)},
				{Int64Value(5), Int64Value(1)},
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
				" (id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"_splitquery_end_id":      int64(1),
				"_splitquery_end_user_id": int64(2),
			},
		},
		{
			Sql: "select * from test_table where" +
				" ((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))" +
				" and" +
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id)))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(2),
				"_splitquery_end_id":        int64(1),
				"_splitquery_end_user_id":   int64(3),
			},
		},
		{
			Sql: "select * from test_table where" +
				" ((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))" +
				" and" +
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id)))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(3),
				"_splitquery_end_id":        int64(5),
				"_splitquery_end_user_id":   int64(1),
			},
		},
		{
			Sql: "select * from test_table where" +
				" (:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_user_id": int64(1),
				"_splitquery_start_id":      int64(5),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithWhereClause(t *testing.T) {
	splitParams, err := NewSplitParams("select * from test_table where name!='foo'",
		map[string]interface{}{}, []string{"id", "user_id"}, getSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{Int64Value(1), Int64Value(2)},
				{Int64Value(1), Int64Value(3)},
				{Int64Value(5), Int64Value(1)},
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
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id)))",
			BindVariables: map[string]interface{}{
				"_splitquery_end_id":      int64(1),
				"_splitquery_end_user_id": int64(2),
			},
		},
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" (((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))" +
				" and" +
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id))))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(2),
				"_splitquery_end_id":        int64(1),
				"_splitquery_end_user_id":   int64(3),
			},
		},
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" (((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))" +
				" and" +
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id))))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_id":      int64(1),
				"_splitquery_start_user_id": int64(3),
				"_splitquery_end_id":        int64(5),
				"_splitquery_end_user_id":   int64(1),
			},
		},
		{
			Sql: "select * from test_table where (name != 'foo') and" +
				" ((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))",
			BindVariables: map[string]interface{}{
				"_splitquery_start_user_id": int64(1),
				"_splitquery_start_id":      int64(5),
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithExistingBindVariables(t *testing.T) {
	splitParams, err := NewSplitParams("select * from test_table",
		map[string]interface{}{"foo": int64(100)}, []string{"id", "user_id"}, getSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{Int64Value(1), Int64Value(2)},
				{Int64Value(1), Int64Value(3)},
				{Int64Value(5), Int64Value(1)},
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
				" (id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id))",
			BindVariables: map[string]interface{}{
				"foo":                     int64(100),
				"_splitquery_end_id":      int64(1),
				"_splitquery_end_user_id": int64(2),
			},
		},
		{
			Sql: "select * from test_table where" +
				" ((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))" +
				" and" +
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id)))",
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
				" ((:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id)))" +
				" and" +
				" ((id < :_splitquery_end_id) or" +
				" ((id = :_splitquery_end_id) and (user_id < :_splitquery_end_user_id)))",
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
				" (:_splitquery_start_id < id) or" +
				" ((:_splitquery_start_id = id) and (:_splitquery_start_user_id <= user_id))",
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
	splitParams, err := NewSplitParams("select * from test_table",
		map[string]interface{}{"foo": int64(100)}, []string{"id", "user_id"}, getSchema())
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
