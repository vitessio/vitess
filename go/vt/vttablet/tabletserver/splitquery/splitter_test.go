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
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/splitquery/splitquery_testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

type FakeSplitAlgorithm struct {
	boundaries   []tuple
	splitColumns []*schema.TableColumn
}

func (a *FakeSplitAlgorithm) generateBoundaries() ([]tuple, error) {
	return a.boundaries, nil
}
func (a *FakeSplitAlgorithm) getSplitColumns() []*schema.TableColumn {
	return a.splitColumns
}

func verifyQueryPartsEqual(t *testing.T, expected, got []*querypb.QuerySplit) {
	if reflect.DeepEqual(expected, got) {
		return
	}
	message := fmt.Sprintf("\nexpected: %v\ngot: %v\n", expected, got)
	if len(expected) != len(got) {
		message += fmt.Sprintf("len is different: expected: %v vs got:%v\n", len(expected), len(got))
		return
	}
	for i := range expected {
		if expected[i].Query.Sql != got[i].Query.Sql {
			message += fmt.Sprintf("expected[%v].Sql:\n%v\n!=\ngot[%v].Sql:\n%v\n",
				i, expected[i].Query.Sql, i, got[i].Query.Sql)
		}
		if expected[i].RowCount != got[i].RowCount {
			message += fmt.Sprintf("expected[%v].RowCount: %v != got[%v].RowCount: %v\n",
				i, expected[i].RowCount, i, got[i].RowCount)
		}
		if !reflect.DeepEqual(expected[i].Query.BindVariables, got[i].Query.BindVariables) {
			message += fmt.Sprintf("expected[%v].BindVariables:\n%v\n!=\ngot[%v].BindVariables:\n%v\n",
				i, expected[i].Query.BindVariables, i, got[i].Query.BindVariables)
		}
	}
	t.Errorf("%s", message)
}

func TestSplit1SplitColumn(t *testing.T) {
	splitParams, err := NewSplitParamsGivenNumRowsPerQueryPart(
		&querypb.BoundQuery{
			Sql:           "select * from test_table",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		[]sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		1000, // numRowsPerQueryPart
		getTestSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{int64Value(1)},
				{int64Value(10)},
				{int64Value(50)},
			},
			splitColumns: splitParams.splitColumns,
		})
	var queryParts []*querypb.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where id < :_splitquery_end_id",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_end_id": sqltypes.Int64BindVar(1),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id <= id)" +
					" and" +
					" (id < :_splitquery_end_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id": sqltypes.Int64BindVar(1),
					"_splitquery_end_id":   sqltypes.Int64BindVar(10),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id <= id)" +
					" and" +
					" (id < :_splitquery_end_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id": sqltypes.Int64BindVar(10),
					"_splitquery_end_id":   sqltypes.Int64BindVar(50),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" :_splitquery_start_id <= id",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id": sqltypes.Int64BindVar(50),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplit2SplitColumns(t *testing.T) {
	splitParams, err := NewSplitParamsGivenNumRowsPerQueryPart(
		&querypb.BoundQuery{
			Sql:           "select * from test_table",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		[]sqlparser.ColIdent{
			sqlparser.NewColIdent("id"),
			sqlparser.NewColIdent("user_id"),
		}, /* splitColumns */
		1000, // numRowsPerQueryPart
		getTestSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{int64Value(1), int64Value(2)},
				{int64Value(1), int64Value(3)},
				{int64Value(5), int64Value(1)},
			},
			splitColumns: splitParams.splitColumns,
		})
	var queryParts []*querypb.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_end_id":      sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id": sqltypes.Int64BindVar(2),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
					" and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id":      sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(2),
					"_splitquery_end_id":        sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id":   sqltypes.Int64BindVar(3),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
					" and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id":      sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(3),
					"_splitquery_end_id":        sqltypes.Int64BindVar(5),
					"_splitquery_end_user_id":   sqltypes.Int64BindVar(1),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" :_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_user_id": sqltypes.Int64BindVar(1),
					"_splitquery_start_id":      sqltypes.Int64BindVar(5),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplit3SplitColumns(t *testing.T) {
	splitParams, err := NewSplitParamsGivenNumRowsPerQueryPart(
		&querypb.BoundQuery{
			Sql:           "select * from test_table",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		[]sqlparser.ColIdent{
			sqlparser.NewColIdent("id"),
			sqlparser.NewColIdent("user_id"),
			sqlparser.NewColIdent("user_id2"),
		}, /* splitColumns */
		1000, // numRowsPerQueryPart
		getTestSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{
					int64Value(1),
					int64Value(2),
					int64Value(2),
				},
				{
					int64Value(2),
					int64Value(1),
					int64Value(1),
				},
			},
			splitColumns: splitParams.splitColumns,
		})
	var queryParts []*querypb.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and" +
					" (user_id < :_splitquery_end_user_id or" +
					" (user_id = :_splitquery_end_user_id and user_id2 < :_splitquery_end_user_id2)))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_end_id":       sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id":  sqltypes.Int64BindVar(2),
					"_splitquery_end_user_id2": sqltypes.Int64BindVar(2),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
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
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id":       sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id":  sqltypes.Int64BindVar(2),
					"_splitquery_start_user_id2": sqltypes.Int64BindVar(2),
					"_splitquery_end_id":         sqltypes.Int64BindVar(2),
					"_splitquery_end_user_id":    sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id2":   sqltypes.Int64BindVar(1),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" :_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and" +
					" (:_splitquery_start_user_id < user_id or" +
					" (:_splitquery_start_user_id = user_id and :_splitquery_start_user_id2 <= user_id2)))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id":       sqltypes.Int64BindVar(2),
					"_splitquery_start_user_id":  sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id2": sqltypes.Int64BindVar(1),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithWhereClause(t *testing.T) {
	splitParams, err := NewSplitParamsGivenNumRowsPerQueryPart(
		&querypb.BoundQuery{
			Sql:           "select * from test_table where name!='foo'",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		[]sqlparser.ColIdent{
			sqlparser.NewColIdent("id"),
			sqlparser.NewColIdent("user_id"),
		}, /* splitColumns */
		1000, // numRowsPerQueryPart
		getTestSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{int64Value(1), int64Value(2)},
				{int64Value(1), int64Value(3)},
				{int64Value(5), int64Value(1)},
			},
			splitColumns: splitParams.splitColumns,
		})
	var queryParts []*querypb.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where (name != 'foo') and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_end_id":      sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id": sqltypes.Int64BindVar(2),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where (name != 'foo') and" +
					" ((:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
					" and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id":      sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(2),
					"_splitquery_end_id":        sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id":   sqltypes.Int64BindVar(3),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where (name != 'foo') and" +
					" ((:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
					" and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id":      sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(3),
					"_splitquery_end_id":        sqltypes.Int64BindVar(5),
					"_splitquery_end_user_id":   sqltypes.Int64BindVar(1),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where (name != 'foo') and" +
					" (:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_user_id": sqltypes.Int64BindVar(1),
					"_splitquery_start_id":      sqltypes.Int64BindVar(5),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithExistingBindVariables(t *testing.T) {
	splitParams, err := NewSplitParamsGivenNumRowsPerQueryPart(
		&querypb.BoundQuery{
			Sql:           "select * from test_table",
			BindVariables: map[string]*querypb.BindVariable{"foo": sqltypes.Int64BindVar(100)},
		},
		[]sqlparser.ColIdent{
			sqlparser.NewColIdent("id"),
			sqlparser.NewColIdent("user_id"),
		}, /* splitColumns */
		1000, // numRowsPerQueryPart
		getTestSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries: []tuple{
				{int64Value(1), int64Value(2)},
				{int64Value(1), int64Value(3)},
				{int64Value(5), int64Value(1)},
			},
			splitColumns: splitParams.splitColumns,
		})
	var queryParts []*querypb.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"foo":                     sqltypes.Int64BindVar(100),
					"_splitquery_end_id":      sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id": sqltypes.Int64BindVar(2),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
					" and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
				BindVariables: map[string]*querypb.BindVariable{
					"foo": sqltypes.Int64BindVar(100),
					"_splitquery_start_id":      sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(2),
					"_splitquery_end_id":        sqltypes.Int64BindVar(1),
					"_splitquery_end_user_id":   sqltypes.Int64BindVar(3),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id))" +
					" and" +
					" (id < :_splitquery_end_id or" +
					" (id = :_splitquery_end_id and user_id < :_splitquery_end_user_id))",
				BindVariables: map[string]*querypb.BindVariable{
					"foo": sqltypes.Int64BindVar(100),
					"_splitquery_start_id":      sqltypes.Int64BindVar(1),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(3),
					"_splitquery_end_id":        sqltypes.Int64BindVar(5),
					"_splitquery_end_user_id":   sqltypes.Int64BindVar(1),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" :_splitquery_start_id < id or" +
					" (:_splitquery_start_id = id and :_splitquery_start_user_id <= user_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"foo": sqltypes.Int64BindVar(100),
					"_splitquery_start_user_id": sqltypes.Int64BindVar(1),
					"_splitquery_start_id":      sqltypes.Int64BindVar(5),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestSplitWithEmptyBoundaryList(t *testing.T) {
	splitParams, err := NewSplitParamsGivenNumRowsPerQueryPart(
		&querypb.BoundQuery{
			Sql:           "select * from test_table",
			BindVariables: map[string]*querypb.BindVariable{"foo": sqltypes.Int64BindVar(100)},
		},
		[]sqlparser.ColIdent{
			sqlparser.NewColIdent("id"),
			sqlparser.NewColIdent("user_id"),
		}, /* splitColumns */
		1000,
		getTestSchema())
	if err != nil {
		t.Fatalf("SplitParams.Initialize() failed with: %v", err)
	}
	splitter := NewSplitter(splitParams,
		&FakeSplitAlgorithm{
			boundaries:   []tuple{},
			splitColumns: splitParams.splitColumns,
		})
	var queryParts []*querypb.QuerySplit
	queryParts, err = splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table",
				BindVariables: map[string]*querypb.BindVariable{
					"foo": sqltypes.Int64BindVar(100),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}

func TestWithRealEqualSplits(t *testing.T) {
	splitParams, err := NewSplitParamsGivenSplitCount(
		&querypb.BoundQuery{
			Sql:           "select * from test_table",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		[]sqlparser.ColIdent{sqlparser.NewColIdent("id"), sqlparser.NewColIdent("user_id")},
		3, /* split_count */
		getTestSchema())
	if err != nil {
		t.Fatalf("want: nil, got: %v", err)
	}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockSQLExecuter := splitquery_testing.NewMockSQLExecuter(mockCtrl)
	expectedCall1 := mockSQLExecuter.EXPECT().SQLExecute(
		"select min(id), max(id) from test_table",
		nil /* Bind Variables */)
	expectedCall1.Return(
		&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{int64Value(10), int64Value(3010)},
			},
		},
		nil)
	equalSplits, err := NewEqualSplitsAlgorithm(splitParams, mockSQLExecuter)
	splitter := NewSplitter(splitParams, equalSplits)
	queryParts, err := splitter.Split()
	if err != nil {
		t.Errorf("Splitter.Split() failed with: %v", err)
	}
	expected := []*querypb.QuerySplit{
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where id < :_splitquery_end_id",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_end_id": sqltypes.Int64BindVar(1010),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" (:_splitquery_start_id <= id)" +
					" and" +
					" (id < :_splitquery_end_id)",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id": sqltypes.Int64BindVar(1010),
					"_splitquery_end_id":   sqltypes.Int64BindVar(2010),
				},
			},
		},
		{
			Query: &querypb.BoundQuery{
				Sql: "select * from test_table where" +
					" :_splitquery_start_id <= id",
				BindVariables: map[string]*querypb.BindVariable{
					"_splitquery_start_id": sqltypes.Int64BindVar(2010),
				},
			},
		},
	}
	verifyQueryPartsEqual(t, expected, queryParts)
}
