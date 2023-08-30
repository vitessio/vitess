package mysqlctl

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var queryMap map[string]*sqltypes.Result

func mockExec(query string, maxRows int, wantFields bool) (*sqltypes.Result, error) {
	queryMap = make(map[string]*sqltypes.Result)
	getColsQuery := fmt.Sprintf(GetColumnNamesQuery, "'test'", "'t1'")
	queryMap[getColsQuery] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
			{sqltypes.NewVarChar("col2")},
			{sqltypes.NewVarChar("col3")},
		},
	}

	queryMap["SELECT `col1`, `col2`, `col3` FROM `test`.`t1` WHERE 1 != 1"] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "col1",
			Type: sqltypes.VarChar,
		}, {
			Name: "col2",
			Type: sqltypes.Int64,
		}, {
			Name: "col3",
			Type: sqltypes.VarBinary,
		}},
	}
	getColsQuery = fmt.Sprintf(GetColumnNamesQuery, "database()", "'t2'")
	queryMap[getColsQuery] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
		},
	}

	queryMap["SELECT `col1` FROM `t2` WHERE 1 != 1"] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "col1",
			Type: sqltypes.VarChar,
		}},
	}
	result, ok := queryMap[query]
	if ok {
		return result, nil
	}

	getColsQuery = fmt.Sprintf(GetColumnNamesQuery, "database()", "'with \\' quote'")
	queryMap[getColsQuery] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "column_name",
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("col1")},
		},
	}

	queryMap["SELECT `col1` FROM `with ' quote` WHERE 1 != 1"] = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "col1",
			Type: sqltypes.VarChar,
		}},
	}
	result, ok = queryMap[query]
	if ok {
		return result, nil
	}

	return nil, fmt.Errorf("query %s not found in mock setup", query)
}

func TestColumnList(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	fields, _, err := GetColumns("test", "t1", mockExec)
	require.NoError(t, err)
	require.Equal(t, `[name:"col1" type:VARCHAR name:"col2" type:INT64 name:"col3" type:VARBINARY]`, fmt.Sprintf("%+v", fields))

	fields, _, err = GetColumns("", "t2", mockExec)
	require.NoError(t, err)
	require.Equal(t, `[name:"col1" type:VARCHAR]`, fmt.Sprintf("%+v", fields))

	fields, _, err = GetColumns("", "with ' quote", mockExec)
	require.NoError(t, err)
	require.Equal(t, `[name:"col1" type:VARCHAR]`, fmt.Sprintf("%+v", fields))

}

func TestNormalizedStatement(t *testing.T) {
	tcases := []struct {
		statement string
		db        string
		typ       string
		expect    string
	}{
		{
			statement: "create table mydb.t (id int primary key)",
			db:        "mydb",
			typ:       tmutils.TableBaseTable,
			expect:    "create table mydb.t (id int primary key)",
		},
		{
			statement: "create table `mydb`.t (id int primary key)",
			db:        "mydb",
			typ:       tmutils.TableBaseTable,
			expect:    "create table `mydb`.t (id int primary key)",
		},
		{
			statement: "create view `mydb`.v as select * from t",
			db:        "mydb",
			typ:       tmutils.TableView,
			expect:    "create view {{.DatabaseName}}.v as select * from t",
		},
		{
			statement: "create view `mydb`.v as select * from `mydb`.`t`",
			db:        "mydb",
			typ:       tmutils.TableView,
			expect:    "create view {{.DatabaseName}}.v as select * from {{.DatabaseName}}.t",
		},
		{
			statement: "create view `mydb`.v as select * from `mydb`.mydb",
			db:        "mydb",
			typ:       tmutils.TableView,
			expect:    "create view {{.DatabaseName}}.v as select * from {{.DatabaseName}}.mydb",
		},
		{
			statement: "create view `mydb`.v as select * from `mydb`.`mydb`",
			db:        "mydb",
			typ:       tmutils.TableView,
			expect:    "create view {{.DatabaseName}}.v as select * from {{.DatabaseName}}.mydb",
		},
	}
	ctx := context.Background()
	for _, tcase := range tcases {
		testName := tcase.statement
		t.Run(testName, func(t *testing.T) {
			result, err := normalizedStatement(ctx, tcase.statement, tcase.db, tcase.typ)
			assert.NoError(t, err)
			assert.Equal(t, tcase.expect, result)
		})
	}
}
