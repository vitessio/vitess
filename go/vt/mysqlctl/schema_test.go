package mysqlctl

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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

func TestGetSchema(t *testing.T) {
	uid := uint32(11111)
	cnf := NewMycnf(uid, 6802)
	// Assigning ServerID to be different from tablet UID to make sure that there are no
	// assumptions in the code that those IDs are the same.
	cnf.ServerID = 22222

	// expect these in the output my.cnf
	os.Setenv("KEYSPACE", "test-messagedb")
	os.Setenv("SHARD", "0")
	os.Setenv("TABLET_TYPE", "PRIMARY")
	os.Setenv("TABLET_ID", "11111")
	os.Setenv("TABLET_DIR", TabletDir(uid))
	os.Setenv("MYSQL_PORT", "15306")
	// this is not being passed, so it should be nil
	os.Setenv("MY_VAR", "myvalue")

	dbconfigs.GlobalDBConfigs.InitWithSocket(cnf.SocketFile, collations.MySQL8())
	mysqld := NewMysqld(&dbconfigs.GlobalDBConfigs)
	sc, err := mysqld.GetSchema(context.Background(), mysqld.dbcfgs.DBName, &tabletmanagerdata.GetSchemaRequest{})

	// TODO: This needs to be fixed
	fmt.Println(sc, err)
}
