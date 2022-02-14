package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
)

func getTestSchemaEngine(t *testing.T) (*Engine, *fakesqldb.DB, func()) {
	db := fakesqldb.New(t)
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325876",
	))
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{})
	db.AddQuery(mysql.BaseShowPrimary, &sqltypes.Result{})
	AddFakeInnoDBReadRowsResult(db, 1)
	se := newEngine(10, 10*time.Second, 10*time.Second, db)
	require.NoError(t, se.Open())
	cancel := func() {
		defer db.Close()
		defer se.Close()
	}
	return se, db, cancel
}
