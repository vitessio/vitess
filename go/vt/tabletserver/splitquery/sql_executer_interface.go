package splitquery

import "github.com/youtube/vitess/go/sqltypes"

// SQLExecuter enacpsulates access to the MySQL database for the this package.
type SQLExecuter interface {
	SQLExecute(sql string, bindVariables map[string]interface{}) (*sqltypes.Result, error)
}

// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source $GOFILE -destination splitquery_testing/mock_sqlexecuter.go  -package splitquery_testing
