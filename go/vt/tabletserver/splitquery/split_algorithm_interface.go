package splitquery

import "github.com/youtube/vitess/go/sqltypes"

type tuple []sqltypes.Value

type SplitAlgorithmInterface interface {
	generateBoundaries() ([]tuple, error)
}

type SQLExecuter interface {
	SQLExecute(sql string, bindVariables map[string]interface{}) (*sqltypes.Result, error)
}
