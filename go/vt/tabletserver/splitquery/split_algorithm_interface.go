package splitquery

import "github.com/youtube/vitess/go/sqltypes"

type tuple []sqltypes.Value

// SplitAlgorithmInterface defines the interface for a splitting algorithm. A splitting algorithm
// implements the generateBoundaries() method that returns a list of "boundary tuples".
// Each tuple is expected to contain values of the split columns, in the order these are defined
// in SplitParams.splitColumns, at a specific "boundary point". The returned list should be
// ordered using ascending lexicographical order.
// If the resulting list of boundary tuples is: {t1, t2, ..., t_k}, the splitquery.Splitter.Split()
// method would generate k+1 query parts. For i=0,1,...,k, the ith query-part contains the rows
// whose split-columns are in the range [t_i, t_i+1), where t_0 and t_k+1 are taken to be
// a "-infinity" tuple and a "+infinity" tuple, respectively.
type SplitAlgorithmInterface interface {
	generateBoundaries() ([]tuple, error)
}

// SQLExecuter enacpsulates access to the MySQL database.
type SQLExecuter interface {
	SQLExecute(sql string, bindVariables map[string]interface{}) (*sqltypes.Result, error)
}
