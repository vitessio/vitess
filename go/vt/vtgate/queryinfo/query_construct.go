package queryinfo

import "github.com/youtube/vitess/go/vt/sqlparser"

// QueryConstruct contains the information about the sql and bindVars to be used by vtgate and engine.
type QueryConstruct struct {
	SQL              string
	Comments         string
	Keyspace         string
	BindVars         map[string]interface{}
	NotInTransaction bool
}

// NewQueryConstruct method initializes the structure.
func NewQueryConstruct(sql, keyspace string, bindVars map[string]interface{}, notInTransaction bool) *QueryConstruct {
	query, comments := sqlparser.SplitTrailingComments(sql)
	return &QueryConstruct{
		SQL:              query,
		Comments:         comments,
		Keyspace:         keyspace,
		BindVars:         bindVars,
		NotInTransaction: notInTransaction,
	}
}
