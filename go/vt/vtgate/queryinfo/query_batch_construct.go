package queryinfo

import "github.com/youtube/vitess/go/vt/sqlparser"

// QueryBatchConstruct contains the information about the sql and bindVars to be used by vtgate and engine.
type QueryBatchConstruct struct {
	BoundQueryList []*queryBound
	Keyspace       string
	AsTransaction  bool
}

type queryBound struct {
	SQL      string
	Comments string
	BindVars map[string]interface{}
}

// NewQueryBatchConstruct method initializes the structure.
func NewQueryBatchConstruct(sqlList []string, keyspace string, bindVarsList []map[string]interface{}, asTransaction bool) *QueryBatchConstruct {
	if bindVarsList == nil {
		bindVarsList = make([]map[string]interface{}, len(sqlList))
	}
	boundQueryList := make([]*queryBound, len(sqlList))
	for sqlNum, sql := range sqlList {
		_, comments := sqlparser.SplitTrailingComments(sql)
		bindVars := bindVarsList[sqlNum]
		if bindVars == nil {
			bindVars = make(map[string]interface{})
			bindVarsList[sqlNum] = bindVars
		}
		boundQueryList[sqlNum] = &queryBound{
			SQL:      sql,
			Comments: comments,
			BindVars: bindVars,
		}
	}
	return &QueryBatchConstruct{
		BoundQueryList: boundQueryList,
		Keyspace:       keyspace,
		AsTransaction:  asTransaction,
	}
}
