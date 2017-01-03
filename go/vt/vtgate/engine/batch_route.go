package engine

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
)

// BatchRoute represents the instructions to route a query list to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type BatchRoute struct {
	PlanList []*Plan
}

// Execute performs a non-streaming exec.
func (batchRoute *BatchRoute) Execute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool) (*sqltypes.Result, error) {
	panic("call not expected")
}

// StreamExecute performs a streaming exec.
func (batchRoute *BatchRoute) StreamExecute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool, sendReply func(*sqltypes.Result) error) error {
	panic("call not expected")
}

// GetFields fetches the field info.
func (batchRoute *BatchRoute) GetFields(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	panic("call not expected")
}

// ExecuteBatch performs a non-streaming exec of list of queries.
func (batchRoute *BatchRoute) ExecuteBatch(vcursor VCursor, queryBatchConstruct *queryinfo.QueryBatchConstruct, joinvars map[string]interface{}, wantfields bool) ([]sqltypes.QueryResponse, error) {
	queryResponses := make([]sqltypes.QueryResponse, len(queryBatchConstruct.BoundQueryList))
	// Currently, batch plan is generated with array of individual plan.
	// Plan for each query is then executed.
	// AsTransaction flag is also discarded.
	for i, plan := range batchRoute.PlanList {
		queryConstruct := queryinfo.NewQueryConstruct(queryBatchConstruct.BoundQueryList[i].SQL, queryBatchConstruct.Keyspace, queryBatchConstruct.BoundQueryList[i].BindVars, false)
		result, err := plan.Instructions.Execute(vcursor, queryConstruct, joinvars, wantfields)
		queryResponses[i] = sqltypes.QueryResponse{
			QueryResult: result,
			QueryError:  err,
		}
	}
	return queryResponses, nil
}
