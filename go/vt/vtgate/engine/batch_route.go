package engine

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
)

// BatchRoute represents the instructions to route a query list to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type BatchRoute struct {
	PlanList     []*Plan
	ExecParallel bool
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
	if batchRoute.ExecParallel {
		return batchRoute.executeUnordered(vcursor, queryBatchConstruct, joinvars, wantfields)
	}
	return batchRoute.executeOrdered(vcursor, queryBatchConstruct, joinvars, wantfields)
}

func (batchRoute *BatchRoute) executeUnordered(vcursor VCursor, queryBatchConstruct *queryinfo.QueryBatchConstruct, joinvars map[string]interface{}, wantfields bool) ([]sqltypes.QueryResponse, error) {
	// AsTransaction flag is discarded.
	queryCount := len(queryBatchConstruct.BoundQueryList)
	queryResponses := make([]sqltypes.QueryResponse, queryCount)
	ch := make(chan response, queryCount)
	for index, plan := range batchRoute.PlanList {
		go batchRoute.executeParallel(vcursor, queryBatchConstruct, joinvars, wantfields, index, plan, ch)
	}
	count := 0
	for {
		chResponse := <-ch
		queryResponses[chResponse.queryIndex] = chResponse.queryResponse
		count++
		if count == queryCount {
			close(ch)
			break
		}
	}
	return queryResponses, nil
}

func (batchRoute *BatchRoute) executeParallel(vcursor VCursor, queryBatchConstruct *queryinfo.QueryBatchConstruct, joinvars map[string]interface{}, wantfields bool, index int, plan *Plan, ch chan response) {
	queryConstruct := queryinfo.NewQueryConstruct(queryBatchConstruct.BoundQueryList[index].SQL, queryBatchConstruct.Keyspace, queryBatchConstruct.BoundQueryList[index].BindVars, false)
	result, err := plan.Instructions.Execute(vcursor, queryConstruct, joinvars, wantfields)
	ch <- response{
		queryIndex:    index,
		queryResponse: sqltypes.QueryResponse{QueryResult: result, QueryError: err},
	}
}

type response struct {
	queryResponse sqltypes.QueryResponse
	queryIndex    int
}

func (batchRoute *BatchRoute) executeOrdered(vcursor VCursor, queryBatchConstruct *queryinfo.QueryBatchConstruct, joinvars map[string]interface{}, wantfields bool) ([]sqltypes.QueryResponse, error) {
	// AsTransaction flag is discarded.
	queryResponses := make([]sqltypes.QueryResponse, len(queryBatchConstruct.BoundQueryList))
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
