package engine

import (
	"errors"

	"fmt"

	"sync"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
)

// BatchRoute represents the instructions to route a query list to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type BatchRoute struct {
	Opcode   BatchRouteOpcode
	PlanList []*Plan
}

// BatchRouteOpcode is a number representing the opcode
// for the BatchRoute primitive.
type BatchRouteOpcode int

// This is the list of BatchRouteOpcode values.
// The opcode dictates the execution path.
const (
	BatchNoCode = BatchRouteOpcode(iota)
	// ExecuteOrdered is for executing plans in serial.
	ExecuteOrdered
	// ExecuteUnOrdered is for executing plans in parallel.
	ExecuteUnOrdered
)

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
	switch batchRoute.Opcode {
	case ExecuteOrdered:
		return batchRoute.executeOrdered(vcursor, queryBatchConstruct, joinvars, wantfields)
	case ExecuteUnOrdered:
		return batchRoute.executeUnordered(vcursor, queryBatchConstruct, joinvars, wantfields)
	default:
		return nil, fmt.Errorf("ExecuteBatch:unsupported query route: %v", batchRoute)
	}
}

func (batchRoute *BatchRoute) executeOrdered(vcursor VCursor, queryBatchConstruct *queryinfo.QueryBatchConstruct, joinvars map[string]interface{}, wantfields bool) ([]sqltypes.QueryResponse, error) {
	// AsTransaction flag is discarded.
	queryResponses := make([]sqltypes.QueryResponse, len(queryBatchConstruct.BoundQueryList))
	for i, plan := range batchRoute.PlanList {
		if plan == nil {
			queryResponses[i] = sqltypes.QueryResponse{
				QueryResult: nil,
				QueryError:  errors.New("no plan generated for this query"),
			}
			continue
		}
		queryConstruct := queryinfo.NewQueryConstruct(queryBatchConstruct.BoundQueryList[i].SQL, queryBatchConstruct.Keyspace, queryBatchConstruct.BoundQueryList[i].BindVars, false)
		result, err := plan.Instructions.Execute(vcursor, queryConstruct, joinvars, wantfields)
		queryResponses[i] = sqltypes.QueryResponse{
			QueryResult: result,
			QueryError:  err,
		}
	}
	return queryResponses, nil
}

type response struct {
	queryResponse sqltypes.QueryResponse
	queryIndex    int
}

func (batchRoute *BatchRoute) executeUnordered(vcursor VCursor, queryBatchConstruct *queryinfo.QueryBatchConstruct, joinvars map[string]interface{}, wantfields bool) ([]sqltypes.QueryResponse, error) {
	// AsTransaction flag is discarded.
	var wg sync.WaitGroup

	queryResponses := make([]sqltypes.QueryResponse, len(queryBatchConstruct.BoundQueryList))
	ch := make(chan response)

	for index, plan := range batchRoute.PlanList {
		wg.Add(1)
		go func(index int, plan *Plan) {
			defer wg.Done()
			if plan == nil {
				ch <- response{
					queryIndex: index,
					queryResponse: sqltypes.QueryResponse{
						QueryResult: nil,
						QueryError:  errors.New("no plan generated for this query"),
					},
				}
				return
			}
			queryConstruct := queryinfo.NewQueryConstruct(queryBatchConstruct.BoundQueryList[index].SQL, queryBatchConstruct.Keyspace, queryBatchConstruct.BoundQueryList[index].BindVars, false)
			result, err := plan.Instructions.Execute(vcursor, queryConstruct, joinvars, wantfields)
			ch <- response{
				queryIndex:    index,
				queryResponse: sqltypes.QueryResponse{QueryResult: result, QueryError: err},
			}
		}(index, plan)

	}
	go func() {
		for chResponse := range ch {
			queryResponses[chResponse.queryIndex] = chResponse.queryResponse
		}
	}()
	wg.Wait()
	return queryResponses, nil
}
