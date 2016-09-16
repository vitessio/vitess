package fakes

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// ErrorQueryService is an implementation of QueryService that returns a
// configurable error for some of its methods.
//
// It is used as base for other, more specialised QueryService fakes e.g.
// StreamHealthQueryService.
type ErrorQueryService struct {
}

// Begin is part of QueryService interface
func (e *ErrorQueryService) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	return 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// Commit is part of QueryService interface
func (e *ErrorQueryService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Rollback is part of QueryService interface
func (e *ErrorQueryService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// Execute is part of QueryService interface
func (e *ErrorQueryService) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamExecute is part of QueryService interface
func (e *ErrorQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// ExecuteBatch is part of QueryService interface
func (e *ErrorQueryService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// BeginExecute is part of QueryService interface
func (e *ErrorQueryService) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	return nil, 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// BeginExecuteBatch is part of QueryService interface
func (e *ErrorQueryService) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	return nil, 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// SplitQuery is part of QueryService interface
// TODO(erez): Remove once the migration to SplitQuery V2 is done.
func (e *ErrorQueryService) SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// SplitQueryV2 is part of QueryService interface
func (e *ErrorQueryService) SplitQueryV2(
	ctx context.Context,
	target *querypb.Target,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]querytypes.QuerySplit, error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamHealthRegister is part of QueryService interface
func (e *ErrorQueryService) StreamHealthRegister(chan<- *querypb.StreamHealthResponse) (int, error) {
	return 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// StreamHealthUnregister is part of QueryService interface
func (e *ErrorQueryService) StreamHealthUnregister(int) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// UpdateStream is part of QueryService interface
func (e *ErrorQueryService) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, sendReply func(*querypb.StreamEvent) error) error {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// HandlePanic is part of QueryService interface
func (e *ErrorQueryService) HandlePanic(*error) {
}

// make sure ErrorQueryService implements QueryService
var _ queryservice.QueryService = &ErrorQueryService{}
