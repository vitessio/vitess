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

// Prepare is part of QueryService interface
func (e *ErrorQueryService) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// CommitPrepared is part of QueryService interface
func (e *ErrorQueryService) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// RollbackPrepared is part of QueryService interface
func (e *ErrorQueryService) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// CreateTransaction is part of QueryService interface
func (e *ErrorQueryService) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// StartCommit is part of QueryService interface
func (e *ErrorQueryService) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// SetRollback is part of QueryService interface
func (e *ErrorQueryService) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// ConcludeTransaction is part of QueryService interface
func (e *ErrorQueryService) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// ReadTransaction is part of QueryService interface
func (e *ErrorQueryService) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	return nil, fmt.Errorf("ErrorQueryService does not implement any method")
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

// MessageStream is part of QueryService interface
func (e *ErrorQueryService) MessageStream(ctx context.Context, target *querypb.Target, name string, sendReply func(*sqltypes.Result) error) (err error) {
	return fmt.Errorf("ErrorQueryService does not implement any method")
}

// MessageAck is part of QueryService interface
func (e *ErrorQueryService) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	return 0, fmt.Errorf("ErrorQueryService does not implement any method")
}

// SplitQuery is part of QueryService interface
func (e *ErrorQueryService) SplitQuery(
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
