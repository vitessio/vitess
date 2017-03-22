package tabletconntest

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// FakeQueryService implements a programmable fake for the query service
// server side.
type FakeQueryService struct {
	t              *testing.T
	TestingGateway bool

	// these fields are used to simulate and synchronize on errors
	HasError      bool
	HasBeginError bool
	TabletError   error
	ErrorWait     chan struct{}

	// these fields are used to simulate and synchronize on panics
	Panics                   bool
	StreamExecutePanicsEarly bool
	UpdateStreamPanicsEarly  bool
	PanicWait                chan struct{}

	// ExpectedTransactionID is what transactionID to expect for Execute
	ExpectedTransactionID int64

	// StreamHealthResponse is what we return for StreamHealth.
	// If not set, return TestStreamHealthStreamHealthResponse
	StreamHealthResponse *querypb.StreamHealthResponse
}

// Close is a no-op.
func (f *FakeQueryService) Close(ctx context.Context) error {
	return nil
}

// HandlePanic is part of the queryservice.QueryService interface
func (f *FakeQueryService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("caught test panic: %v", x)
	}
}

// TestTarget is the target we use for this test
var TestTarget = &querypb.Target{
	Keyspace:   "test_keyspace",
	Shard:      "test_shard",
	TabletType: topodatapb.TabletType_REPLICA,
}

var TestCallerID = &vtrpcpb.CallerID{
	Principal:    "test_principal",
	Component:    "test_component",
	Subcomponent: "test_subcomponent",
}

var TestVTGateCallerID = &querypb.VTGateCallerID{
	Username: "test_username",
}

var TestExecuteOptions = &querypb.ExecuteOptions{
	IncludedFields:    querypb.ExecuteOptions_TYPE_ONLY,
	IncludeEventToken: true,
	CompareEventToken: &querypb.EventToken{
		Timestamp: 9876,
		Shard:     "ssss",
		Position:  "pppp",
	},
}

const TestAsTransaction bool = true

func (f *FakeQueryService) checkTargetCallerID(ctx context.Context, name string, target *querypb.Target) {
	if !reflect.DeepEqual(target, TestTarget) {
		f.t.Errorf("invalid Target for %v: got %#v expected %#v", name, target, TestTarget)
	}

	ef := callerid.EffectiveCallerIDFromContext(ctx)
	if ef == nil {
		f.t.Errorf("no effective caller id for %v", name)
	} else {
		if !reflect.DeepEqual(ef, TestCallerID) {
			f.t.Errorf("invalid effective caller id for %v: got %v expected %v", name, ef, TestCallerID)
		}
	}

	im := callerid.ImmediateCallerIDFromContext(ctx)
	if im == nil {
		f.t.Errorf("no immediate caller id for %v", name)
	} else {
		if !reflect.DeepEqual(im, TestVTGateCallerID) {
			f.t.Errorf("invalid immediate caller id for %v: got %v expected %v", name, im, TestVTGateCallerID)
		}
	}
}

const BeginTransactionID int64 = 9990

// Begin is part of the queryservice.QueryService interface
func (f *FakeQueryService) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	if f.HasBeginError {
		return 0, f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Begin", target)
	return BeginTransactionID, nil
}

const CommitTransactionID int64 = 999044

// Commit is part of the queryservice.QueryService interface
func (f *FakeQueryService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Commit", target)
	if transactionID != CommitTransactionID {
		f.t.Errorf("Commit: invalid TransactionId: got %v expected %v", transactionID, CommitTransactionID)
	}
	return nil
}

const RollbackTransactionID int64 = 999044

// Rollback is part of the queryservice.QueryService interface
func (f *FakeQueryService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Rollback", target)
	if transactionID != RollbackTransactionID {
		f.t.Errorf("Rollback: invalid TransactionId: got %v expected %v", transactionID, RollbackTransactionID)
	}
	return nil
}

const Dtid string = "aa"

// Prepare is part of the queryservice.QueryService interface
func (f *FakeQueryService) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Prepare", target)
	if transactionID != CommitTransactionID {
		f.t.Errorf("Prepare: invalid TransactionID: got %v expected %v", transactionID, CommitTransactionID)
	}
	if dtid != Dtid {
		f.t.Errorf("Prepare: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return nil
}

// CommitPrepared is part of the queryservice.QueryService interface
func (f *FakeQueryService) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "CommitPrepared", target)
	if dtid != Dtid {
		f.t.Errorf("CommitPrepared: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return nil
}

// RollbackPrepared is part of the queryservice.QueryService interface
func (f *FakeQueryService) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "RollbackPrepared", target)
	if originalID != RollbackTransactionID {
		f.t.Errorf("RollbackPrepared: invalid TransactionID: got %v expected %v", originalID, RollbackTransactionID)
	}
	if dtid != Dtid {
		f.t.Errorf("RollbackPrepared: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return nil
}

var Participants = []*querypb.Target{{
	Keyspace: "ks0",
	Shard:    "0",
}, {
	Keyspace: "ks1",
	Shard:    "1",
}}

// CreateTransaction is part of the queryservice.QueryService interface
func (f *FakeQueryService) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "CreateTransaction", target)
	if dtid != Dtid {
		f.t.Errorf("CreateTransaction: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	if !reflect.DeepEqual(participants, Participants) {
		f.t.Errorf("invalid CreateTransaction participants: got %v, expected %v", participants, Participants)
	}
	return nil
}

// StartCommit is part of the queryservice.QueryService interface
func (f *FakeQueryService) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "StartCommit", target)
	if transactionID != CommitTransactionID {
		f.t.Errorf("StartCommit: invalid TransactionID: got %v expected %v", transactionID, CommitTransactionID)
	}
	if dtid != Dtid {
		f.t.Errorf("StartCommit: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return nil
}

// SetRollback is part of the queryservice.QueryService interface
func (f *FakeQueryService) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "SetRollback", target)
	if transactionID != CommitTransactionID {
		f.t.Errorf("SetRollback: invalid TransactionID: got %v expected %v", transactionID, CommitTransactionID)
	}
	if dtid != Dtid {
		f.t.Errorf("SetRollback: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return nil
}

// ConcludeTransaction is part of the queryservice.QueryService interface
func (f *FakeQueryService) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "ConcludeTransaction", target)
	if dtid != Dtid {
		f.t.Errorf("ConcludeTransaction: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return nil
}

var Metadata = &querypb.TransactionMetadata{
	Dtid:         "aa",
	State:        querypb.TransactionState_PREPARE,
	TimeCreated:  1,
	Participants: Participants,
}

// ReadTransaction is part of the queryservice.QueryService interface
func (f *FakeQueryService) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	if f.HasError {
		return nil, f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "ReadTransaction", target)
	if dtid != Dtid {
		f.t.Errorf("ReadTransaction: invalid dtid: got %s expected %s", dtid, Dtid)
	}
	return Metadata, nil
}

const ExecuteQuery = "executeQuery"

var ExecuteBindVars = map[string]interface{}{
	"bind1": &querypb.BindVariable{
		Type:  querypb.Type_INT64,
		Value: []byte("1114444"),
	},
}

const ExecuteTransactionID int64 = 678

var ExecuteQueryResult = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int8,
		},
		{
			Name: "field2",
			Type: sqltypes.Char,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("1")),
			sqltypes.NULL,
		},
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.Char, []byte("row2 value2")),
		},
	},
	Extras: &querypb.ResultExtras{
		EventToken: &querypb.EventToken{
			Timestamp: 456321,
			Shard:     "test_shard",
			Position:  "test_position",
		},
		Fresher: true,
	},
}

// Execute is part of the queryservice.QueryService interface
func (f *FakeQueryService) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if f.HasError {
		return nil, f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if sql != ExecuteQuery {
		f.t.Errorf("invalid Execute.Query.Sql: got %v expected %v", sql, ExecuteQuery)
	}
	if !reflect.DeepEqual(bindVariables, ExecuteBindVars) {
		f.t.Errorf("invalid Execute.BindVariables: got %v expected %v", bindVariables, ExecuteBindVars)
	}
	if !proto.Equal(options, TestExecuteOptions) {
		f.t.Errorf("invalid Execute.ExecuteOptions: got %v expected %v", options, TestExecuteOptions)
	}
	f.checkTargetCallerID(ctx, "Execute", target)
	if transactionID != f.ExpectedTransactionID {
		f.t.Errorf("invalid Execute.TransactionId: got %v expected %v", transactionID, f.ExpectedTransactionID)
	}
	return &ExecuteQueryResult, nil
}

const StreamExecuteQuery = "streamExecuteQuery"

var StreamExecuteBindVars = map[string]interface{}{
	"bind1": &querypb.BindVariable{
		Type:  querypb.Type_INT64,
		Value: []byte("93848000"),
	},
}

var StreamExecuteQueryResult1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int8,
		},
		{
			Name: "field2",
			Type: sqltypes.Char,
		},
	},
}

var StreamExecuteQueryResult2 = sqltypes.Result{
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Char, []byte("row1 value2")),
		},
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.Char, []byte("row2 value2")),
		},
	},
}

// StreamExecute is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if f.Panics && f.StreamExecutePanicsEarly {
		panic(fmt.Errorf("test-triggered panic early"))
	}
	if sql != StreamExecuteQuery {
		f.t.Errorf("invalid StreamExecute.Sql: got %v expected %v", sql, StreamExecuteQuery)
	}
	if !reflect.DeepEqual(bindVariables, StreamExecuteBindVars) {
		f.t.Errorf("invalid StreamExecute.BindVariables: got %v expected %v", bindVariables, StreamExecuteBindVars)
	}
	if !proto.Equal(options, TestExecuteOptions) {
		f.t.Errorf("invalid StreamExecute.ExecuteOptions: got %v expected %v", options, TestExecuteOptions)
	}
	f.checkTargetCallerID(ctx, "StreamExecute", target)
	if err := callback(&StreamExecuteQueryResult1); err != nil {
		f.t.Errorf("callback1 failed: %v", err)
	}
	if f.Panics && !f.StreamExecutePanicsEarly {
		// wait until the client gets the response, then panics
		<-f.PanicWait
		panic(fmt.Errorf("test-triggered panic late"))
	}
	if f.HasError {
		// wait until the client has the response, since all
		// streaming implementation may not send previous
		// messages if an error has been triggered.
		<-f.ErrorWait
		return f.TabletError
	}
	if err := callback(&StreamExecuteQueryResult2); err != nil {
		f.t.Errorf("callback2 failed: %v", err)
	}
	return nil
}

var ExecuteBatchQueries = []querytypes.BoundQuery{
	{
		Sql: "executeBatchQueries1",
		BindVariables: map[string]interface{}{
			"bind1": &querypb.BindVariable{
				Type:  querypb.Type_INT64,
				Value: []byte("43"),
			},
		},
	},
	{
		Sql: "executeBatchQueries2",
		BindVariables: map[string]interface{}{
			"bind2": &querypb.BindVariable{
				Type:  querypb.Type_INT64,
				Value: []byte("72"),
			},
		},
	},
}

const ExecuteBatchTransactionID int64 = 678

var ExecuteBatchQueryResultList = []sqltypes.Result{
	{
		Fields: []*querypb.Field{
			{
				Name: "field1",
				Type: sqltypes.Int8,
			},
		},
		RowsAffected: 1232,
		InsertID:     712,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int8, []byte("1")),
			},
			{
				sqltypes.MakeTrusted(sqltypes.Int8, []byte("2")),
			},
		},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 456322,
				Shard:     "test_shard2",
				Position:  "test_position2",
			},
			Fresher: true,
		},
	},
	{
		Fields: []*querypb.Field{
			{
				Name: "field1",
				Type: sqltypes.VarBinary,
			},
		},
		RowsAffected: 12333,
		InsertID:     74442,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("row1 value1")),
			},
			{
				sqltypes.MakeString([]byte("row1 value2")),
			},
		},
	},
}

// ExecuteBatch is part of the queryservice.QueryService interface
func (f *FakeQueryService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if f.HasError {
		return nil, f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if !reflect.DeepEqual(queries, ExecuteBatchQueries) {
		f.t.Errorf("invalid ExecuteBatch.Queries: got %v expected %v", queries, ExecuteBatchQueries)
	}
	if !proto.Equal(options, TestExecuteOptions) {
		f.t.Errorf("invalid ExecuteBatch.ExecuteOptions: got %v expected %v", options, TestExecuteOptions)
	}
	f.checkTargetCallerID(ctx, "ExecuteBatch", target)
	if asTransaction != TestAsTransaction {
		f.t.Errorf("invalid ExecuteBatch.AsTransaction: got %v expected %v", asTransaction, TestAsTransaction)
	}
	if transactionID != f.ExpectedTransactionID {
		f.t.Errorf("invalid ExecuteBatch.TransactionId: got %v expected %v", transactionID, f.ExpectedTransactionID)
	}
	return ExecuteBatchQueryResultList, nil
}

var SplitQuerySplitColumns = []string{"nice_column_to_split"}

const SplitQuerySplitCount = 372

var SplitQueryBoundQuery = querytypes.BoundQuery{
	Sql: "splitQuery",
	BindVariables: map[string]interface{}{
		"bind1": &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: []byte("43"),
		},
	},
}

const SplitQueryNumRowsPerQueryPart = 123
const SplitQueryAlgorithm = querypb.SplitQueryRequest_FULL_SCAN

var SplitQueryQuerySplitList = []querytypes.QuerySplit{
	{
		Sql: "splitQuery",
		BindVariables: map[string]interface{}{
			"bind1": &querypb.BindVariable{
				Type:  querypb.Type_INT64,
				Value: []byte("43"),
			},
			"keyspace_id": &querypb.BindVariable{
				Type:  querypb.Type_INT64,
				Value: []byte("3333"),
			},
		},
		RowCount: 4456,
	},
}

// BeginExecute combines Begin and Execute.
func (f *FakeQueryService) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	transactionID, err := f.Begin(ctx, target)
	if err != nil {
		return nil, 0, err
	}

	result, err := f.Execute(ctx, target, sql, bindVariables, transactionID, options)
	return result, transactionID, err
}

// BeginExecuteBatch combines Begin and ExecuteBatch.
func (f *FakeQueryService) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	transactionID, err := f.Begin(ctx, target)
	if err != nil {
		return nil, 0, err
	}

	results, err := f.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
	return results, transactionID, err
}

var (
	MessageName         = "vitess_message"
	MessageStreamResult = &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: sqltypes.VarBinary,
		}, {
			Name: "message",
			Type: sqltypes.VarBinary,
		}},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("row1 value2")),
		}, {
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("row2 value2")),
		}},
	}
	MessageIDs = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
)

// MessageStream is part of the queryservice.QueryService interface
func (f *FakeQueryService) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) (err error) {
	if f.HasError {
		return f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if name != MessageName {
		f.t.Errorf("name: %s, want %s", name, MessageName)
	}
	callback(MessageStreamResult)
	return nil
}

// MessageAck is part of the queryservice.QueryService interface
func (f *FakeQueryService) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	if f.HasError {
		return 0, f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if name != MessageName {
		f.t.Errorf("name: %s, want %s", name, MessageName)
	}
	if !reflect.DeepEqual(ids, MessageIDs) {
		f.t.Errorf("ids: %v, want %v", ids, MessageIDs)
	}
	return 1, nil
}

// SplitQuery is part of the queryservice.QueryService interface
func (f *FakeQueryService) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]querytypes.QuerySplit, error) {

	if f.HasError {
		return nil, f.TabletError
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "SplitQuery", target)
	if !reflect.DeepEqual(query, SplitQueryBoundQuery) {
		f.t.Errorf("invalid SplitQuery.SplitQueryRequest.Query: got %v expected %v",
			querytypes.QueryAsString(query.Sql, query.BindVariables), SplitQueryBoundQuery)
	}
	if !reflect.DeepEqual(splitColumns, SplitQuerySplitColumns) {
		f.t.Errorf("invalid SplitQuery.SplitColumn: got %v expected %v",
			splitColumns, SplitQuerySplitColumns)
	}
	if splitCount != SplitQuerySplitCount {
		f.t.Errorf("invalid SplitQuery.SplitCount: got %v expected %v",
			splitCount, SplitQuerySplitCount)
	}
	if numRowsPerQueryPart != SplitQueryNumRowsPerQueryPart {
		f.t.Errorf("invalid SplitQuery.numRowsPerQueryPart: got %v expected %v",
			numRowsPerQueryPart, SplitQueryNumRowsPerQueryPart)
	}
	if algorithm != SplitQueryAlgorithm {
		f.t.Errorf("invalid SplitQuery.algorithm: got %v expected %v",
			algorithm, SplitQueryAlgorithm)
	}
	return SplitQueryQuerySplitList, nil
}

var TestStreamHealthStreamHealthResponse = &querypb.StreamHealthResponse{
	Target: &querypb.Target{
		Keyspace:   "test_keyspace",
		Shard:      "test_shard",
		TabletType: topodatapb.TabletType_RDONLY,
	},
	Serving: true,
	TabletExternallyReparentedTimestamp: 1234589,
	RealtimeStats: &querypb.RealtimeStats{
		HealthError:                            "random error",
		SecondsBehindMaster:                    234,
		BinlogPlayersCount:                     1,
		SecondsBehindMasterFilteredReplication: 2,
		CpuUsage: 1.0,
	},
}
var TestStreamHealthErrorMsg = "to trigger a server error"

// StreamHealth is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	if f.HasError {
		return errors.New(TestStreamHealthErrorMsg)
	}
	if f.Panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	shr := f.StreamHealthResponse
	if shr == nil {
		shr = TestStreamHealthStreamHealthResponse
	}
	callback(shr)
	return nil
}

const UpdateStreamPosition = "update stream position"

const UpdateStreamTimestamp = 123654

var UpdateStreamStreamEvent1 = querypb.StreamEvent{
	Statements: []*querypb.StreamEvent_Statement{
		{
			Category:  querypb.StreamEvent_Statement_DML,
			TableName: "table1",
		},
	},
	EventToken: &querypb.EventToken{
		Timestamp: 789654,
		Shard:     "shard1",
		Position:  "streaming position 1",
	},
}

var UpdateStreamStreamEvent2 = querypb.StreamEvent{
	Statements: []*querypb.StreamEvent_Statement{
		{
			Category:  querypb.StreamEvent_Statement_DML,
			TableName: "table2",
		},
	},
	EventToken: &querypb.EventToken{
		Timestamp: 789655,
		Shard:     "shard1",
		Position:  "streaming position 2",
	},
}

// UpdateStream is part of the queryservice.QueryService interface
func (f *FakeQueryService) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	if f.Panics && f.UpdateStreamPanicsEarly {
		panic(fmt.Errorf("test-triggered panic early"))
	}
	if position != UpdateStreamPosition {
		f.t.Errorf("invalid UpdateStream.position: got %v expected %v", position, UpdateStreamPosition)
	}
	if timestamp != UpdateStreamTimestamp {
		f.t.Errorf("invalid UpdateStream.timestamp: got %v expected %v", timestamp, UpdateStreamTimestamp)
	}
	f.checkTargetCallerID(ctx, "UpdateStream", target)
	if err := callback(&UpdateStreamStreamEvent1); err != nil {
		f.t.Errorf("callback1 failed: %v", err)
	}
	if f.Panics && !f.UpdateStreamPanicsEarly {
		// wait until the client gets the response, then panics
		<-f.PanicWait
		panic(fmt.Errorf("test-triggered panic late"))
	}
	if f.HasError {
		// wait until the client has the response, since all
		// streaming implementation may not send previous
		// messages if an error has been triggered.
		<-f.ErrorWait
		return f.TabletError
	}
	if err := callback(&UpdateStreamStreamEvent2); err != nil {
		f.t.Errorf("callback2 failed: %v", err)
	}
	return nil
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) *FakeQueryService {
	return &FakeQueryService{
		t: t,
	}
}
