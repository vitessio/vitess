package client

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
}

// queryExecute contains all the fields we use to test Execute
type queryExecute struct {
	SQL              string
	BindVariables    map[string]interface{}
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	execCase, ok := execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:              sql,
		BindVariables:    bindVariables,
		TabletType:       tabletType,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		return nil, fmt.Errorf("request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.session != nil {
		*session = *execCase.session
	}
	return execCase.result, nil
}

// ExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteBatchShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	return nil, nil
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	return nil, nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		return fmt.Errorf("request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := sendReply(result); err != nil {
			return err
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := sendReply(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// StreamExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context) (*vtgatepb.Session, error) {
	return session1, nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, session *vtgatepb.Session) error {
	if !reflect.DeepEqual(session, session2) {
		return errors.New("commit: session mismatch")
	}
	return nil
}

// Rollback is part of the VTGateService interface
func (f *fakeVTGateService) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	if !reflect.DeepEqual(session, session2) {
		return errors.New("rollback: session mismatch")
	}
	return nil
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return nil, nil
}

// GetSrvKeyspace is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return &topodatapb.SrvKeyspace{}, nil
}

// GetSrvShard is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvShard(ctx context.Context, keyspace, shard string) (*topodatapb.SrvShard, error) {
	return &topodatapb.SrvShard{}, nil
}

// HandlePanic is part of the VTGateService interface
func (f *fakeVTGateService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer() vtgateservice.VTGateService {
	return &fakeVTGateService{}
}

var execMap = map[string]struct {
	execQuery *queryExecute
	result    *sqltypes.Result
	session   *vtgatepb.Session
	err       error
}{
	"request1": {
		execQuery: &queryExecute{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"v1": int64(0),
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		result:  &result1,
		session: nil,
	},
	"txRequest": {
		execQuery: &queryExecute{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"v1": int64(0),
			},
			TabletType: topodatapb.TabletType_MASTER,
			Session:    session1,
		},
		result:  &sqltypes.Result{},
		session: session2,
	},
}

var result1 = sqltypes.Result{
	Fields: []*querypb.Field{
		&querypb.Field{
			Name: "field1",
			Type: sqltypes.Int16,
		},
		&querypb.Field{
			Name: "field2",
			Type: sqltypes.VarChar,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("value1")),
		},
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("2")),
			sqltypes.MakeString([]byte("value2")),
		},
	},
}

var session1 = &vtgatepb.Session{
	InTransaction: true,
}

var session2 = &vtgatepb.Session{
	InTransaction: true,
	ShardSessions: []*vtgatepb.Session_ShardSession{
		&vtgatepb.Session_ShardSession{
			Target: &querypb.Target{
				Keyspace:   "ks",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		},
	},
}
