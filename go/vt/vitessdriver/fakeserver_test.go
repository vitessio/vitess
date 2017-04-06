package vitessdriver

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
	Keyspace         string
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	execCase, ok := execMap[sql]
	if !ok {
		return session, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:              sql,
		BindVariables:    bindVariables,
		Keyspace:         keyspace,
		TabletType:       tabletType,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		return session, nil, fmt.Errorf("Execute request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.session != nil {
		*session = *execCase.session
	}
	return session, execCase.result, nil
}

// ExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

// ExecuteBatch is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatch(ctx context.Context, sql []string, bindVariables []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sql) == 1 {
		execCase, ok := execMap[sql[0]]
		if !ok {
			return session, nil, fmt.Errorf("no match for: %s", sql)
		}
		if bindVariables == nil {
			bindVariables = make([]map[string]interface{}, 1)
		}
		query := &queryExecute{
			SQL:           sql[0],
			BindVariables: bindVariables[0],
			Keyspace:      keyspace,
			TabletType:    tabletType,
			Session:       session,
		}
		if !reflect.DeepEqual(query, execCase.execQuery) {
			return session, nil, fmt.Errorf("Execute request mismatch: got %+v, want %+v", query, execCase.execQuery)
		}
		if execCase.session != nil {
			*session = *execCase.session
		}
		return session, []sqltypes.QueryResponse{
			{QueryResult: execCase.result},
		}, nil
	}
	return session, nil, nil
}

// ExecuteBatchShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, nil
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		Keyspace:      keyspace,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		return fmt.Errorf("request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := callback(result); err != nil {
			return err
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := callback(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// StreamExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	return session1, nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
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

// ResolveTransaction is part of the VTGateService interface
func (f *fakeVTGateService) ResolveTransaction(ctx context.Context, dtid string) error {
	if dtid != dtid2 {
		return errors.New("ResolveTransaction: dtid mismatch")
	}
	return nil
}

func (f *fakeVTGateService) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	return nil
}

func (f *fakeVTGateService) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	return 0, nil
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return nil, nil
}

// GetSrvKeyspace is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return &topodatapb.SrvKeyspace{}, nil
}

// UpdateStream is part of the VTGateService interface
func (f *fakeVTGateService) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	return nil
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
	"request": {
		execQuery: &queryExecute{
			SQL: "request",
			BindVariables: map[string]interface{}{
				"v1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
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
				"v1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			TabletType: topodatapb.TabletType_MASTER,
			Session:    session1,
		},
		result:  &sqltypes.Result{},
		session: session2,
	},
	"requestKeyspace": {
		execQuery: &queryExecute{
			SQL: "requestKeyspace",
			BindVariables: map[string]interface{}{
				"v1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:   "ks",
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		result:  &result1,
		session: nil,
	},
	"txRequestKeyspace": {
		execQuery: &queryExecute{
			SQL: "txRequestKeyspace",
			BindVariables: map[string]interface{}{
				"v1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:   "ks",
			TabletType: topodatapb.TabletType_MASTER,
			Session:    session1,
		},
		result:  &sqltypes.Result{},
		session: session2,
	},
}

var result1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int16,
		},
		{
			Name: "field2",
			Type: sqltypes.VarChar,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("value1")),
		},
		{
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
		{
			Target: &querypb.Target{
				Keyspace:   "ks",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		},
	},
}

var dtid2 = "aa"
