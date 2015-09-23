package client

import (
	"errors"
	"fmt"
	"reflect"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &proto.Query{
		Sql:              sql,
		BindVariables:    bindVariables,
		TabletType:       topo.ProtoToTabletType(tabletType),
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		return fmt.Errorf("request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	*reply = *execCase.reply
	return nil
}

// ExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return nil
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return nil
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return nil
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return nil
}

// ExecuteBatchShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	return nil
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	return nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &proto.Query{
		Sql:           sql,
		BindVariables: bindVariables,
		TabletType:    topo.ProtoToTabletType(tabletType),
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		return fmt.Errorf("request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Error != "" {
		return errors.New(execCase.reply.Error)
	}
	return nil
}

// StreamExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context, outSession *proto.Session) error {
	*outSession = *session1
	return nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, inSession *proto.Session) error {
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("commit: session mismatch")
	}
	return nil
}

// Rollback is part of the VTGateService interface
func (f *fakeVTGateService) Rollback(ctx context.Context, inSession *proto.Session) error {
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("rollback: session mismatch")
	}
	return nil
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	return nil, nil
}

// GetSrvKeyspace is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
	return &pb.SrvKeyspace{}, nil
}

// GetSrvShard is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvShard(ctx context.Context, keyspace, shard string) (*pb.SrvShard, error) {
	return &pb.SrvShard{}, nil
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
	execQuery  *proto.Query
	shardQuery *proto.QueryShard
	reply      *proto.QueryResult
	err        error
}{
	"request1": {
		execQuery: &proto.Query{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"v1": int64(0),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		shardQuery: &proto.QueryShard{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:   "ks",
			Shards:     []string{"1", "2"},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		reply: &proto.QueryResult{
			Result:  &result1,
			Session: nil,
			Error:   "",
		},
	},
	"txRequest": {
		execQuery: &proto.Query{
			Sql: "txRequest",
			BindVariables: map[string]interface{}{
				"v1": int64(0),
			},
			TabletType: "master",
			Session:    session1,
		},
		shardQuery: &proto.QueryShard{
			Sql: "txRequest",
			BindVariables: map[string]interface{}{
				"v1": int64(0),
			},
			TabletType: "master",
			Keyspace:   "",
			Shards:     []string{},
			Session:    session1,
		},
		reply: &proto.QueryResult{
			Result:  &mproto.QueryResult{},
			Session: session2,
			Error:   "",
		},
	},
}

var result1 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: mproto.VT_LONG,
		},
		mproto.Field{
			Name: "field2",
			Type: mproto.VT_VAR_STRING,
		},
	},
	RowsAffected: 123,
	InsertId:     72,
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

var session1 = &proto.Session{
	InTransaction: true,
	ShardSessions: []*proto.ShardSession{},
}

var session2 = &proto.Session{
	InTransaction: true,
	ShardSessions: []*proto.ShardSession{
		&proto.ShardSession{
			Keyspace:      "ks",
			Shard:         "1",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		},
	},
}
