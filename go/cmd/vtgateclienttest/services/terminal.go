// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"errors"
	"fmt"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/tb"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var errTerminal = errors.New("vtgate test client, errTerminal")

// terminalClient implements vtgateservice.VTGateService.
// It is the last client in the chain, and returns a well known error.
type terminalClient struct{}

func newTerminalClient() *terminalClient {
	return &terminalClient{}
}

func (c *terminalClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	if sql == "quit://" {
		log.Fatal("Received quit:// query. Going down.")
	}
	return session, nil, errTerminal
}

func (c *terminalClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) == 1 {
		if sqlList[0] == "quit://" {
			log.Fatal("Received quit:// query. Going down.")
		}
	}
	return session, nil, errTerminal
}

func (c *terminalClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	return nil, errTerminal
}

func (c *terminalClient) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	return errTerminal
}

func (c *terminalClient) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	return errTerminal
}

func (c *terminalClient) ResolveTransaction(ctx context.Context, dtid string) error {
	return errTerminal
}

func (c *terminalClient) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	return 0, errTerminal
}

func (c *terminalClient) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return nil, errTerminal
}

func (c *terminalClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return nil, errTerminal
}

func (c *terminalClient) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	return errTerminal
}

func (c *terminalClient) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
