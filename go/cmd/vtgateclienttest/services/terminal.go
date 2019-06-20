/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var errTerminal = errors.New("vtgate test client, errTerminal")

// terminalClient implements vtgateservice.VTGateService.
// It is the last client in the chain, and returns a well known error.
type terminalClient struct{}

func newTerminalClient() *terminalClient {
	return &terminalClient{}
}

func (c *terminalClient) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	if sql == "quit://" {
		log.Fatal("Received quit:// query. Going down.")
	}
	return session, nil, errTerminal
}

func (c *terminalClient) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) == 1 {
		if sqlList[0] == "quit://" {
			log.Fatal("Received quit:// query. Going down.")
		}
	}
	return session, nil, errTerminal
}

func (c *terminalClient) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return nil, errTerminal
}

func (c *terminalClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
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

func (c *terminalClient) MessageAckKeyspaceIds(ctx context.Context, keyspace string, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error) {
	return 0, errTerminal
}

func (c *terminalClient) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
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

func (c *terminalClient) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return errTerminal
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
