// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"errors"
	"fmt"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var errTerminal = errors.New("vtgate test client, errTerminal")

// terminalClient implements vtgateservice.VTGateService.
// It is the last client in the chain, and returns a well known error.
type terminalClient struct{}

func newTerminalClient() *terminalClient {
	return &terminalClient{}
}

func (c *terminalClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if sql == "quit://" {
		log.Fatal("Received quit:// query. Going down.")
	}
	return errTerminal
}

func (c *terminalClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return errTerminal
}

func (c *terminalClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return errTerminal
}

func (c *terminalClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return errTerminal
}

func (c *terminalClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	return errTerminal
}

func (c *terminalClient) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	return errTerminal
}

func (c *terminalClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	return errTerminal
}

func (c *terminalClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return errTerminal
}

func (c *terminalClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	return errTerminal
}

func (c *terminalClient) Begin(ctx context.Context, outSession *proto.Session) error {
	return errTerminal
}

func (c *terminalClient) Commit(ctx context.Context, inSession *proto.Session) error {
	return errTerminal
}

func (c *terminalClient) Rollback(ctx context.Context, inSession *proto.Session) error {
	return errTerminal
}

func (c *terminalClient) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	return nil, errTerminal
}

func (c *terminalClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
	return nil, errTerminal
}

func (c *terminalClient) GetSrvShard(ctx context.Context, keyspace, shard string) (*pb.SrvShard, error) {
	return nil, errTerminal
}

func (c *terminalClient) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
