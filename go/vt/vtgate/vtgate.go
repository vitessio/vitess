// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var RpcVTGate *VTGate

// VTGate is the rpc interface to vtgate. Only one instance
// can be created.
type VTGate struct {
	scatterConn *ScatterConn
}

// registration mechanism
type RegisterVTGate func(*VTGate)

var RegisterVTGates []RegisterVTGate

func Init(serv SrvTopoServer, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration) {
	if RpcVTGate != nil {
		log.Fatalf("VTGate already initialized")
	}
	RpcVTGate = &VTGate{
		scatterConn: NewScatterConn(serv, cell, retryDelay, retryCount, timeout),
	}
	for _, f := range RegisterVTGates {
		f(RpcVTGate)
	}
}

// ExecuteShard executes a non-streaming query on the specified shards.
func (vtg *VTGate) ExecuteShard(context interface{}, query *proto.QueryShard, reply *proto.QueryResult) error {
	qr, err := vtg.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.Shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteShard: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyspaceIds executes a non-streaming query based on the specified keyspace ids.
func (vtg *VTGate) ExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	shards, err := mapKeyspaceIdsToShards(
		vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		query.Keyspace,
		query.TabletType,
		query.KeyspaceIds)
	if err != nil {
		return err
	}
	qr, err := vtg.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteKeyRange: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteKeyRanges executes a non-streaming query based on the specified keyranges.
func (vtg *VTGate) ExecuteKeyRanges(context interface{}, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	shards, err := mapKeyRangesToShards(
		vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		query.Keyspace,
		query.TabletType,
		query.KeyRanges)
	if err != nil {
		return err
	}
	qr, err := vtg.scatterConn.Execute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteKeyRange: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// ExecuteEntityIds excutes a non-streaming query based on given KeyspaceId map.
func (vtg *VTGate) ExecuteEntityIds(context interface{}, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	shardMap, err := mapEntityIdsToShards(
		vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		query.Keyspace,
		query.EntityKeyspaceIdMap,
		query.TabletType)
	if err != nil {
		return err
	}
	shards, sqls, bindVars := buildEntityIds(shardMap, query.Sql, query.EntityColumnName, query.BindVariables)
	qr, err := vtg.scatterConn.ExecuteEntityIds(
		context,
		shards,
		sqls,
		bindVars,
		query.Keyspace,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		proto.PopulateQueryResult(qr, reply)
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteEntityIds: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

func buildEntityIds(shardMap map[string][]key.KeyspaceId, qSql, entityColName string, qBindVars map[string]interface{}) ([]string, map[string]string, map[string]map[string]interface{}) {
	shards := make([]string, 0, 1)
	sqls := make(map[string]string)
	bindVars := make(map[string]map[string]interface{})
	for shard, kids := range shardMap {
		var b bytes.Buffer
		b.Write([]byte(entityColName))
		b.Write([]byte(" in ("))
		bindVar := make(map[string]interface{})
		for k, v := range qBindVars {
			bindVar[k] = v
		}
		for i, kid := range kids {
			bvName := fmt.Sprintf("%v%v", entityColName, i)
			bindVar[bvName] = kid
			if i > 0 {
				b.Write([]byte(", "))
			}
			b.Write([]byte(fmt.Sprintf(":%v", bvName)))
		}
		b.Write([]byte(")"))
		shards = append(shards, shard)
		sqls[shard] = insertSqlClause(qSql, b.String())
		bindVars[shard] = bindVar
	}
	return shards, sqls, bindVars
}

func insertSqlClause(querySql, clause string) string {
	// get first index of any additional clause: group by, order by, limit, for update, sql end if nothing
	// insert clause into the index position
	sql := strings.ToLower(querySql)
	idxExtra := len(sql)
	if idxGroupBy := strings.Index(sql, " group by"); idxGroupBy > 0 && idxGroupBy < idxExtra {
		idxExtra = idxGroupBy
	}
	if idxOrderBy := strings.Index(sql, " order by"); idxOrderBy > 0 && idxOrderBy < idxExtra {
		idxExtra = idxOrderBy
	}
	if idxLimit := strings.Index(sql, " limit"); idxLimit > 0 && idxLimit < idxExtra {
		idxExtra = idxLimit
	}
	if idxForUpdate := strings.Index(sql, " for update"); idxForUpdate > 0 && idxForUpdate < idxExtra {
		idxExtra = idxForUpdate
	}
	var b bytes.Buffer
	b.Write([]byte(querySql[:idxExtra]))
	if strings.Contains(sql, "where") {
		b.Write([]byte(" and "))
	} else {
		b.Write([]byte(" where "))
	}
	b.Write([]byte(clause))
	if idxExtra < len(sql) {
		b.Write([]byte(querySql[idxExtra:]))
	}
	return b.String()
}

// ExecuteBatchShard executes a group of queries on the specified shards.
func (vtg *VTGate) ExecuteBatchShard(context interface{}, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	qrs, err := vtg.scatterConn.ExecuteBatch(
		context,
		batchQuery.Queries,
		batchQuery.Keyspace,
		batchQuery.Shards,
		batchQuery.TabletType,
		NewSafeSession(batchQuery.Session))
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteBatchShard: %v, queries: %+v", err, batchQuery)
	}
	reply.Session = batchQuery.Session
	return nil
}

// ExecuteBatchKeyspaceIds executes a group of queries based on the specified keyspace ids.
func (vtg *VTGate) ExecuteBatchKeyspaceIds(context interface{}, query *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	shards, err := mapKeyspaceIdsToShards(
		vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		query.Keyspace,
		query.TabletType,
		query.KeyspaceIds)
	if err != nil {
		return err
	}
	qrs, err := vtg.scatterConn.ExecuteBatch(
		context,
		query.Queries,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session))
	if err == nil {
		reply.List = qrs.List
	} else {
		reply.Error = err.Error()
		log.Errorf("ExecuteBatchKeyspaceIds: %v, query: %+v", err, query)
	}
	reply.Session = query.Session
	return nil
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing. The api supports supplying multiple KeyspaceIds
// to make it future proof.
func (vtg *VTGate) StreamExecuteKeyspaceIds(context interface{}, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	shards, err := mapKeyspaceIdsToShards(
		vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		query.Keyspace,
		query.TabletType,
		query.KeyspaceIds)
	if err != nil {
		return err
	}
	if len(shards) != 1 {
		return fmt.Errorf("KeyspaceIds cannot map to more than one shard")
	}
	err = vtg.scatterConn.StreamExecute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session),
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})
	if err != nil {
		log.Errorf("StreamExecuteKeyspaceIds: %v, query: %+v", err, query)
	}
	// now we can send the final Sessoin info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing. The api supports supplying multiple keyranges
// to make it future proof.
func (vtg *VTGate) StreamExecuteKeyRanges(context interface{}, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	shards, err := mapKeyRangesToShards(
		vtg.scatterConn.toposerv,
		vtg.scatterConn.cell,
		query.Keyspace,
		query.TabletType,
		query.KeyRanges)
	if err != nil {
		return err
	}
	if len(shards) != 1 {
		return fmt.Errorf("KeyRanges cannot map to more than one shard")
	}

	err = vtg.scatterConn.StreamExecute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		shards,
		query.TabletType,
		NewSafeSession(query.Session),
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses are sent.
			return sendReply(reply)
		})

	if err != nil {
		log.Errorf("StreamExecuteKeyRange: %v, query: %+v", err, query)
	}
	// now we can send the final Session info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// StreamExecuteShard executes a streaming query on the specified shards.
func (vtg *VTGate) StreamExecuteShard(context interface{}, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	err := vtg.scatterConn.StreamExecute(
		context,
		query.Sql,
		query.BindVariables,
		query.Keyspace,
		query.Shards,
		query.TabletType,
		NewSafeSession(query.Session),
		func(mreply *mproto.QueryResult) error {
			reply := new(proto.QueryResult)
			proto.PopulateQueryResult(mreply, reply)
			// Note we don't populate reply.Session here,
			// as it may change incrementaly as responses
			// are sent.
			return sendReply(reply)
		})

	if err != nil {
		log.Errorf("StreamExecuteShard: %v, query: %+v", err, query)
	}
	// now we can send the final Session info.
	if query.Session != nil {
		sendReply(&proto.QueryResult{Session: query.Session})
	}
	return err
}

// Begin begins a transaction. It has to be concluded by a Commit or Rollback.
func (vtg *VTGate) Begin(context interface{}, outSession *proto.Session) error {
	outSession.InTransaction = true
	return nil
}

// Commit commits a transaction.
func (vtg *VTGate) Commit(context interface{}, inSession *proto.Session) error {
	return vtg.scatterConn.Commit(context, NewSafeSession(inSession))
}

// Rollback rolls back a transaction.
func (vtg *VTGate) Rollback(context interface{}, inSession *proto.Session) error {
	return vtg.scatterConn.Rollback(context, NewSafeSession(inSession))
}
