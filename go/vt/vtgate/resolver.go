// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

var (
	separator        = []byte(", ")
	sqlVarIdentifier = []byte(":")
	openBracket      = []byte(" in (")
	closeBracket     = []byte(")")
	kwAnd            = []byte(" and ")
	kwWhere          = []byte(" where ")
	insert_dml       = "insert"
	update_dml       = "update"
	delete_dml       = "delete"
)

// Resolver is the layer to resolve KeyspaceIds and KeyRanges
// to shards. It will try to re-resolve shards if ScatterConn
// returns retryable error, which may imply horizontal or vertical
// resharding happened.
type Resolver struct {
	scatterConn *ScatterConn
}

// NewResolver creates a new Resolver. All input parameters are passed through
// for creating ScatterConn.
func NewResolver(serv SrvTopoServer, statsName, cell string, retryDelay time.Duration, retryCount int, timeout time.Duration) *Resolver {
	return &Resolver{
		scatterConn: NewScatterConn(serv, statsName, cell, retryDelay, retryCount, timeout),
	}
}

// InitializeConnections pre-initializes VTGate by connecting to vttablets of all keyspace/shard/type.
// It is not necessary to call this function before serving queries,
// but it would reduce connection overhead when serving.
func (res *Resolver) InitializeConnections(ctx context.Context) error {
	return res.scatterConn.InitializeConnections(ctx)
}

// ExecuteKeyspaceIds executes a non-streaming query based on KeyspaceIds.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
// This throws an error if a dml spans multiple keyspace_ids. Resharding depends
// on being able to uniquely route a write.
func (res *Resolver) ExecuteKeyspaceIds(context context.Context, query *proto.KeyspaceIdQuery) (*mproto.QueryResult, error) {
	if isDml(query.Sql) && len(query.KeyspaceIds) > 1 {
		return nil, fmt.Errorf("DML should not span multiple keyspace_ids")
	}
	mapToShards := func(keyspace string) (string, []string, error) {
		return mapKeyspaceIdsToShards(
			res.scatterConn.toposerv,
			res.scatterConn.cell,
			keyspace,
			query.TabletType,
			query.KeyspaceIds)
	}
	return res.Execute(context, query.Sql, query.BindVariables, query.Keyspace, query.TabletType, query.Session, mapToShards)
}

// ExecuteKeyRanges executes a non-streaming query based on KeyRanges.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) ExecuteKeyRanges(context context.Context, query *proto.KeyRangeQuery) (*mproto.QueryResult, error) {
	mapToShards := func(keyspace string) (string, []string, error) {
		return mapKeyRangesToShards(
			res.scatterConn.toposerv,
			res.scatterConn.cell,
			keyspace,
			query.TabletType,
			query.KeyRanges)
	}
	return res.Execute(context, query.Sql, query.BindVariables, query.Keyspace, query.TabletType, query.Session, mapToShards)
}

// Execute executes a non-streaming query based on shards resolved by given func.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) Execute(
	context context.Context,
	sql string,
	bindVars map[string]interface{},
	keyspace string,
	tabletType topo.TabletType,
	session *proto.Session,
	mapToShards func(string) (string, []string, error),
) (*mproto.QueryResult, error) {
	keyspace, shards, err := mapToShards(keyspace)
	if err != nil {
		return nil, err
	}
	for {
		qr, err := res.scatterConn.Execute(
			context,
			sql,
			bindVars,
			keyspace,
			shards,
			tabletType,
			NewSafeSession(session))
		if connError, ok := err.(*ShardConnError); ok && connError.Code == tabletconn.ERR_RETRY {
			resharding := false
			newKeyspace, newShards, err := mapToShards(keyspace)
			if err != nil {
				return nil, err
			}
			// check keyspace change for vertical resharding
			if newKeyspace != keyspace {
				keyspace = newKeyspace
				resharding = true
			}
			// check shards change for horizontal resharding
			if !StrsEquals(newShards, shards) {
				shards = newShards
				resharding = true
			}
			// retry if resharding happened
			if resharding {
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		return qr, err
	}
}

// ExecuteEntityIds executes a non-streaming query based on given KeyspaceId map.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) ExecuteEntityIds(
	context context.Context,
	query *proto.EntityIdsQuery,
) (*mproto.QueryResult, error) {
	newKeyspace, shardIDMap, err := mapEntityIdsToShards(
		res.scatterConn.toposerv,
		res.scatterConn.cell,
		query.Keyspace,
		query.EntityKeyspaceIDs,
		query.TabletType)
	if err != nil {
		return nil, err
	}
	query.Keyspace = newKeyspace
	shards, sqls, bindVars := buildEntityIds(shardIDMap, query.Sql, query.EntityColumnName, query.BindVariables)
	for {
		qr, err := res.scatterConn.ExecuteEntityIds(
			context,
			shards,
			sqls,
			bindVars,
			query.Keyspace,
			query.TabletType,
			NewSafeSession(query.Session))
		if connError, ok := err.(*ShardConnError); ok && connError.Code == tabletconn.ERR_RETRY {
			resharding := false
			newKeyspace, newShardIDMap, err := mapEntityIdsToShards(
				res.scatterConn.toposerv,
				res.scatterConn.cell,
				query.Keyspace,
				query.EntityKeyspaceIDs,
				query.TabletType)
			if err != nil {
				return nil, err
			}
			// check keyspace change for vertical resharding
			if newKeyspace != query.Keyspace {
				query.Keyspace = newKeyspace
				resharding = true
			}
			// check shards change for horizontal resharding
			newShards, newSqls, newBindVars := buildEntityIds(newShardIDMap, query.Sql, query.EntityColumnName, query.BindVariables)
			if !StrsEquals(newShards, shards) {
				shards = newShards
				sqls = newSqls
				bindVars = newBindVars
				resharding = true
			}
			// retry if resharding happened
			if resharding {
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		return qr, err
	}
}

// ExecuteBatchKeyspaceIds executes a group of queries based on KeyspaceIds.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) ExecuteBatchKeyspaceIds(context context.Context, query *proto.KeyspaceIdBatchQuery) (*tproto.QueryResultList, error) {
	mapToShards := func(keyspace string) (string, []string, error) {
		return mapKeyspaceIdsToShards(
			res.scatterConn.toposerv,
			res.scatterConn.cell,
			keyspace,
			query.TabletType,
			query.KeyspaceIds)
	}
	return res.ExecuteBatch(context, query.Queries, query.Keyspace, query.TabletType, query.Session, mapToShards)
}

// ExecuteBatch executes a group of queries based on shards resolved by given func.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) ExecuteBatch(
	context context.Context,
	queries []tproto.BoundQuery,
	keyspace string,
	tabletType topo.TabletType,
	session *proto.Session,
	mapToShards func(string) (string, []string, error),
) (*tproto.QueryResultList, error) {
	keyspace, shards, err := mapToShards(keyspace)
	if err != nil {
		return nil, err
	}
	for {
		qrs, err := res.scatterConn.ExecuteBatch(
			context,
			queries,
			keyspace,
			shards,
			tabletType,
			NewSafeSession(session))
		if connError, ok := err.(*ShardConnError); ok && connError.Code == tabletconn.ERR_RETRY {
			resharding := false
			newKeyspace, newShards, err := mapToShards(keyspace)
			if err != nil {
				return nil, err
			}
			// check keyspace change for vertical resharding
			if newKeyspace != keyspace {
				keyspace = newKeyspace
				resharding = true
			}
			// check shards change for horizontal resharding
			if !StrsEquals(newShards, shards) {
				shards = newShards
				resharding = true
			}
			// retry if resharding happened
			if resharding {
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		return qrs, err
	}
}

// StreamExecuteKeyspaceIds executes a streaming query on the specified KeyspaceIds.
// The KeyspaceIds are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple KeyspaceIds to make it future proof.
func (res *Resolver) StreamExecuteKeyspaceIds(context context.Context, query *proto.KeyspaceIdQuery, sendReply func(*mproto.QueryResult) error) error {
	mapToShards := func(keyspace string) (string, []string, error) {
		return mapKeyspaceIdsToShards(
			res.scatterConn.toposerv,
			res.scatterConn.cell,
			query.Keyspace,
			query.TabletType,
			query.KeyspaceIds)
	}
	return res.StreamExecute(context, query.Sql, query.BindVariables, query.Keyspace, query.TabletType, query.Session, mapToShards, sendReply)
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (res *Resolver) StreamExecuteKeyRanges(context context.Context, query *proto.KeyRangeQuery, sendReply func(*mproto.QueryResult) error) error {
	mapToShards := func(keyspace string) (string, []string, error) {
		return mapKeyRangesToShards(
			res.scatterConn.toposerv,
			res.scatterConn.cell,
			query.Keyspace,
			query.TabletType,
			query.KeyRanges)
	}
	return res.StreamExecute(context, query.Sql, query.BindVariables, query.Keyspace, query.TabletType, query.Session, mapToShards, sendReply)
}

// StreamExecuteShard executes a streaming query on shards resolved by given func.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
func (res *Resolver) StreamExecute(
	context context.Context,
	sql string,
	bindVars map[string]interface{},
	keyspace string,
	tabletType topo.TabletType,
	session *proto.Session,
	mapToShards func(string) (string, []string, error),
	sendReply func(*mproto.QueryResult) error,
) error {
	keyspace, shards, err := mapToShards(keyspace)
	if err != nil {
		return err
	}
	err = res.scatterConn.StreamExecute(
		context,
		sql,
		bindVars,
		keyspace,
		shards,
		tabletType,
		NewSafeSession(session),
		sendReply)
	return err
}

// Commit commits a transaction.
func (res *Resolver) Commit(context context.Context, inSession *proto.Session) error {
	return res.scatterConn.Commit(context, NewSafeSession(inSession))
}

// Rollback rolls back a transaction.
func (res *Resolver) Rollback(context context.Context, inSession *proto.Session) error {
	return res.scatterConn.Rollback(context, NewSafeSession(inSession))
}

// StrsEquals compares contents of two string slices.
func StrsEquals(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func buildEntityIds(shardIDMap map[string][]interface{}, qSql, entityColName string, qBindVars map[string]interface{}) ([]string, map[string]string, map[string]map[string]interface{}) {
	shards := make([]string, len(shardIDMap))
	shardsIdx := 0
	sqls := make(map[string]string)
	bindVars := make(map[string]map[string]interface{})
	for shard, ids := range shardIDMap {
		var b bytes.Buffer
		b.Write([]byte(entityColName))
		b.Write(openBracket)
		bindVar := make(map[string]interface{})
		for k, v := range qBindVars {
			bindVar[k] = v
		}
		for i, id := range ids {
			bvName := fmt.Sprintf("%v%v", entityColName, i)
			bindVar[bvName] = id
			if i > 0 {
				b.Write(separator)
			}
			b.Write(sqlVarIdentifier)
			b.Write([]byte(bvName))
		}
		b.Write(closeBracket)
		sqls[shard] = insertSqlClause(qSql, b.String())
		bindVars[shard] = bindVar
		shards[shardsIdx] = shard
		shardsIdx++
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
		b.Write(kwAnd)
	} else {
		b.Write(kwWhere)
	}
	b.Write([]byte(clause))
	if idxExtra < len(sql) {
		b.Write([]byte(querySql[idxExtra:]))
	}
	return b.String()
}

func isDml(querySql string) bool {
	var sqlKW string
	if i := strings.Index(querySql, " "); i >= 0 {
		sqlKW = querySql[:i]
	}
	sqlKW = strings.ToLower(sqlKW)
	if sqlKW == insert_dml || sqlKW == update_dml || sqlKW == delete_dml {
		return true
	}
	return false
}
