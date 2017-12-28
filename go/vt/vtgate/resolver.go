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

package vtgate

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	sqlListIdentifier = []byte("::")
	inOperator        = []byte(" in ")
	kwAnd             = []byte(" and ")
	kwWhere           = []byte(" where ")
)

// Resolver is the layer to resolve KeyspaceIds and KeyRanges
// to shards. It will try to re-resolve shards if ScatterConn
// returns retryable error, which may imply horizontal or vertical
// resharding happened.
type Resolver struct {
	scatterConn *ScatterConn
	toposerv    topo.SrvTopoServer
	cell        string
}

// NewResolver creates a new Resolver.
func NewResolver(serv topo.SrvTopoServer, cell string, sc *ScatterConn) *Resolver {
	return &Resolver{
		scatterConn: sc,
		toposerv:    serv,
		cell:        cell,
	}
}

// isRetryableError will be true if the error should be retried.
func isRetryableError(err error) bool {
	return vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION
}

// ExecuteKeyspaceIds executes a non-streaming query based on KeyspaceIds.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
// This throws an error if a dml spans multiple keyspace_ids. Resharding depends
// on being able to uniquely route a write.
func (res *Resolver) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if sqlparser.IsDML(sql) && len(keyspaceIds) > 1 {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "DML should not span multiple keyspace_ids")
	}
	mapToShards := func(k string) (string, []string, error) {
		return mapKeyspaceIdsToShards(
			ctx,
			res.toposerv,
			res.cell,
			k,
			tabletType,
			keyspaceIds)
	}
	return res.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, mapToShards, notInTransaction, options, nil /* LogStats */)
}

// ExecuteKeyRanges executes a non-streaming query based on KeyRanges.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	mapToShards := func(k string) (string, []string, error) {
		return mapKeyRangesToShards(
			ctx,
			res.toposerv,
			res.cell,
			k,
			tabletType,
			keyRanges)
	}
	return res.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, mapToShards, notInTransaction, options, nil)
}

// Execute executes a non-streaming query based on shards resolved by given func.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) Execute(
	ctx context.Context,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	keyspace string,
	tabletType topodatapb.TabletType,
	session *vtgatepb.Session,
	mapToShards func(string) (string, []string, error),
	notInTransaction bool,
	options *querypb.ExecuteOptions,
	logStats *LogStats,
) (*sqltypes.Result, error) {
	keyspace, shards, err := mapToShards(keyspace)
	if err != nil {
		return nil, err
	}
	if logStats != nil {
		logStats.ShardQueries = uint32(len(shards))
	}
	for {
		qr, err := res.scatterConn.Execute(
			ctx,
			sql,
			bindVars,
			keyspace,
			shards,
			tabletType,
			NewSafeSession(session),
			notInTransaction,
			options)
		if isRetryableError(err) {
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
	ctx context.Context,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	keyspace string,
	entityColumnName string,
	entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId,
	tabletType topodatapb.TabletType,
	session *vtgatepb.Session,
	notInTransaction bool,
	options *querypb.ExecuteOptions,
) (*sqltypes.Result, error) {
	newKeyspace, shardIDMap, err := mapEntityIdsToShards(
		ctx,
		res.toposerv,
		res.cell,
		keyspace,
		entityKeyspaceIDs,
		tabletType)
	if err != nil {
		return nil, err
	}
	keyspace = newKeyspace
	shards, sqls, bindVars := buildEntityIds(shardIDMap, sql, entityColumnName, bindVariables)
	for {
		qr, err := res.scatterConn.ExecuteEntityIds(
			ctx,
			shards,
			sqls,
			bindVars,
			keyspace,
			tabletType,
			NewSafeSession(session),
			notInTransaction,
			options)
		if isRetryableError(err) {
			resharding := false
			newKeyspace, newShardIDMap, err := mapEntityIdsToShards(
				ctx,
				res.toposerv,
				res.cell,
				keyspace,
				entityKeyspaceIDs,
				tabletType)
			if err != nil {
				return nil, err
			}
			// check keyspace change for vertical resharding
			if newKeyspace != keyspace {
				keyspace = newKeyspace
				resharding = true
			}
			// check shards change for horizontal resharding
			newShards, newSqls, newBindVars := buildEntityIds(newShardIDMap, sql, entityColumnName, bindVariables)
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
func (res *Resolver) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	buildBatchRequest := func() (*scatterBatchRequest, error) {
		shardQueries, err := boundKeyspaceIDQueriesToBoundShardQueries(ctx, res.toposerv, res.cell, tabletType, queries)
		if err != nil {
			return nil, err
		}
		return boundShardQueriesToScatterBatchRequest(shardQueries)
	}
	return res.ExecuteBatch(ctx, tabletType, asTransaction, session, options, buildBatchRequest)
}

// ExecuteBatch executes a group of queries based on shards resolved by given func.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) ExecuteBatch(
	ctx context.Context,
	tabletType topodatapb.TabletType,
	asTransaction bool,
	session *vtgatepb.Session,
	options *querypb.ExecuteOptions,
	buildBatchRequest func() (*scatterBatchRequest, error),
) ([]sqltypes.Result, error) {
	batchRequest, err := buildBatchRequest()
	if err != nil {
		return nil, err
	}
	for {
		qrs, err := res.scatterConn.ExecuteBatch(
			ctx,
			batchRequest,
			tabletType,
			asTransaction,
			NewSafeSession(session),
			options)
		// Don't retry transactional requests.
		if asTransaction {
			return qrs, err
		}
		// If lower level retries failed, check if there was a resharding event
		// and retry again if needed.
		if isRetryableError(err) {
			newBatchRequest, buildErr := buildBatchRequest()
			if buildErr != nil {
				return nil, buildErr
			}
			// Use reflect to see if the request has changed.
			if reflect.DeepEqual(*batchRequest, *newBatchRequest) {
				return qrs, err
			}
			batchRequest = newBatchRequest
			continue
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
func (res *Resolver) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	mapToShards := func(k string) (string, []string, error) {
		return mapKeyspaceIdsToShards(
			ctx,
			res.toposerv,
			res.cell,
			k,
			tabletType,
			keyspaceIds)
	}
	return res.streamExecute(ctx, sql, bindVariables, keyspace, tabletType, mapToShards, options, callback)
}

// StreamExecuteKeyRanges executes a streaming query on the specified KeyRanges.
// The KeyRanges are resolved to shards using the serving graph.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// The api supports supplying multiple keyranges to make it future proof.
func (res *Resolver) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]*querypb.BindVariable, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	mapToShards := func(k string) (string, []string, error) {
		return mapKeyRangesToShards(
			ctx,
			res.toposerv,
			res.cell,
			k,
			tabletType,
			keyRanges)
	}
	return res.streamExecute(ctx, sql, bindVariables, keyspace, tabletType, mapToShards, options, callback)
}

// streamExecute executes a streaming query on shards resolved by given func.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
func (res *Resolver) streamExecute(
	ctx context.Context,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	keyspace string,
	tabletType topodatapb.TabletType,
	mapToShards func(string) (string, []string, error),
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) error {
	keyspace, shards, err := mapToShards(keyspace)
	if err != nil {
		return err
	}
	err = res.scatterConn.StreamExecute(
		ctx,
		sql,
		bindVars,
		keyspace,
		shards,
		tabletType,
		options,
		callback)
	return err
}

// MessageStream streams messages.
func (res *Resolver) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	var shards []string
	var err error
	if shard != "" {
		// If we pass in a shard, resolve the keyspace following redirects.
		keyspace, _, _, err = getKeyspaceShards(ctx, res.toposerv, res.cell, keyspace, topodatapb.TabletType_MASTER)
		shards = []string{shard}
	} else {
		// If we pass in a KeyRange, resolve it to one shard only for now.
		keyspace, shards, err = mapExactShards(ctx, res.toposerv, res.cell, keyspace, topodatapb.TabletType_MASTER, keyRange)
	}
	if err != nil {
		return err
	}
	return res.scatterConn.MessageStream(ctx, keyspace, shards, name, callback)
}

// MessageAckKeyspaceIds routes message acks based on the associated keyspace ids.
func (res *Resolver) MessageAckKeyspaceIds(ctx context.Context, keyspace, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error) {
	newKeyspace, _, allShards, err := getKeyspaceShards(ctx, res.toposerv, res.cell, keyspace, topodatapb.TabletType_MASTER)
	if err != nil {
		return 0, err
	}

	shardIDs := make(map[string][]*querypb.Value)
	for _, idKeyspaceID := range idKeyspaceIDs {
		shard, err := getShardForKeyspaceID(allShards, idKeyspaceID.KeyspaceId)
		if err != nil {
			return 0, err
		}
		shardIDs[shard] = append(shardIDs[shard], idKeyspaceID.Id)
	}
	return res.scatterConn.MessageAck(ctx, newKeyspace, shardIDs, name)
}

// UpdateStream streams the events.
// TODO(alainjobart): Implement the multi-shards merge code.
func (res *Resolver) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	if shard != "" {
		// If we pass in a shard, resolve the keyspace following redirects.
		var err error
		keyspace, _, _, err = getKeyspaceShards(ctx, res.toposerv, res.cell, keyspace, tabletType)
		if err != nil {
			return err
		}
	} else {
		// If we pass in a KeyRange, resolve it to one shard only for now.
		var shards []string
		var err error
		keyspace, shards, err = mapExactShards(
			ctx,
			res.toposerv,
			res.cell,
			keyspace,
			tabletType,
			keyRange)
		if err != nil {
			return err
		}
		if len(shards) != 1 {
			return fmt.Errorf("UpdateStream only supports exactly one shard per keyrange at the moment, but provided keyrange %v maps to %v", keyRange, shards)
		}
		shard = shards[0]
	}

	// Just send it to ScatterConn.  With just one connection, the
	// timestamp to resume from is the one we get.
	// Also use the incoming event if the shard matches.
	position := ""
	if event != nil && event.Shard == shard {
		position = event.Position
		timestamp = 0
	}
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	return res.scatterConn.UpdateStream(ctx, target, timestamp, position, func(se *querypb.StreamEvent) error {
		var timestamp int64
		if se.EventToken != nil {
			timestamp = se.EventToken.Timestamp
			se.EventToken.Shard = shard
		}
		return callback(se, timestamp)
	})
}

// GetGatewayCacheStatus returns a displayable version of the Gateway cache.
func (res *Resolver) GetGatewayCacheStatus() gateway.TabletCacheStatusList {
	return res.scatterConn.GetGatewayCacheStatus()
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

func buildEntityIds(shardIDMap map[string][]*querypb.Value, qSQL, entityColName string, qBindVars map[string]*querypb.BindVariable) ([]string, map[string]string, map[string]map[string]*querypb.BindVariable) {
	shards := make([]string, len(shardIDMap))
	shardsIdx := 0
	sqls := make(map[string]string)
	bindVars := make(map[string]map[string]*querypb.BindVariable)
	for shard, values := range shardIDMap {
		var b bytes.Buffer
		b.Write([]byte(entityColName))
		bindVar := make(map[string]*querypb.BindVariable)
		for k, v := range qBindVars {
			bindVar[k] = v
		}
		bvName := fmt.Sprintf("%v_entity_ids", entityColName)
		bindVar[bvName] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: values,
		}
		b.Write(inOperator)
		b.Write(sqlListIdentifier)
		b.Write([]byte(bvName))
		bindVars[shard] = bindVar
		sqls[shard] = insertSQLClause(qSQL, b.String())
		shards[shardsIdx] = shard
		shardsIdx++
	}
	return shards, sqls, bindVars
}

func insertSQLClause(querySQL, clause string) string {
	// get first index of any additional clause: group by, order by, limit, for update, sql end if nothing
	// insert clause into the index position
	sql := strings.ToLower(querySQL)
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
	b.Write([]byte(querySQL[:idxExtra]))
	if strings.Contains(sql, "where") {
		b.Write(kwAnd)
	} else {
		b.Write(kwWhere)
	}
	b.Write([]byte(clause))
	if idxExtra < len(sql) {
		b.Write([]byte(querySQL[idxExtra:]))
	}
	return b.String()
}
