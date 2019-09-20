/*
Copyright 2019 The Vitess Authors.

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
	"sync"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/gateway"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
// resharding happened. It is implemented using a srvtopo.Resolver.
type Resolver struct {
	scatterConn *ScatterConn
	resolver    *srvtopo.Resolver
	toposerv    srvtopo.Server
	cell        string
}

// NewResolver creates a new Resolver.
func NewResolver(resolver *srvtopo.Resolver, serv srvtopo.Server, cell string, sc *ScatterConn) *Resolver {
	return &Resolver{
		scatterConn: sc,
		resolver:    resolver,
		toposerv:    serv,
		cell:        cell,
	}
}

// isRetryableError will be true if the error should be retried.
func isRetryableError(err error) bool {
	return vterrors.Code(err) == vtrpcpb.Code_FAILED_PRECONDITION
}

// Execute executes a non-streaming query based on provided destination.
// It retries query if new keyspace/shards are re-resolved after a retryable error.
func (res *Resolver) Execute(
	ctx context.Context,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	keyspace string,
	tabletType topodatapb.TabletType,
	destination key.Destination,
	session *vtgatepb.Session,
	notInTransaction bool,
	options *querypb.ExecuteOptions,
	logStats *LogStats,
) (*sqltypes.Result, error) {
	rss, err := res.resolver.ResolveDestination(ctx, keyspace, tabletType, destination)
	if err != nil {
		return nil, err
	}
	if logStats != nil {
		logStats.ShardQueries = uint32(len(rss))
	}
	for {
		qr, err := res.scatterConn.Execute(
			ctx,
			sql,
			bindVars,
			rss,
			tabletType,
			NewSafeSession(session),
			notInTransaction,
			options)
		if isRetryableError(err) {
			newRss, err := res.resolver.ResolveDestination(ctx, keyspace, tabletType, destination)
			if err != nil {
				return nil, err
			}
			if !srvtopo.ResolvedShardsEqual(rss, newRss) {
				// If the mapping to underlying shards changed,
				// we might be resharding. Try again.
				rss = newRss
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
	// Unpack the entityKeyspaceIDs into []ids and []Destination
	ids := make([]*querypb.Value, len(entityKeyspaceIDs))
	destinations := make([]key.Destination, len(entityKeyspaceIDs))
	for i, eki := range entityKeyspaceIDs {
		ids[i] = &querypb.Value{
			Type:  eki.Type,
			Value: eki.Value,
		}
		destinations[i] = key.DestinationKeyspaceID(eki.KeyspaceId)
	}

	rss, values, err := res.resolver.ResolveDestinations(
		ctx,
		keyspace,
		tabletType,
		ids,
		destinations)
	if err != nil {
		return nil, err
	}
	for {
		sqls, bindVars := buildEntityIds(values, sql, entityColumnName, bindVariables)
		qr, err := res.scatterConn.ExecuteEntityIds(
			ctx,
			rss,
			sqls,
			bindVars,
			tabletType,
			NewSafeSession(session),
			notInTransaction,
			options)
		if isRetryableError(err) {
			newRss, newValues, err := res.resolver.ResolveDestinations(
				ctx,
				keyspace,
				tabletType,
				ids,
				destinations)
			if err != nil {
				return nil, err
			}
			if !srvtopo.ResolvedShardsEqual(rss, newRss) || !srvtopo.ValuesEqual(values, newValues) {
				// Retry if resharding happened.
				rss = newRss
				values = newValues
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		return qr, err
	}
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

// StreamExecute executes a streaming query on shards resolved by given func.
// This function currently temporarily enforces the restriction of executing on
// one shard since it cannot merge-sort the results to guarantee ordering of
// response which is needed for checkpointing.
// Note we guarantee the callback will not be called concurrently
// by mutiple go routines.
func (res *Resolver) StreamExecute(
	ctx context.Context,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	keyspace string,
	tabletType topodatapb.TabletType,
	destination key.Destination,
	options *querypb.ExecuteOptions,
	callback func(*sqltypes.Result) error,
) error {
	rss, err := res.resolver.ResolveDestination(ctx, keyspace, tabletType, destination)
	if err != nil {
		return err
	}
	err = res.scatterConn.StreamExecute(
		ctx,
		sql,
		bindVars,
		rss,
		tabletType,
		options,
		callback)
	return err
}

// MessageStream streams messages.
func (res *Resolver) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	var destination key.Destination
	if shard != "" {
		// If we pass in a shard, resolve the keyspace/shard
		// following redirects.
		destination = key.DestinationShard(shard)
	} else {
		// If we pass in a KeyRange, resolve it to the proper shards.
		// Note we support multiple shards here, we will just aggregate
		// the message streams.
		destination = key.DestinationExactKeyRange{KeyRange: keyRange}
	}
	rss, err := res.resolver.ResolveDestination(ctx, keyspace, topodatapb.TabletType_MASTER, destination)
	if err != nil {
		return err
	}
	return res.scatterConn.MessageStream(ctx, rss, name, callback)
}

// MessageAckKeyspaceIds routes message acks based on the associated keyspace ids.
func (res *Resolver) MessageAckKeyspaceIds(ctx context.Context, keyspace, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error) {
	ids := make([]*querypb.Value, len(idKeyspaceIDs))
	ksids := make([]key.Destination, len(idKeyspaceIDs))
	for i, iki := range idKeyspaceIDs {
		ids[i] = iki.Id
		ksids[i] = key.DestinationKeyspaceID(iki.KeyspaceId)
	}

	rss, values, err := res.resolver.ResolveDestinations(ctx, keyspace, topodatapb.TabletType_MASTER, ids, ksids)
	if err != nil {
		return 0, err
	}

	return res.scatterConn.MessageAck(ctx, rss, values, name)
}

// UpdateStream streams the events.
// TODO(alainjobart): Implement the multi-shards merge code.
func (res *Resolver) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	var destination key.Destination
	if shard != "" {
		// If we pass in a shard, resolve the keyspace/shard
		// following redirects.
		destination = key.DestinationShard(shard)
	} else {
		// If we pass in a KeyRange, resolve it to one shard
		// only for now.
		destination = key.DestinationExactKeyRange{KeyRange: keyRange}
	}
	rss, err := res.resolver.ResolveDestination(ctx, keyspace, tabletType, destination)
	if err != nil {
		return err
	}
	if len(rss) != 1 {
		return fmt.Errorf("UpdateStream only supports exactly one shard per keyrange at the moment, but provided keyrange %v maps to %v shards", keyRange, len(rss))
	}

	// Just send it to ScatterConn.  With just one connection, the
	// timestamp to resume from is the one we get.
	// Also use the incoming event if the shard matches.
	position := ""
	if event != nil && event.Shard == shard {
		position = event.Position
		timestamp = 0
	}
	return res.scatterConn.UpdateStream(ctx, rss[0], timestamp, position, func(se *querypb.StreamEvent) error {
		var timestamp int64
		if se.EventToken != nil {
			timestamp = se.EventToken.Timestamp
			se.EventToken.Shard = shard
		}
		return callback(se, timestamp)
	})
}

// VStream streams events from one target. This function ensures that events of each
// transaction are streamed together, along with the corresponding GTID.
func (res *Resolver) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, send func(events []*binlogdatapb.VEvent) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// mu protects sending on ch, err and positions.
	// mu is needed for sending because transactions can come
	// in separate chunks. If so, we have to send all the
	// chunks together.
	var mu sync.Mutex
	ch := make(chan []*binlogdatapb.VEvent)
	var outerErr error

	positions := make(map[topo.KeyspaceShard]*binlogdatapb.ShardGtid, len(vgtid.ShardGtids))
	for _, shardPos := range vgtid.ShardGtids {
		ks := topo.KeyspaceShard{Keyspace: shardPos.Keyspace, Shard: shardPos.Shard}
		positions[ks] = shardPos
	}

	var loopwg, wg sync.WaitGroup
	// Make sure goroutines don't start until loop has exited.
	// Otherwise there's a race because the goroutines update the map.
	loopwg.Add(1)
	for ks, pos := range positions {
		wg.Add(1)
		go func(ks topo.KeyspaceShard, pos string) {
			loopwg.Wait()
			defer wg.Done()
			err := res.vstreamOneShard(ctx, ks.Keyspace, ks.Shard, tabletType, pos, filter, func(eventss [][]*binlogdatapb.VEvent) error {
				mu.Lock()
				defer mu.Unlock()

				// Send all chunks while holding the lock.
				for _, evs := range eventss {
					// Replace GTID and table names.
					for _, ev := range evs {
						switch ev.Type {
						case binlogdatapb.VEventType_GTID:
							// Update the VGtid and send that instead.
							positions[ks].Gtid = ev.Gtid
							ev.Type = binlogdatapb.VEventType_VGTID
							ev.Gtid = ""
							ev.Vgtid = proto.Clone(vgtid).(*binlogdatapb.VGtid)
						case binlogdatapb.VEventType_FIELD:
							ev.FieldEvent.TableName = ks.Keyspace + "." + ev.FieldEvent.TableName
						case binlogdatapb.VEventType_ROW:
							ev.RowEvent.TableName = ks.Keyspace + "." + ev.RowEvent.TableName
						}
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case ch <- evs:
					}
				}
				return nil
			})

			// Set the error on exit. First one wins.
			mu.Lock()
			defer mu.Unlock()
			if outerErr == nil {
				outerErr = err
				cancel()
			}
		}(ks, pos.Gtid)
	}
	// Allow goroutines to start.
	loopwg.Done()

	go func() {
		wg.Wait()
		close(ch)
	}()

	for ev := range ch {
		if err := send(ev); err != nil {
			return err
		}
	}

	return outerErr
}

// vstreamOneShard streams from one shard. If transactions come in separate chunks, they are grouped and sent.
func (res *Resolver) vstreamOneShard(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, startPos string, filter *binlogdatapb.Filter, send func(eventss [][]*binlogdatapb.VEvent) error) error {
	errCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var eventss [][]*binlogdatapb.VEvent
		rss, err := res.resolver.ResolveDestination(ctx, keyspace, tabletType, key.DestinationShard(shard))
		if err != nil {
			return err
		}
		if len(rss) != 1 {
			// Unreachable.
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected number or shards: %v", rss)
		}
		err = rss[0].QueryService.VStream(ctx, rss[0].Target, startPos, filter, func(events []*binlogdatapb.VEvent) error {
			// Remove all heartbeat events for now.
			// Otherwise they can accumulate indefinitely if there are no real events.
			// TODO(sougou): figure out a model for this.
			for i := 0; i < len(events); i++ {
				if events[i].Type == binlogdatapb.VEventType_HEARTBEAT {
					events = append(events[:i], events[i+1:]...)
				}
			}
			if len(events) == 0 {
				return nil
			}
			// We received a valid event. Reset error count.
			errCount = 0

			eventss = append(eventss, events)
			lastEvent := events[len(events)-1]
			switch lastEvent.Type {
			case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL:
				if err := send(eventss); err != nil {
					return err
				}
				eventss = nil
			}
			return nil
		})
		if err == nil {
			// Unreachable.
			err = vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstream ended unexpectedly")
		}
		if !isRetryableError(err) {
			log.Errorf("vstream for %s/%s error: %v", keyspace, shard, err)
			return err
		}
		errCount++
		if errCount >= 3 {
			log.Errorf("vstream for %s/%s had three consecutive failures: %v", keyspace, shard, err)
			return err
		}
	}
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

// buildEntityIds populates SQL and BindVariables.
func buildEntityIds(values [][]*querypb.Value, qSQL, entityColName string, qBindVars map[string]*querypb.BindVariable) ([]string, []map[string]*querypb.BindVariable) {
	sqls := make([]string, len(values))
	bindVars := make([]map[string]*querypb.BindVariable, len(values))
	for i, val := range values {
		var b bytes.Buffer
		b.Write([]byte(entityColName))
		bindVariables := make(map[string]*querypb.BindVariable)
		for k, v := range qBindVars {
			bindVariables[k] = v
		}
		bvName := fmt.Sprintf("%v_entity_ids", entityColName)
		bindVariables[bvName] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: val,
		}
		b.Write(inOperator)
		b.Write(sqlListIdentifier)
		b.Write([]byte(bvName))
		sqls[i] = insertSQLClause(qSQL, b.String())
		bindVars[i] = bindVariables
	}
	return sqls, bindVars
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
