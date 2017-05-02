// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlannotation"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// Executor is the engine that executes queries by utilizing
// the abilities of the underlying vttablets.
type Executor struct {
	serv        topo.SrvTopoServer
	cell        string
	resolver    *Resolver
	scatterConn *ScatterConn
	txConn      *TxConn

	mu           sync.Mutex
	vschema      *vindexes.VSchema
	normalize    bool
	plans        *cache.LRUCache
	vschemaStats *VSchemaStats
}

var executorOnce sync.Once

// NewExecutor creates a new Executor.
func NewExecutor(ctx context.Context, serv topo.SrvTopoServer, cell, statsName string, resolver *Resolver, normalize bool) *Executor {
	exr := &Executor{
		serv:        serv,
		cell:        cell,
		resolver:    resolver,
		scatterConn: resolver.scatterConn,
		txConn:      resolver.scatterConn.txConn,
		plans:       cache.NewLRUCache(10000),
		normalize:   normalize,
	}
	exr.watchSrvVSchema(ctx, cell)
	executorOnce.Do(func() {
		http.Handle("/debug/query_plans", exr)
		http.Handle("/debug/vschema", exr)
	})
	return exr
}

// Execute executes a non-streaming query.
func (exr *Executor) Execute(ctx context.Context, sql string, bindVars map[string]interface{}, session *vtgatepb.Session) (*sqltypes.Result, error) {
	intercepted, err := exr.intercept(ctx, sql, session)
	if err != nil {
		return nil, err
	}
	if intercepted {
		return &sqltypes.Result{}, nil
	}
	nsf := NewSafeSession(session)

	// Autocommit handling
	autocommit := false
	if session.Autocommit && !session.InTransaction && sqlparser.IsDML(sql) {
		autocommit = true
		if err := exr.txConn.Begin(ctx, nsf); err != nil {
			return nil, err
		}
		defer exr.txConn.Rollback(ctx, nsf)
	}

	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	qr, err := exr.innerExec(ctx, sql, bindVars, session)
	if err == nil && autocommit {
		// Set the error if commit fails.
		err = exr.txConn.Commit(ctx, nsf)
	}
	return qr, err
}

// intercept checks for transactional or set statements. If they match then it performs the
// necessary operation and returns a new session and true indicating that it's intercepted
// the call. If so, the caller (Execute) should just return without proceeding further.
func (exr *Executor) intercept(ctx context.Context, sql string, session *vtgatepb.Session) (bool, error) {
	switch sqlparser.Preview(sql) {
	case sqlparser.StmtBegin:
		err := exr.txConn.Begin(ctx, NewSafeSession(session))
		return true, err
	case sqlparser.StmtCommit:
		err := exr.txConn.Commit(ctx, NewSafeSession(session))
		return true, err
	case sqlparser.StmtRollback:
		if !session.InTransaction {
			return true, nil
		}
		err := exr.txConn.Rollback(ctx, NewSafeSession(session))
		return true, err
	case sqlparser.StmtSet:
		vals, err := sqlparser.ExtractSetNums(sql)
		if err != nil {
			return true, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
		}
		if len(vals) != 1 {
			return true, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "too many set values: %s", sql)
		}
		val, ok := vals["autocommit"]
		if !ok {
			return true, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported construct: %s", sql)
		}
		if val != 0 {
			session.Autocommit = true
		} else {
			session.Autocommit = false
		}
		return true, nil
	}
	return false, nil
}

func (exr *Executor) innerExec(ctx context.Context, sql string, bindVars map[string]interface{}, session *vtgatepb.Session) (*sqltypes.Result, error) {
	target := parseTarget(session.TargetString)
	if target.Shard != "" {
		// V1 mode.
		sql = sqlannotation.AnnotateIfDML(sql, nil)
		f := func(keyspace string) (string, []string, error) {
			return keyspace, []string{target.Shard}, nil
		}
		return exr.resolver.Execute(ctx, sql, bindVars, target.Keyspace, target.TabletType, session, f, false, session.Options)
	}

	// V3 mode.
	vcursor := newVCursorImpl(ctx, target.TabletType, session, exr)
	queryConstruct := queryinfo.NewQueryConstruct(sql, target.Keyspace, bindVars)
	plan, err := exr.getPlan(sql, target.Keyspace, bindVars)
	if err != nil {
		return nil, err
	}
	return plan.Instructions.Execute(vcursor, queryConstruct, make(map[string]interface{}), true)
}

// StreamExecute executes a streaming query.
func (exr *Executor) StreamExecute(ctx context.Context, sql string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, callback func(*sqltypes.Result) error) error {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	vcursor := newVCursorImpl(ctx, tabletType, session, exr)
	queryConstruct := queryinfo.NewQueryConstruct(sql, keyspace, bindVars)
	plan, err := exr.getPlan(sql, keyspace, bindVars)
	if err != nil {
		return err
	}
	return plan.Instructions.StreamExecute(vcursor, queryConstruct, make(map[string]interface{}), true, callback)
}

// MessageAck acks messages.
func (exr *Executor) MessageAck(ctx context.Context, keyspace, name string, ids []*querypb.Value) (int64, error) {
	vschema := exr.VSchema()
	if vschema == nil {
		return 0, errors.New("vschema not initialized")
	}
	table, err := vschema.Find(keyspace, name)
	if err != nil {
		return 0, err
	}
	// TODO(sougou): Change this to use Session.
	vcursor := newVCursorImpl(ctx, topodatapb.TabletType_MASTER, &vtgatepb.Session{}, exr)
	newKeyspace, _, allShards, err := getKeyspaceShards(ctx, exr.serv, exr.cell, table.Keyspace.Name, topodatapb.TabletType_MASTER)
	shardIDs := make(map[string][]*querypb.Value)
	if table.Keyspace.Sharded {
		// We always use the (unique) primary vindex. The ID must be the
		// primary vindex for message tables.
		mapper := table.ColumnVindexes[0].Vindex.(vindexes.Unique)
		// convert []*querypb.Value to []interface{} for calling Map.
		asInterface := make([]interface{}, 0, len(ids))
		for _, id := range ids {
			asInterface = append(asInterface, &querypb.BindVariable{
				Type:  id.Type,
				Value: id.Value,
			})
		}
		ksids, err := mapper.Map(vcursor, asInterface)
		if err != nil {
			return 0, err
		}
		for i, ksid := range ksids {
			if len(ksid) == 0 {
				continue
			}
			shard, err := getShardForKeyspaceID(allShards, ksid)
			if err != nil {
				return 0, err
			}
			shardIDs[shard] = append(shardIDs[shard], ids[i])
		}
	} else {
		shardIDs[allShards[0].Name] = ids
	}
	return exr.scatterConn.MessageAck(ctx, newKeyspace, shardIDs, name)
}

// IsKeyspaceRangeBasedSharded returns true if the keyspace in the vschema is
// marked as sharded.
func (exr *Executor) IsKeyspaceRangeBasedSharded(keyspace string) bool {
	vschema := exr.VSchema()
	ks, ok := vschema.Keyspaces[keyspace]
	if !ok {
		return false
	}
	if ks.Keyspace == nil {
		return false
	}
	return ks.Keyspace.Sharded
}

// watchSrvVSchema watches the SrvVSchema from the topo. The function does
// not return an error. It instead logs warnings on failure.
// The SrvVSchema object is roll-up of all the Keyspace information,
// so when a keyspace is added or removed, it will be properly updated.
//
// This function will wait until the first value has either been processed
// or triggered an error before returning.
func (exr *Executor) watchSrvVSchema(ctx context.Context, cell string) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		foundFirstValue := false

		// Create a closure to save the vschema. If the value
		// passed is nil, it means we encountered an error and
		// we don't know the real value. In this case, we want
		// to use the previous value if it was set, or an
		// empty vschema if it wasn't.
		saveVSchema := func(v *vschemapb.SrvVSchema, errorMessage string) {
			// transform the provided SrvVSchema into a VSchema
			var vschema *vindexes.VSchema
			if v != nil {
				var err error
				vschema, err = vindexes.BuildVSchema(v)
				if err != nil {
					log.Warningf("Error creating VSchema for cell %v (will try again next update): %v", cell, err)
					v = nil
					errorMessage = fmt.Sprintf("Error creating VSchema for cell %v: %v", cell, err)
					if vschemaCounters != nil {
						vschemaCounters.Add("Parsing", 1)
					}
				}
			}
			if v == nil {
				// we encountered an error, build an
				// empty vschema
				vschema, _ = vindexes.BuildVSchema(&vschemapb.SrvVSchema{})
			}

			// Build the display version.
			stats := NewVSchemaStats(vschema, errorMessage)

			// save our value
			exr.mu.Lock()
			if v != nil {
				// no errors, we can save our schema
				exr.vschema = vschema
			} else {
				// we had an error, use the empty vschema
				// if we had nothing before.
				if exr.vschema == nil {
					exr.vschema = vschema
				}
			}
			exr.vschemaStats = stats
			exr.mu.Unlock()
			exr.plans.Clear()

			if vschemaCounters != nil {
				vschemaCounters.Add("Reload", 1)
			}

			// notify the listener
			if !foundFirstValue {
				foundFirstValue = true
				wg.Done()
			}
		}

		for {
			current, changes, _ := exr.serv.WatchSrvVSchema(ctx, cell)
			if current.Err != nil {
				// Don't log if there is no VSchema to start with.
				if current.Err != topo.ErrNoNode {
					log.Warningf("Error watching vschema for cell %s (will wait 5s before retrying): %v", cell, current.Err)
				}
				saveVSchema(nil, fmt.Sprintf("Error watching SvrVSchema: %v", current.Err.Error()))
				if vschemaCounters != nil {
					vschemaCounters.Add("WatchError", 1)
				}
				time.Sleep(5 * time.Second)
				continue
			}
			saveVSchema(current.Value, "")

			for c := range changes {
				if c.Err != nil {
					// If the SrvVschema disappears, we need to clear our record.
					// Otherwise, keep what we already had before.
					if c.Err == topo.ErrNoNode {
						saveVSchema(nil, "SrvVSchema object was removed from topology.")
					}
					log.Warningf("Error while watching vschema for cell %s (will wait 5s before retrying): %v", cell, c.Err)
					if vschemaCounters != nil {
						vschemaCounters.Add("WatchError", 1)
					}
					break
				}
				saveVSchema(c.Value, "")
			}

			// Sleep a bit before trying again.
			time.Sleep(5 * time.Second)
		}
	}()

	// wait for the first value to have been processed
	wg.Wait()
}

// VSchema returns the VSchema.
func (exr *Executor) VSchema() *vindexes.VSchema {
	exr.mu.Lock()
	defer exr.mu.Unlock()
	return exr.vschema
}

// getPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (exr *Executor) getPlan(sql, keyspace string, bindvars map[string]interface{}) (*engine.Plan, error) {
	if exr.VSchema() == nil {
		return nil, errors.New("vschema not initialized")
	}
	key := sql
	if keyspace != "" {
		key = keyspace + ":" + sql
	}
	if result, ok := exr.plans.Get(key); ok {
		return result.(*engine.Plan), nil
	}
	if !exr.normalize {
		plan, err := planbuilder.Build(sql, &wrappedVSchema{
			vschema:  exr.VSchema(),
			keyspace: sqlparser.NewTableIdent(keyspace),
		})
		if err != nil {
			return nil, err
		}
		exr.plans.Set(key, plan)
		return plan, nil
	}
	// Normalize and retry.
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	sqlparser.Normalize(stmt, bindvars, "vtg")
	normalized := sqlparser.String(stmt)
	normkey := normalized
	if keyspace != "" {
		normkey = keyspace + ":" + normalized
	}
	if result, ok := exr.plans.Get(normkey); ok {
		return result.(*engine.Plan), nil
	}
	plan, err := planbuilder.BuildFromStmt(normalized, stmt, &wrappedVSchema{
		vschema:  exr.VSchema(),
		keyspace: sqlparser.NewTableIdent(keyspace),
	})
	if err != nil {
		return nil, err
	}
	exr.plans.Set(normkey, plan)
	return plan, nil
}

// ServeHTTP shows the current plans in the query cache.
func (exr *Executor) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	if request.URL.Path == "/debug/query_plans" {
		keys := exr.plans.Keys()
		response.Header().Set("Content-Type", "text/plain")
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%#v\n", sqlparser.TruncateForUI(v))))
			if plan, ok := exr.plans.Peek(v); ok {
				if b, err := json.MarshalIndent(plan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
				}
				response.Write(([]byte)("\n\n"))
			}
		}
	} else if request.URL.Path == "/debug/vschema" {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		b, err := json.MarshalIndent(exr.VSchema().Keyspaces, "", " ")
		if err != nil {
			response.Write([]byte(err.Error()))
			return
		}
		buf := bytes.NewBuffer(nil)
		json.HTMLEscape(buf, b)
		response.Write(buf.Bytes())
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}

// VSchemaStats returns the loaded vschema stats.
func (exr *Executor) VSchemaStats() *VSchemaStats {
	exr.mu.Lock()
	defer exr.mu.Unlock()
	if exr.vschemaStats == nil {
		return &VSchemaStats{
			Error: "No VSchema loaded yet.",
		}
	}
	return exr.vschemaStats
}

type wrappedVSchema struct {
	vschema  *vindexes.VSchema
	keyspace sqlparser.TableIdent
}

func (vs *wrappedVSchema) Find(keyspace, tablename sqlparser.TableIdent) (table *vindexes.Table, err error) {
	if keyspace.IsEmpty() {
		keyspace = vs.keyspace
	}
	return vs.vschema.Find(keyspace.String(), tablename.String())
}
