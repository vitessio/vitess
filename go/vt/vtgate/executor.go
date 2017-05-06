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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
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
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var noKeyspaceErr = vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no keyspace in database name specified. Supported database name format: keyspace[:shard][@type]")

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
	e := &Executor{
		serv:        serv,
		cell:        cell,
		resolver:    resolver,
		scatterConn: resolver.scatterConn,
		txConn:      resolver.scatterConn.txConn,
		plans:       cache.NewLRUCache(10000),
		normalize:   normalize,
	}
	e.watchSrvVSchema(ctx, cell)
	executorOnce.Do(func() {
		http.Handle("/debug/query_plans", e)
		http.Handle("/debug/vschema", e)
	})
	return e
}

// Execute executes a non-streaming query.
func (e *Executor) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}

	switch sqlparser.Preview(sql) {
	case sqlparser.StmtSelect:
		return e.handleExec(ctx, session, sql, bindVars)
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		nsf := NewSafeSession(session)
		autocommit := false
		if session.Autocommit && !session.InTransaction {
			autocommit = true
			if err := e.txConn.Begin(ctx, nsf); err != nil {
				return nil, err
			}
			defer e.txConn.Rollback(ctx, nsf)
		}

		qr, err := e.handleExec(ctx, session, sql, bindVars)
		if err != nil {
			return nil, err
		}

		if autocommit {
			if err := e.txConn.Commit(ctx, nsf); err != nil {
				return nil, err
			}
		}
		return qr, nil
	case sqlparser.StmtDDL:
		return e.handleDDL(ctx, session, sql, bindVars)
	case sqlparser.StmtBegin:
		err := e.txConn.Begin(ctx, NewSafeSession(session))
		return &sqltypes.Result{}, err
	case sqlparser.StmtCommit:
		err := e.txConn.Commit(ctx, NewSafeSession(session))
		return &sqltypes.Result{}, err
	case sqlparser.StmtRollback:
		err := e.txConn.Rollback(ctx, NewSafeSession(session))
		return &sqltypes.Result{}, err
	case sqlparser.StmtSet:
		return e.handleSet(ctx, session, sql, bindVars)
	case sqlparser.StmtShow:
		return e.handleShow(ctx, session, sql, bindVars)
	case sqlparser.StmtUse:
		return e.handleUse(ctx, session, sql, bindVars)
	case sqlparser.StmtOther:
		return e.handleOther(ctx, session, sql, bindVars)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized statement: %s", sql)
}

func (e *Executor) handleExec(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	target := parseTarget(session.TargetString)
	if target.Shard != "" {
		// V1 mode.
		sql = sqlannotation.AnnotateIfDML(sql, nil)
		return e.shardExec(ctx, session, sql, bindVars, target)
	}

	// V3 mode.
	query, comments := sqlparser.SplitTrailingComments(sql)
	vcursor := newVCursorImpl(ctx, session, target, comments, e)
	plan, err := e.getPlan(vcursor, query, bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := plan.Instructions.Execute(vcursor, bindVars, make(map[string]interface{}), true)
	// Check if there was partial DML execution. If so, rollback the transaction.
	if err != nil && session.InTransaction && vcursor.hasPartialDML {
		_ = e.txConn.Rollback(ctx, NewSafeSession(session))
		err = vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction rolled back due to partial DML execution: %v", err)
	}
	return qr, err
}

func (e *Executor) shardExec(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}, target querypb.Target) (*sqltypes.Result, error) {
	f := func(keyspace string) (string, []string, error) {
		return keyspace, []string{target.Shard}, nil
	}
	return e.resolver.Execute(ctx, sql, bindVars, target.Keyspace, target.TabletType, session, f, false /* notInTransaction */, session.Options)
}

func (e *Executor) handleDDL(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	target := parseTarget(session.TargetString)
	if target.Keyspace == "" {
		return nil, noKeyspaceErr
	}

	f := func(keyspace string) (string, []string, error) {
		var shards []string
		if target.Shard == "" {
			ks, _, allShards, err := getKeyspaceShards(ctx, e.serv, e.cell, keyspace, target.TabletType)
			if err != nil {
				return "", nil, err
			}
			// The usual keyspace resolution rules are applied.
			// This means that the keyspace can be remapped to a new one
			// if vertical resharding is in progress.
			keyspace = ks
			for _, shard := range allShards {
				shards = append(shards, shard.Name)
			}
		} else {
			shards = []string{target.Shard}
		}
		return keyspace, shards, nil
	}
	return e.resolver.Execute(ctx, sql, bindVars, target.Keyspace, target.TabletType, session, f, false /* notInTransaction */, session.Options)
}

func (e *Executor) handleSet(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	vals, err := sqlparser.ExtractSetValues(sql)
	if err != nil {
		return &sqltypes.Result{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	for k, v := range vals {
		switch k {
		case "autocommit":
			val, ok := v.(int64)
			if !ok {
				return &sqltypes.Result{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for autocommit: %T", v)
			}
			if val != 0 {
				session.Autocommit = true
			} else {
				session.Autocommit = false
			}
		case "transaction_mode":
			val, ok := v.(string)
			if !ok {
				return &sqltypes.Result{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for transaction_mode: %T", v)
			}
			out, ok := vtgatepb.TransactionMode_value[strings.ToUpper(val)]
			if !ok {
				return &sqltypes.Result{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid transaction_mode: %s", val)
			}
			session.TransactionMode = vtgatepb.TransactionMode(out)
		default:
			return &sqltypes.Result{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported construct: %s", sql)
		}
	}
	return &sqltypes.Result{}, nil
}

func (e *Executor) handleShow(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	show, ok := stmt.(*sqlparser.Show)
	if !ok {
		// This code is unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unrecognized SHOW statement: %v", sql)
	}
	target := parseTarget(session.TargetString)

	switch show.Type {
	case sqlparser.ShowDatabasesStr, sqlparser.ShowKeyspacesStr:
		keyspaces, err := getAllKeyspaces(ctx, e.serv, e.cell)
		if err != nil {
			return nil, err
		}

		rows := make([][]sqltypes.Value, len(keyspaces))
		for i, v := range keyspaces {
			rows[i] = buildVarCharRow(v)
		}

		return &sqltypes.Result{
			Fields:       buildVarCharFields("Databases"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case sqlparser.ShowShardsStr:
		keyspaces, err := getAllKeyspaces(ctx, e.serv, e.cell)
		if err != nil {
			return nil, err
		}

		var rows [][]sqltypes.Value
		for _, keyspace := range keyspaces {
			_, _, shards, err := getKeyspaceShards(ctx, e.serv, e.cell, keyspace, target.TabletType)
			if err != nil {
				return nil, err
			}

			for _, shard := range shards {
				rows = append(rows, buildVarCharRow(topoproto.KeyspaceShardString(keyspace, shard.Name)))
			}
		}

		return &sqltypes.Result{
			Fields:       buildVarCharFields("Shards"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	case sqlparser.ShowVSchemaTablesStr:
		if target.Keyspace == "" {
			return nil, noKeyspaceErr
		}
		ks, ok := e.VSchema().Keyspaces[target.Keyspace]
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "keyspace %s not found in vschema", target.Keyspace)
		}

		var tables []string
		for name := range ks.Tables {
			tables = append(tables, name)
		}
		sort.Strings(tables)

		rows := make([][]sqltypes.Value, len(tables))
		for i, v := range tables {
			rows[i] = buildVarCharRow(v)
		}

		return &sqltypes.Result{
			Fields:       buildVarCharFields("Tables"),
			Rows:         rows,
			RowsAffected: uint64(len(rows)),
		}, nil
	}

	// Any other show statement is passed through
	return e.handleOther(ctx, session, sql, bindVars)
}

func (e *Executor) handleUse(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	use, ok := stmt.(*sqlparser.Use)
	if !ok {
		// This code is unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unrecognized USE statement: %v", sql)
	}
	session.TargetString = use.DBName.String()
	return &sqltypes.Result{}, nil
}

func (e *Executor) handleOther(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	target := parseTarget(session.TargetString)
	if target.Keyspace == "" {
		return nil, noKeyspaceErr
	}
	if target.Shard == "" {
		var err error
		target.Keyspace, target.Shard, err = getAnyShard(ctx, e.serv, e.cell, target.Keyspace, target.TabletType)
		if err != nil {
			return nil, err
		}
	}
	return e.shardExec(ctx, session, sql, bindVars, target)
}

// StreamExecute executes a streaming query.
func (e *Executor) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]interface{}, target querypb.Target, callback func(*sqltypes.Result) error) error {
	if bindVars == nil {
		bindVars = make(map[string]interface{})
	}
	query, comments := sqlparser.SplitTrailingComments(sql)
	vcursor := newVCursorImpl(ctx, session, target, comments, e)
	plan, err := e.getPlan(vcursor, query, bindVars)
	if err != nil {
		return err
	}
	return plan.Instructions.StreamExecute(vcursor, bindVars, make(map[string]interface{}), true, callback)
}

// MessageAck acks messages.
func (e *Executor) MessageAck(ctx context.Context, keyspace, name string, ids []*querypb.Value) (int64, error) {
	table, err := e.VSchema().Find(keyspace, name)
	if err != nil {
		return 0, err
	}
	// TODO(sougou): Change this to use Session.
	vcursor := newVCursorImpl(
		ctx,
		&vtgatepb.Session{},
		querypb.Target{
			Keyspace:   table.Keyspace.Name,
			TabletType: topodatapb.TabletType_MASTER,
		},
		"",
		e,
	)
	newKeyspace, _, allShards, err := getKeyspaceShards(ctx, e.serv, e.cell, table.Keyspace.Name, topodatapb.TabletType_MASTER)
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
	return e.scatterConn.MessageAck(ctx, newKeyspace, shardIDs, name)
}

// IsKeyspaceRangeBasedSharded returns true if the keyspace in the vschema is
// marked as sharded.
func (e *Executor) IsKeyspaceRangeBasedSharded(keyspace string) bool {
	vschema := e.VSchema()
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
func (e *Executor) watchSrvVSchema(ctx context.Context, cell string) {
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
			e.mu.Lock()
			if v != nil {
				// no errors, we can save our schema
				e.vschema = vschema
			} else {
				// we had an error, use the empty vschema
				// if we had nothing before.
				if e.vschema == nil {
					e.vschema = vschema
				}
			}
			e.vschemaStats = stats
			e.mu.Unlock()
			e.plans.Clear()

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
			current, changes, _ := e.serv.WatchSrvVSchema(ctx, cell)
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
func (e *Executor) VSchema() *vindexes.VSchema {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.vschema
}

// getPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (e *Executor) getPlan(vcursor *vcursorImpl, sql string, bindVars map[string]interface{}) (*engine.Plan, error) {
	if e.VSchema() == nil {
		return nil, errors.New("vschema not initialized")
	}
	keyspace := vcursor.target.Keyspace
	key := sql
	if keyspace != "" {
		key = keyspace + ":" + sql
	}
	if result, ok := e.plans.Get(key); ok {
		return result.(*engine.Plan), nil
	}
	if !e.normalize {
		plan, err := planbuilder.Build(sql, vcursor)
		if err != nil {
			return nil, err
		}
		e.plans.Set(key, plan)
		return plan, nil
	}
	// Normalize and retry.
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	sqlparser.Normalize(stmt, bindVars, "vtg")
	normalized := sqlparser.String(stmt)
	normkey := normalized
	if keyspace != "" {
		normkey = keyspace + ":" + normalized
	}
	if result, ok := e.plans.Get(normkey); ok {
		return result.(*engine.Plan), nil
	}
	plan, err := planbuilder.BuildFromStmt(normalized, stmt, vcursor)
	if err != nil {
		return nil, err
	}
	e.plans.Set(normkey, plan)
	return plan, nil
}

// ServeHTTP shows the current plans in the query cache.
func (e *Executor) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	if request.URL.Path == "/debug/query_plans" {
		keys := e.plans.Keys()
		response.Header().Set("Content-Type", "text/plain")
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%#v\n", sqlparser.TruncateForUI(v))))
			if plan, ok := e.plans.Peek(v); ok {
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
		b, err := json.MarshalIndent(e.VSchema().Keyspaces, "", " ")
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
func (e *Executor) VSchemaStats() *VSchemaStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.vschemaStats == nil {
		return &VSchemaStats{
			Error: "No VSchema loaded yet.",
		}
	}
	return e.vschemaStats
}

// parseTarget parses the string representation of a Target
// of the form keyspace:shard@tablet_type. You can use a / instead of a :.
func parseTarget(targetString string) querypb.Target {
	// Default tablet type is master.
	target := querypb.Target{
		TabletType: topodatapb.TabletType_MASTER,
	}
	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		// No need to check the error. UNKNOWN will be returned on
		// error and it will fail downstream.
		target.TabletType, _ = topoproto.ParseTabletType(targetString[last+1:])
		targetString = targetString[:last]
	}
	last = strings.LastIndexAny(targetString, "/:")
	if last != -1 {
		target.Shard = targetString[last+1:]
		targetString = targetString[:last]
	}
	target.Keyspace = targetString
	return target
}

func buildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{Name: v, Type: sqltypes.VarChar}
	}
	return fields
}

func buildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.MakeTrusted(sqltypes.VarChar, []byte(v))
	}
	return row
}
