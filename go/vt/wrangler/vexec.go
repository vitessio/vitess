/*
Copyright 2020 The Vitess Authors.

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

package wrangler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	vtctldvexec "vitess.io/vitess/go/vt/vtctl/workflow/vexec" // renamed to avoid a collision with the vexec struct in this package
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	vexecTableQualifier   = "_vt"
	vreplicationTableName = "vreplication"
)

// vexec is the construct by which we run a query against backend shards. vexec is created by user-facing
// interface, like vtctl or vtgate.
// vexec parses, analyzes and plans th equery, and maintains state of each such step's result.
type vexec struct {
	ctx      context.Context
	workflow string
	keyspace string
	// query is vexec's input
	query string
	// stmt is parsed from the query
	stmt sqlparser.Statement
	// tableName is extracted from the query, and used to determine the plan
	tableName string
	// planner will plan and execute a (possibly rewritten) query on backend shards
	planner vexecPlanner
	// plannedQuery is the result of supplementing original query with extra conditionals
	plannedQuery string

	wr *Wrangler

	masters []*topo.TabletInfo
}

func newVExec(ctx context.Context, workflow, keyspace, query string, wr *Wrangler) *vexec {
	return &vexec{
		ctx:      ctx,
		workflow: workflow,
		keyspace: keyspace,
		query:    query,
		wr:       wr,
	}
}

// QueryResultForRowsAffected aggregates results into row-type results (fields + values)
func (wr *Wrangler) QueryResultForRowsAffected(results map[*topo.TabletInfo]*sqltypes.Result) *sqltypes.Result {
	var qr = &sqltypes.Result{}
	qr.Fields = []*querypb.Field{{
		Name: "Tablet",
		Type: sqltypes.VarBinary,
	}, {
		Name: "RowsAffected",
		Type: sqltypes.Uint64,
	}}
	var row2 []sqltypes.Value
	for tablet, result := range results {
		row2 = nil
		row2 = append(row2, sqltypes.NewVarBinary(tablet.AliasString()))
		row2 = append(row2, sqltypes.NewUint64(result.RowsAffected))
		qr.Rows = append(qr.Rows, row2)
	}
	return qr
}

// QueryResultForTabletResults aggregates given results into a "rows-affected" type result (no row data)
func (wr *Wrangler) QueryResultForTabletResults(results map[*topo.TabletInfo]*sqltypes.Result) *sqltypes.Result {
	var qr = &sqltypes.Result{}
	defaultFields := []*querypb.Field{{
		Name: "Tablet",
		Type: sqltypes.VarBinary,
	}}
	var row2 []sqltypes.Value
	for tablet, result := range results {
		if qr.Fields == nil {
			qr.Fields = append(qr.Fields, defaultFields...)
			qr.Fields = append(qr.Fields, result.Fields...)
		}
		for _, row := range result.Rows {
			row2 = nil
			row2 = append(row2, sqltypes.NewVarBinary(tablet.AliasString()))
			row2 = append(row2, row...)
			qr.Rows = append(qr.Rows, row2)
		}
	}
	return qr
}

// VExecResult runs VExec and the naggregates the results into a single *sqltypes.Result
func (wr *Wrangler) VExecResult(ctx context.Context, workflow, keyspace, query string, dryRun bool) (qr *sqltypes.Result, err error) {

	results, err := wr.VExec(ctx, workflow, keyspace, query, dryRun)
	if err != nil {
		return nil, err
	}
	if dryRun {
		return nil, nil
	}
	var numFields int
	for _, result := range results {
		numFields = len(result.Fields)
		break
	}
	if numFields != 0 {
		qr = wr.QueryResultForTabletResults(results)
	} else {
		qr = wr.QueryResultForRowsAffected(results)
	}
	return qr, nil
}

// VExec executes queries on a table on all masters in the target keyspace of the workflow
func (wr *Wrangler) VExec(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*sqltypes.Result, error) {
	results, err := wr.runVexec(ctx, workflow, keyspace, query, dryRun)
	retResults := make(map[*topo.TabletInfo]*sqltypes.Result)
	for tablet, result := range results {
		retResults[tablet] = sqltypes.Proto3ToResult(result)
	}
	return retResults, err
}

// runVexec is th emain function that runs a dry or wet execution of 'query` on backend shards.
func (wr *Wrangler) runVexec(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	vx := newVExec(ctx, workflow, keyspace, query, wr)

	if err := vx.getMasters(); err != nil {
		return nil, err
	}
	plan, err := vx.parseAndPlan(ctx)
	if err != nil {
		return nil, err
	}
	vx.plannedQuery = plan.parsedQuery.Query
	if dryRun {
		return nil, vx.outputDryRunInfo(ctx)
	}
	return vx.exec()
}

// parseAndPlan parses and analyses the query, then generates a plan
func (vx *vexec) parseAndPlan(ctx context.Context) (plan *vexecPlan, err error) {
	if err := vx.parseQuery(); err != nil {
		return nil, err
	}
	if err := vx.getPlanner(ctx); err != nil {
		return nil, err
	}
	plan, err = vx.buildPlan(ctx)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (vx *vexec) outputDryRunInfo(ctx context.Context) error {
	return vx.planner.dryRun(ctx)
}

// exec runs our planned query on backend shard masters. It collects query results from all
// shards and returns an aggregate (UNION ALL -like) result.
func (vx *vexec) exec() (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	results := make(map[*topo.TabletInfo]*querypb.QueryResult)
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(vx.ctx, 10*time.Second)
	defer cancel()
	for _, master := range vx.masters {
		wg.Add(1)
		go func(ctx context.Context, master *topo.TabletInfo) {
			defer wg.Done()
			log.Infof("Running %s on %s\n", vx.plannedQuery, master.AliasString())
			qr, err := vx.planner.exec(ctx, master.Alias, vx.plannedQuery)
			log.Infof("Result is %s: %v", master.AliasString(), qr)
			if err != nil {
				allErrors.RecordError(err)
			} else {
				mu.Lock()
				results[master] = qr
				mu.Unlock()
			}
		}(ctx, master)
	}
	wg.Wait()
	return results, allErrors.AggrError(vterrors.Aggregate)
}

// parseQuery parses the input query
func (vx *vexec) parseQuery() (err error) {
	if vx.stmt, err = sqlparser.Parse(vx.query); err != nil {
		return err
	}
	if vx.tableName, err = extractTableName(vx.stmt); err != nil {
		return err
	}
	return nil
}

// getMasters identifies master tablet for all shards relevant to our keyspace
func (vx *vexec) getMasters() error {
	var err error
	shards, err := vx.wr.ts.GetShardNames(vx.ctx, vx.keyspace)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards found in keyspace %s", vx.keyspace)
	}
	var allMasters []*topo.TabletInfo
	var master *topo.TabletInfo
	for _, shard := range shards {
		if master, err = vx.getMasterForShard(shard); err != nil {
			return err
		}
		if master == nil {
			return fmt.Errorf("no master found for shard %s", shard)
		}
		allMasters = append(allMasters, master)
	}
	vx.masters = allMasters
	return nil
}

func (vx *vexec) getMasterForShard(shard string) (*topo.TabletInfo, error) {
	si, err := vx.wr.ts.GetShard(vx.ctx, vx.keyspace, shard)
	if err != nil {
		return nil, err
	}
	if si.MasterAlias == nil {
		return nil, fmt.Errorf("no master found for shard %s", shard)
	}
	master, err := vx.wr.ts.GetTablet(vx.ctx, si.MasterAlias)
	if err != nil {
		return nil, err
	}
	if master == nil {
		return nil, fmt.Errorf("could not get tablet for %s:%s", vx.keyspace, si.MasterAlias)
	}
	return master, nil
}

// WorkflowAction can start/stop/delete or list streams in _vt.vreplication on all masters in the target keyspace of the workflow.
func (wr *Wrangler) WorkflowAction(ctx context.Context, workflow, keyspace, action string, dryRun bool) (map[*topo.TabletInfo]*sqltypes.Result, error) {

	if action == "show" {
		replStatus, err := wr.ShowWorkflow(ctx, workflow, keyspace)
		if err != nil {
			return nil, err
		}
		err = dumpStreamListAsJSON(replStatus, wr)
		return nil, err
	} else if action == "listall" {
		workflows, err := wr.ListAllWorkflows(ctx, keyspace, false)
		if err != nil {
			return nil, err
		}
		wr.printWorkflowList(keyspace, workflows)
		return nil, err
	}
	results, err := wr.execWorkflowAction(ctx, workflow, keyspace, action, dryRun)
	retResults := make(map[*topo.TabletInfo]*sqltypes.Result)
	for tablet, result := range results {
		retResults[tablet] = sqltypes.Proto3ToResult(result)
	}
	return retResults, err
}

func (wr *Wrangler) getWorkflowActionQuery(action string) (string, error) {
	var query string
	updateSQL := "update _vt.vreplication set state = %s"
	switch action {
	case "stop":
		query = fmt.Sprintf(updateSQL, encodeString("Stopped"))
	case "start":
		query = fmt.Sprintf(updateSQL, encodeString("Running"))
	case "delete":
		query = "delete from _vt.vreplication"
	default:
		return "", fmt.Errorf("invalid action found: %s", action)
	}
	return query, nil
}

func (wr *Wrangler) execWorkflowAction(ctx context.Context, workflow, keyspace, action string, dryRun bool) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	query, err := wr.getWorkflowActionQuery(action)
	if err != nil {
		return nil, err
	}
	return wr.runVexec(ctx, workflow, keyspace, query, dryRun)
}

// ReplicationStatusResult represents the result of trying to get the replication status for a given workflow.
type ReplicationStatusResult struct {
	// Workflow represents the name of the workflow relevant to the related replication statuses.
	Workflow string
	// SourceLocation represents the keyspace and shards that we are vreplicating from.
	SourceLocation ReplicationLocation
	// TargetLocation represents the keyspace and shards that we are vreplicating into.
	TargetLocation ReplicationLocation
	// MaxVReplicationLag represents the maximum vreplication lag seen across all shards.
	MaxVReplicationLag int64

	// Statuses is a map of <shard>/<master tablet alias> : ShardReplicationStatus (for the given shard).
	ShardStatuses map[string]*ShardReplicationStatus
}

// ReplicationLocation represents a location that data is either replicating from, or replicating into.
type ReplicationLocation struct {
	Keyspace string
	Shards   []string
}

// ShardReplicationStatus holds relevant vreplication related info for the given shard.
type ShardReplicationStatus struct {
	// MasterReplicationStatuses represents all of the replication statuses for the master tablets in the given shard.
	MasterReplicationStatuses []*ReplicationStatus
	// TabletControls represents the tablet controls for the tablets in the shard.
	TabletControls []*topodatapb.Shard_TabletControl
	// MasterIsServing indicates whether the master tablet of the given shard is currently serving write traffic.
	MasterIsServing bool
}

type copyState struct {
	Table  string
	LastPK string
}

// ReplicationStatus includes data from the _vt.vreplication table, along with other useful relevant data.
type ReplicationStatus struct {
	// Shard represents the relevant shard name.
	Shard string
	// Tablet is the tablet alias that the ReplicationStatus came from.
	Tablet string
	// ID represents the id column from the _vt.vreplication table.
	ID int64
	// Bls represents the BinlogSource.
	Bls binlogdatapb.BinlogSource
	// Pos represents the pos column from the _vt.vreplication table.
	Pos string
	// StopPos represents the stop_pos column from the _vt.vreplication table.
	StopPos string
	// State represents the state column from the _vt.vreplication table.
	State string
	// DbName represents the db_name column from the _vt.vreplication table.
	DBName string
	// TransactionTimestamp represents the transaction_timestamp column from the _vt.vreplication table.
	TransactionTimestamp int64
	// TimeUpdated represents the time_updated column from the _vt.vreplication table.
	TimeUpdated int64
	// Message represents the message column from the _vt.vreplication table.
	Message string

	// CopyState represents the rows from the _vt.copy_state table.
	CopyState []copyState
}

func (wr *Wrangler) getReplicationStatusFromRow(ctx context.Context, row []sqltypes.Value, master *topo.TabletInfo) (*ReplicationStatus, string, error) {
	var err error
	var id, timeUpdated, transactionTimestamp int64
	var state, dbName, pos, stopPos, message string
	var bls binlogdatapb.BinlogSource
	id, err = evalengine.ToInt64(row[0])
	if err != nil {
		return nil, "", err
	}
	if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
		return nil, "", err
	}
	pos = row[2].ToString()
	stopPos = row[3].ToString()
	state = row[5].ToString()
	dbName = row[6].ToString()
	timeUpdated, err = evalengine.ToInt64(row[7])
	if err != nil {
		return nil, "", err
	}
	transactionTimestamp, err = evalengine.ToInt64(row[8])
	if err != nil {
		return nil, "", err
	}
	message = row[9].ToString()
	status := &ReplicationStatus{
		Shard:                master.Shard,
		Tablet:               master.AliasString(),
		ID:                   id,
		Bls:                  bls,
		Pos:                  pos,
		StopPos:              stopPos,
		State:                state,
		DBName:               dbName,
		TransactionTimestamp: transactionTimestamp,
		TimeUpdated:          timeUpdated,
		Message:              message,
	}
	status.CopyState, err = wr.getCopyState(ctx, master, id)
	if err != nil {
		return nil, "", err
	}

	status.State = updateState(message, status.State, status.CopyState, timeUpdated)
	return status, bls.Keyspace, nil
}

func (wr *Wrangler) getStreams(ctx context.Context, workflow, keyspace string) (*ReplicationStatusResult, error) {
	var rsr ReplicationStatusResult
	rsr.ShardStatuses = make(map[string]*ShardReplicationStatus)
	rsr.Workflow = workflow
	var results map[*topo.TabletInfo]*querypb.QueryResult
	query := "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, message from _vt.vreplication"
	results, err := wr.runVexec(ctx, workflow, keyspace, query, false)
	if err != nil {
		return nil, err
	}

	// We set a topo timeout since we contact topo for the shard record.
	ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer cancel()
	var sourceKeyspace string
	sourceShards := sets.NewString()
	targetShards := sets.NewString()
	for master, result := range results {
		var rsrStatus []*ReplicationStatus
		qr := sqltypes.Proto3ToResult(result)
		if len(qr.Rows) == 0 {
			continue
		}
		for _, row := range qr.Rows {
			status, sk, err := wr.getReplicationStatusFromRow(ctx, row, master)
			if err != nil {
				return nil, err
			}
			sourceKeyspace = sk
			sourceShards.Insert(status.Bls.Shard)
			rsrStatus = append(rsrStatus, status)

			timeUpdated := time.Unix(status.TimeUpdated, 0)
			replicationLag := time.Since(timeUpdated)
			if replicationLag.Seconds() > float64(rsr.MaxVReplicationLag) {
				rsr.MaxVReplicationLag = int64(replicationLag.Seconds())
			}
		}
		si, err := wr.ts.GetShard(ctx, keyspace, master.Shard)
		if err != nil {
			return nil, err
		}
		targetShards.Insert(si.ShardName())
		rsr.ShardStatuses[fmt.Sprintf("%s/%s", master.Shard, master.AliasString())] = &ShardReplicationStatus{
			MasterReplicationStatuses: rsrStatus,
			TabletControls:            si.TabletControls,
			MasterIsServing:           si.IsMasterServing,
		}
	}
	rsr.SourceLocation = ReplicationLocation{
		Keyspace: sourceKeyspace,
		Shards:   sourceShards.List(),
	}
	rsr.TargetLocation = ReplicationLocation{
		Keyspace: keyspace,
		Shards:   targetShards.List(),
	}

	return &rsr, nil
}

// ListActiveWorkflows will return a list of all active workflows for the given keyspace.
func (wr *Wrangler) ListActiveWorkflows(ctx context.Context, keyspace string) ([]string, error) {
	return wr.ListAllWorkflows(ctx, keyspace, true)
}

// ListAllWorkflows will return a list of all workflows (Running and Stopped) for the given keyspace.
func (wr *Wrangler) ListAllWorkflows(ctx context.Context, keyspace string, active bool) ([]string, error) {
	where := ""
	if active {
		where = " where state <> 'Stopped'"
	}
	query := "select distinct workflow from _vt.vreplication" + where
	vx := vtctldvexec.NewVExec(keyspace, "", wr.ts, wr.tmc)
	results, err := vx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	workflowsSet := sets.NewString()
	for _, result := range results {
		if len(result.Rows) == 0 {
			continue
		}
		qr := sqltypes.Proto3ToResult(result)
		for _, row := range qr.Rows {
			for _, value := range row {
				// Even though we query for distinct, we must de-dup because we query per master tablet.
				workflowsSet.Insert(value.ToString())
			}
		}
	}
	workflows := workflowsSet.List()
	return workflows, nil
}

// ShowWorkflow will return all of the relevant replication related information for the given workflow.
func (wr *Wrangler) ShowWorkflow(ctx context.Context, workflow, keyspace string) (*ReplicationStatusResult, error) {
	replStatus, err := wr.getStreams(ctx, workflow, keyspace)
	if err != nil {
		return nil, err
	}
	if len(replStatus.ShardStatuses) == 0 {
		return nil, fmt.Errorf("no streams found for workflow %s in keyspace %s", workflow, keyspace)
	}

	return replStatus, nil
}

func updateState(message, state string, cs []copyState, timeUpdated int64) string {
	if strings.Contains(strings.ToLower(message), "error") {
		state = "Error"
	} else if state == "Running" && len(cs) > 0 {
		state = "Copying"
	} else if state == "Running" && int64(time.Now().Second())-timeUpdated > 10 /* seconds */ {
		state = "Lagging"
	}
	return state
}

func dumpStreamListAsJSON(replStatus *ReplicationStatusResult, wr *Wrangler) error {
	text, err := json.MarshalIndent(replStatus, "", "\t")
	if err != nil {
		return err
	}
	wr.Logger().Printf("%s\n", text)
	return nil
}

func (wr *Wrangler) printWorkflowList(keyspace string, workflows []string) {
	list := strings.Join(workflows, ", ")
	if list == "" {
		wr.Logger().Printf("No workflows found in keyspace %s\n", keyspace)
		return
	}
	wr.Logger().Printf("Following workflow(s) found in keyspace %s: %v\n", keyspace, list)
}

func (wr *Wrangler) getCopyState(ctx context.Context, tablet *topo.TabletInfo, id int64) ([]copyState, error) {
	var cs []copyState
	query := fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d", id)
	qr, err := wr.VReplicationExec(ctx, tablet.Alias, query)
	if err != nil {
		return nil, err
	}

	result := sqltypes.Proto3ToResult(qr)
	if result != nil {
		for _, row := range result.Rows {
			// These fields are varbinary, but close enough
			table := row[0].ToString()
			lastPK := row[1].ToString()
			copyState := copyState{
				Table:  table,
				LastPK: lastPK,
			}
			cs = append(cs, copyState)
		}
	}

	return cs, nil
}
