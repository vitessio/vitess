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
	"math"
	"strings"
	"sync"
	"time"

	workflow2 "vitess.io/vitess/go/vt/vtctl/workflow"

	"google.golang.org/protobuf/encoding/prototext"

	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	vtctldvexec "vitess.io/vitess/go/vt/vtctl/workflow/vexec" // renamed to avoid a collision with the vexec struct in this package
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	vexecTableQualifier   = "_vt"
	vreplicationTableName = "vreplication"
	sqlVReplicationDelete = "delete from _vt.vreplication"
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

	primaries []*topo.TabletInfo
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

// VExec executes queries on a table on all primaries in the target keyspace of the workflow
func (wr *Wrangler) VExec(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*sqltypes.Result, error) {
	if wr.VExecFunc != nil {
		return wr.VExecFunc(ctx, workflow, keyspace, query, dryRun)
	}
	results, err := wr.runVexec(ctx, workflow, keyspace, query, dryRun)
	retResults := make(map[*topo.TabletInfo]*sqltypes.Result)
	for tablet, result := range results {
		retResults[tablet] = sqltypes.Proto3ToResult(result)
	}
	return retResults, err
}

// runVexec is the main function that runs a dry or wet execution of 'query' on backend shards.
func (wr *Wrangler) runVexec(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	vx := newVExec(ctx, workflow, keyspace, query, wr)

	if err := vx.getPrimaries(); err != nil {
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

// exec runs our planned query on backend shard primaries. It collects query results from all
// shards and returns an aggregate (UNION ALL -like) result.
func (vx *vexec) exec() (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	results := make(map[*topo.TabletInfo]*querypb.QueryResult)
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(vx.ctx, 10*time.Second)
	defer cancel()
	for _, primary := range vx.primaries {
		wg.Add(1)
		go func(ctx context.Context, primary *topo.TabletInfo) {
			defer wg.Done()
			qr, err := vx.planner.exec(ctx, primary.Alias, vx.plannedQuery)
			if err != nil {
				allErrors.RecordError(err)
			} else {
				// If we deleted a workflow then let's make a best effort attempt to clean
				// up any related data.
				if vx.query == sqlVReplicationDelete {
					vx.wr.deleteWorkflowVDiffData(ctx, primary.Tablet, vx.workflow)
				}
				mu.Lock()
				results[primary] = qr
				mu.Unlock()
			}
		}(ctx, primary)
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

// getPrimaries identifies primary tablet for all shards relevant to our keyspace
func (vx *vexec) getPrimaries() error {
	var err error
	shards, err := vx.wr.ts.GetShardNames(vx.ctx, vx.keyspace)
	if err != nil {
		return err
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards found in keyspace %s", vx.keyspace)
	}
	var allPrimaries []*topo.TabletInfo
	var primary *topo.TabletInfo
	for _, shard := range shards {
		if primary, err = vx.getPrimaryForShard(shard); err != nil {
			return err
		}
		if primary == nil {
			return fmt.Errorf("no primary found for shard %s", shard)
		}
		allPrimaries = append(allPrimaries, primary)
	}
	vx.primaries = allPrimaries
	return nil
}

func (vx *vexec) getPrimaryForShard(shard string) (*topo.TabletInfo, error) {
	si, err := vx.wr.ts.GetShard(vx.ctx, vx.keyspace, shard)
	if err != nil {
		return nil, err
	}
	if si.PrimaryAlias == nil {
		return nil, fmt.Errorf("no primary found for shard %s", shard)
	}
	primary, err := vx.wr.ts.GetTablet(vx.ctx, si.PrimaryAlias)
	if err != nil {
		return nil, err
	}
	if primary == nil {
		return nil, fmt.Errorf("could not get tablet for %s:%s", vx.keyspace, si.PrimaryAlias)
	}
	return primary, nil
}

func (wr *Wrangler) convertQueryResultToSQLTypesResult(results map[*topo.TabletInfo]*querypb.QueryResult) map[*topo.TabletInfo]*sqltypes.Result {
	retResults := make(map[*topo.TabletInfo]*sqltypes.Result)
	for tablet, result := range results {
		retResults[tablet] = sqltypes.Proto3ToResult(result)
	}
	return retResults
}

// WorkflowAction can start/stop/delete or list streams in _vt.vreplication on all primaries in the target keyspace of the workflow.
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
	return wr.convertQueryResultToSQLTypesResult(results), err
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
		query = sqlVReplicationDelete
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

// WorkflowTagAction sets or clears the tags for a workflow in a keyspace
func (wr *Wrangler) WorkflowTagAction(ctx context.Context, keyspace string, workflow string, tags string) (map[*topo.TabletInfo]*sqltypes.Result, error) {
	query := fmt.Sprintf("update _vt.vreplication set tags = %s", encodeString(tags))
	results, err := wr.runVexec(ctx, workflow, keyspace, query, false)
	return wr.convertQueryResultToSQLTypesResult(results), err
}

// ReplicationStatusResult represents the result of trying to get the replication status for a given workflow.
type ReplicationStatusResult struct {
	// Workflow represents the name of the workflow relevant to the related replication statuses.
	Workflow string
	// SourceLocation represents the keyspace and shards that we are vreplicating from.
	SourceLocation ReplicationLocation
	// TargetLocation represents the keyspace and shards that we are vreplicating into.
	TargetLocation ReplicationLocation
	// MaxVReplicationLag represents the lag between the current time and the last time an event was seen from the
	// source shards. This defines the "liveness" of the source streams. This will be high only if one of the source streams
	// is no longer running (say, due to a network partition , primary not being available, or a vstreamer failure)
	// MaxVReplicationTransactionLag (see below) represents the "mysql" replication lag, i.e. how far behind we are in
	// terms of data replication from the source to the target.
	MaxVReplicationLag int64
	// MaxVReplicationTransactionLag represents the lag across all shards, between the current time and the timestamp
	// of the last transaction OR heartbeat timestamp (if there have been no writes to replicate from the source).
	MaxVReplicationTransactionLag int64
	// Frozen is true if this workflow has been deemed complete and is in a limbo "frozen" state (Message=="FROZEN")
	Frozen bool
	// Statuses is a map of <shard>/<primary tablet alias> : ShardReplicationStatus (for the given shard).
	ShardStatuses map[string]*ShardReplicationStatus
	// SourceTimeZone represents the time zone provided to the workflow, only set if not UTC
	SourceTimeZone string
	// TargetTimeZone is set to the original SourceTimeZone, in reverse streams, if it was provided to the workflow
	TargetTimeZone string
}

// ReplicationLocation represents a location that data is either replicating from, or replicating into.
type ReplicationLocation struct {
	Keyspace string
	Shards   []string
}

// ShardReplicationStatus holds relevant vreplication related info for the given shard.
type ShardReplicationStatus struct {
	// PrimaryReplicationStatuses represents all of the replication statuses for the primary tablets in the given shard.
	PrimaryReplicationStatuses []*ReplicationStatus
	// TabletControls represents the tablet controls for the tablets in the shard.
	TabletControls []*topodatapb.Shard_TabletControl
	// PrimaryIsServing indicates whether the primary tablet of the given shard is currently serving write traffic.
	PrimaryIsServing bool
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
	Bls *binlogdatapb.BinlogSource
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
	// TimeHeartbeat represents the time_heartbeat column from the _vt.vreplication table.
	TimeHeartbeat int64
	// TimeThrottled represents the time_throttled column from the _vt.vreplication table.
	TimeThrottled int64
	// ComponentThrottled represents the component_throttled column from the _vt.vreplication table.
	ComponentThrottled string
	// Message represents the message column from the _vt.vreplication table.
	Message string
	// Tags contain the tags specified for this stream
	Tags            string
	WorkflowType    string
	WorkflowSubType string
	// CopyState represents the rows from the _vt.copy_state table.
	CopyState []copyState
	// sourceTimeZone represents the time zone of each stream, only set if not UTC
	sourceTimeZone string
	// targetTimeZone is set to the sourceTimeZone of the forward stream, if it was provided in the workflow
	targetTimeZone string
}

func (wr *Wrangler) getReplicationStatusFromRow(ctx context.Context, row sqltypes.RowNamedValues, primary *topo.TabletInfo) (*ReplicationStatus, string, error) {
	var err error
	var id, timeUpdated, transactionTimestamp, timeHeartbeat, timeThrottled int64
	var state, dbName, pos, stopPos, message, tags, componentThrottled string
	var workflowType, workflowSubType int64
	var bls binlogdatapb.BinlogSource
	var mpos mysql.Position

	id, err = row.ToInt64("id")
	if err != nil {
		return nil, "", err
	}
	rowBytes, err := row.ToBytes("source")
	if err != nil {
		return nil, "", err
	}
	if err := prototext.Unmarshal(rowBytes, &bls); err != nil {
		return nil, "", err
	}

	// gtid in the pos column can be compressed, so check and possibly uncompress
	pos, err = row.ToString("pos")
	if err != nil {
		return nil, "", err
	}
	if pos != "" {
		mpos, err = binlogplayer.DecodePosition(pos)
		if err != nil {
			return nil, "", err
		}
		pos = mpos.String()
	}
	stopPos, err = row.ToString("stop_pos")
	if err != nil {
		return nil, "", err
	}
	state, err = row.ToString("state")
	if err != nil {
		return nil, "", err
	}
	dbName, err = row.ToString("db_name")
	if err != nil {
		return nil, "", err
	}
	timeUpdated, err = row.ToInt64("time_updated")
	if err != nil {
		return nil, "", err
	}
	transactionTimestamp, err = row.ToInt64("transaction_timestamp")
	if err != nil {
		return nil, "", err
	}
	timeHeartbeat, err = row.ToInt64("time_heartbeat")
	if err != nil {
		return nil, "", err
	}
	timeThrottled, err = row.ToInt64("time_throttled")
	if err != nil {
		return nil, "", err
	}
	componentThrottled, err = row.ToString("component_throttled")
	if err != nil {
		return nil, "", err
	}
	message, err = row.ToString("message")
	if err != nil {
		return nil, "", err
	}
	tags, err = row.ToString("tags")
	if err != nil {
		return nil, "", err
	}
	workflowType, _ = row.ToInt64("workflow_type")
	workflowSubType, _ = row.ToInt64("workflow_sub_type")

	status := &ReplicationStatus{
		Shard:                primary.Shard,
		Tablet:               primary.AliasString(),
		ID:                   id,
		Bls:                  &bls,
		Pos:                  pos,
		StopPos:              stopPos,
		State:                state,
		DBName:               dbName,
		TransactionTimestamp: transactionTimestamp,
		TimeUpdated:          timeUpdated,
		TimeHeartbeat:        timeHeartbeat,
		TimeThrottled:        timeThrottled,
		ComponentThrottled:   componentThrottled,
		Message:              message,
		Tags:                 tags,
		sourceTimeZone:       bls.SourceTimeZone,
		targetTimeZone:       bls.TargetTimeZone,
		WorkflowType:         binlogdatapb.VReplicationWorkflowType_name[int32(workflowType)],
		WorkflowSubType:      binlogdatapb.VReplicationWorkflowSubType_name[int32(workflowSubType)],
	}
	status.CopyState, err = wr.getCopyState(ctx, primary, id)
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
	query := `select 
		id,
		source,
		pos,
		stop_pos,
		max_replication_lag,
		state,
		db_name,
		time_updated,
		transaction_timestamp,
		time_heartbeat,
		time_throttled,
		component_throttled,
		message,
		tags,
		workflow_type, 
		workflow_sub_type
	from _vt.vreplication`
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
	for primary, result := range results {
		var rsrStatus []*ReplicationStatus
		nqr := sqltypes.Proto3ToResult(result).Named()
		if len(nqr.Rows) == 0 {
			continue
		}
		for _, row := range nqr.Rows {
			status, sk, err := wr.getReplicationStatusFromRow(ctx, row, primary)
			if err != nil {
				return nil, err
			}
			rsr.SourceTimeZone = status.sourceTimeZone
			rsr.TargetTimeZone = status.targetTimeZone
			sourceKeyspace = sk
			sourceShards.Insert(status.Bls.Shard)
			rsrStatus = append(rsrStatus, status)

			if status.Message == workflow2.Frozen {
				rsr.Frozen = true
			}

			// MaxVReplicationLag is the time since the last event was processed from the source
			// The last event can be an actual binlog event or a heartbeat in case no binlog events occur within (default) 1 second
			timeUpdated := time.Unix(status.TimeUpdated, 0)
			replicationLag := time.Since(timeUpdated)
			if replicationLag.Seconds() > float64(rsr.MaxVReplicationLag) {
				rsr.MaxVReplicationLag = int64(replicationLag.Seconds())
			}

			// MaxVReplicationTransactionLag estimates the actual lag between the source and the target
			// If we are still processing source events it is the difference b/w current time and the timestamp of the last event
			// If heartbeats are more recent than the last event, then the lag is the time since the last heartbeat as
			// there can be an actual event immediately after the heartbeat, but which has not yet
			// been processed on the target
			// We don't allow switching during the copy phase, so in that case we just return a large lag.
			// All timestamps are in seconds since epoch
			lastTransactionTimestamp := status.TransactionTimestamp
			lastHeartbeatTime := status.TimeHeartbeat
			if status.State == "Copying" {
				rsr.MaxVReplicationTransactionLag = math.MaxInt64
			} else {
				if lastTransactionTimestamp == 0 /* no new events after copy */ ||
					lastHeartbeatTime > lastTransactionTimestamp /* no recent transactions, so all caught up */ {

					lastTransactionTimestamp = lastHeartbeatTime
				}
				now := time.Now().Unix() /*seconds since epoch*/
				transactionReplicationLag := now - lastTransactionTimestamp
				if transactionReplicationLag > rsr.MaxVReplicationTransactionLag {
					rsr.MaxVReplicationTransactionLag = transactionReplicationLag
				}
			}
		}
		si, err := wr.ts.GetShard(ctx, keyspace, primary.Shard)
		if err != nil {
			return nil, err
		}
		targetShards.Insert(si.ShardName())
		rsr.ShardStatuses[fmt.Sprintf("%s/%s", primary.Shard, primary.AliasString())] = &ShardReplicationStatus{
			PrimaryReplicationStatuses: rsrStatus,
			TabletControls:             si.TabletControls,
			PrimaryIsServing:           si.IsPrimaryServing,
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
				// Even though we query for distinct, we must de-dup because we query per primary tablet.
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
