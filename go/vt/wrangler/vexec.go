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

	"vitess.io/vitess/go/vt/log"

	"github.com/gogo/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

const (
	vreplicationTableName = "_vt.vreplication"
)

type vexec struct {
	ctx      context.Context
	workflow string
	keyspace string
	query    string

	wr *Wrangler

	plan    *vexecPlan
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

// VExec executes queries on _vt.vreplication on all masters in the target keyspace of the workflow
func (wr *Wrangler) VExec(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*sqltypes.Result, error) {
	results, err := wr.runVexec(ctx, workflow, keyspace, query, dryRun)
	retResults := make(map[*topo.TabletInfo]*sqltypes.Result)
	for tablet, result := range results {
		retResults[tablet] = sqltypes.Proto3ToResult(result)
	}
	return retResults, err
}

func (wr *Wrangler) runVexec(ctx context.Context, workflow, keyspace, query string, dryRun bool) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	vx := newVExec(ctx, workflow, keyspace, query, wr)
	if err := vx.getMasters(); err != nil {
		return nil, err
	}
	if _, err := vx.buildVExecPlan(); err != nil {
		return nil, err
	}
	fullQuery := vx.plan.parsedQuery.Query
	if dryRun {
		return nil, vx.outputDryRunInfo(wr)
	}
	return vx.exec(fullQuery)
}

func (vx *vexec) outputDryRunInfo(wr *Wrangler) error {
	rsr, err := vx.wr.getStreams(vx.ctx, vx.workflow, vx.keyspace)
	if err != nil {
		return err
	}

	wr.Logger().Printf("Query: %s\nwill be run on the following streams in keyspace %s for workflow %s:\n\n",
		vx.plan.parsedQuery.Query, vx.keyspace, vx.workflow)
	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Tablet", "ID", "BinLogSource", "State", "DBName", "Current GTID", "MaxReplicationLag"})
	for _, master := range vx.masters {
		key := fmt.Sprintf("%s/%s", master.Shard, master.AliasString())
		for _, stream := range rsr.Statuses[key] {
			table.Append([]string{key, fmt.Sprintf("%d", stream.ID), stream.Bls.String(), stream.State, stream.DBName, stream.Pos, fmt.Sprintf("%d", stream.MaxReplicationLag)})
		}
	}
	table.SetAutoMergeCellsByColumnIndex([]int{0})
	table.SetRowLine(true)
	table.Render()
	wr.Logger().Printf(tableString.String())
	wr.Logger().Printf("\n\n")

	return nil
}

func (vx *vexec) exec(query string) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	var wg sync.WaitGroup
	workflow := vx.workflow
	allErrors := &concurrency.AllErrorRecorder{}
	results := make(map[*topo.TabletInfo]*querypb.QueryResult)
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(vx.ctx, 10*time.Second)
	defer cancel()
	for _, master := range vx.masters {
		wg.Add(1)
		go func(ctx context.Context, master *topo.TabletInfo) {
			defer wg.Done()
			log.Infof("Running %s on %s\n", query, master.AliasString())
			qr, err := vx.wr.VReplicationExec(ctx, master.Alias, query)
			log.Infof("Result is %s: %v", master.AliasString(), qr)
			if err != nil {
				allErrors.RecordError(err)
			} else {
				if qr.RowsAffected == 0 {
					allErrors.RecordError(fmt.Errorf("\nno matching streams found for workflow %s, tablet %s, query %s", workflow, master.Alias, query))
				} else {
					mu.Lock()
					results[master] = qr
					mu.Unlock()
				}
			}
		}(ctx, master)
	}
	wg.Wait()
	return results, allErrors.AggrError(vterrors.Aggregate)
}

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

// WorkflowAction can start/stop/delete or list strams in _vt.vreplication on all masters in the target keyspace of the workflow
func (wr *Wrangler) WorkflowAction(ctx context.Context, workflow, keyspace, action string, dryRun bool) (map[*topo.TabletInfo]*sqltypes.Result, error) {
	if action == "list" {
		return nil, wr.listStreams(ctx, workflow, keyspace)
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
	var err error
	query, err := wr.getWorkflowActionQuery(action)
	if err != nil {
		return nil, err
	}
	vx := newVExec(ctx, workflow, keyspace, query, wr)
	err = vx.getMasters()
	if err != nil {
		return nil, err
	}
	if _, err := vx.buildVExecPlan(); err != nil {
		return nil, err
	}
	fullQuery := vx.plan.parsedQuery.Query
	if dryRun {
		return nil, vx.outputDryRunInfo(wr)
	}
	results, err := vx.exec(fullQuery)
	return results, err
}

type replicationStatusResult struct {
	Workflow       string
	SourceKeyspace string
	TargetKeyspace string

	Statuses map[string][]*replicationStatus
}

type copyState struct {
	Table  string
	LastPK string
}

type replicationStatus struct {
	Shard             string
	Tablet            string
	ID                int64
	Bls               binlogdatapb.BinlogSource
	Pos               string
	StopPos           string
	State             string
	MaxReplicationLag int64
	DBName            string
	TimeUpdated       int64
	Message           string

	CopyState []copyState
}

func (wr *Wrangler) getReplicationStatusFromRow(ctx context.Context, row []sqltypes.Value, master *topo.TabletInfo) (*replicationStatus, string, error) {
	var err error
	var id, maxReplicationLag, timeUpdated int64
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
	maxReplicationLag, err = evalengine.ToInt64(row[4])
	if err != nil {
		return nil, "", err
	}
	state = row[5].ToString()
	dbName = row[6].ToString()
	timeUpdated, err = evalengine.ToInt64(row[7])
	if err != nil {
		return nil, "", err
	}
	message = row[8].ToString()
	status := &replicationStatus{
		Shard:             master.Shard,
		Tablet:            master.AliasString(),
		ID:                id,
		Bls:               bls,
		Pos:               pos,
		StopPos:           stopPos,
		State:             state,
		DBName:            dbName,
		MaxReplicationLag: maxReplicationLag,
		TimeUpdated:       timeUpdated,
		Message:           message,
	}
	status.CopyState, err = wr.getCopyState(ctx, master, id)
	if err != nil {
		return nil, "", err
	}

	status.State = updateState(message, status.State, status.CopyState, timeUpdated)
	return status, bls.Keyspace, nil
}

func (wr *Wrangler) getStreams(ctx context.Context, workflow, keyspace string) (*replicationStatusResult, error) {
	var rsr replicationStatusResult
	rsr.Statuses = make(map[string][]*replicationStatus)
	rsr.Workflow = workflow
	rsr.TargetKeyspace = keyspace
	var results map[*topo.TabletInfo]*querypb.QueryResult
	query := "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, message from _vt.vreplication"
	results, err := wr.runVexec(ctx, workflow, keyspace, query, false)
	if err != nil {
		return nil, err
	}
	for master, result := range results {
		var rsrStatus []*replicationStatus
		qr := sqltypes.Proto3ToResult(result)
		for _, row := range qr.Rows {
			status, sourceKeyspace, err := wr.getReplicationStatusFromRow(ctx, row, master)
			fmt.Printf("getReplicationStatusFromRow status for master %s is %v\n", master.AliasString(), status)
			if err != nil {
				return nil, err
			}
			rsr.SourceKeyspace = sourceKeyspace

			rsrStatus = append(rsrStatus, status)
		}
		rsr.Statuses[fmt.Sprintf("%s/%s", master.Shard, master.AliasString())] = rsrStatus
	}
	return &rsr, nil
}

func (wr *Wrangler) listStreams(ctx context.Context, workflow, keyspace string) error {
	replStatus, err := wr.getStreams(ctx, workflow, keyspace)
	if err != nil {
		return err
	}
	if err := dumpStreamListAsJSON(replStatus, wr); err != nil {
		return err
	}

	return nil
}

func updateState(message, state string, cs []copyState, timeUpdated int64) string {
	if message != "" {
		state = "Error"
	} else if state == "Running" && len(cs) > 0 {
		state = "Copying"
	} else if state == "Running" && int64(time.Now().Second())-timeUpdated > 10 /* seconds */ {
		state = "Lagging"
	}
	return state
}

func dumpStreamListAsJSON(replStatus *replicationStatusResult, wr *Wrangler) error {
	text, err := json.MarshalIndent(replStatus, "", "\t")
	if err != nil {
		return err
	}
	wr.Logger().Printf("%s\n", text)
	return nil
}

func (wr *Wrangler) getCopyState(ctx context.Context, tablet *topo.TabletInfo, id int64) ([]copyState, error) {
	var cs []copyState
	query := fmt.Sprintf(`select table_name, lastpk from _vt.copy_state where vrepl_id = %d`, id)
	qr, err := wr.VReplicationExec(ctx, tablet.Alias, query)
	if err != nil {
		return nil, err
	}
	if qr != nil {
		for _, row := range qr.Rows {
			table := string(row.Values[0])
			lastPK := string(row.Values[1])
			copyState := copyState{
				Table:  table,
				LastPK: lastPK,
			}
			cs = append(cs, copyState)
		}
	}

	return cs, nil
}
