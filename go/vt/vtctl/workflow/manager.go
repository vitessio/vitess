/*
Copyright 2021 The Vitess Authors.

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

package workflow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

type Manager struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient
}

func NewManager(ts *topo.Server, tmc tmclient.TabletManagerClient) *Manager {
	return &Manager{
		ts:  ts,
		tmc: tmc,
	}
}

func (manager *Manager) GetWorkflows(ctx context.Context, req *vtctldatapb.GetWorkflowsRequest) (*vtctldatapb.GetWorkflowsResponse, error) {
	where := ""
	if req.ActiveOnly {
		where = "WHERE state <> 'Stopped'"
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			workflow,
			source,
			pos,
			stop_pos,
			max_replication_lag,
			state,
			db_name,
			time_updated,
			transaction_timestamp,
			message
		FROM
			_vt.vreplication
		%s`,
		where,
	)

	vx := NewVExec(req.Keyspace, manager.ts, manager.tmc)
	results, err := vx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	workflowsMap := make(map[string]*vtctldatapb.Workflow, len(results))
	sourceKeyspaceByWorkflow := make(map[string]string, len(results))
	sourceShardsByWorkflow := make(map[string]sets.String, len(results))
	targetKeyspaceByWorkflow := make(map[string]string, len(results))
	targetShardsByWorkflow := make(map[string]sets.String, len(results))
	maxVReplicationLagByWorkflow := make(map[string]float64, len(results))

	// We guarantee the following invariants when this function is called for a
	// given workflow:
	// - workflow.Name != "" (more precisely, ".Name is set 'properly'")
	// - workflowsMap[workflow.Name] == workflow
	// - sourceShardsByWorkflow[workflow.Name] != nil
	// - targetShardsByWorkflow[workflow.Name] != nil
	// - workflow.ShardStatuses != nil
	scanWorkflow := func(ctx context.Context, workflow *vtctldatapb.Workflow, row []sqltypes.Value, tablet *topo.TabletInfo) error {
		id, err := evalengine.ToInt64(row[0])
		if err != nil {
			return err
		}

		var bls binlogdatapb.BinlogSource
		if err := proto.UnmarshalText(row[2].ToString(), &bls); err != nil {
			return err
		}

		pos := row[3].ToString()
		stopPos := row[4].ToString()
		state := row[6].ToString()
		dbName := row[7].ToString()

		timeUpdatedSeconds, err := evalengine.ToInt64(row[8])
		if err != nil {
			return err
		}

		transactionTimeSeconds, err := evalengine.ToInt64(row[9])
		if err != nil {
			return err
		}

		message := row[10].ToString()

		status := &vtctldatapb.Workflow_ReplicationStatus{
			Id:           id,
			Shard:        tablet.Shard,
			Tablet:       tablet.Alias,
			BinlogSource: &bls,
			Position:     pos,
			StopPosition: stopPos,
			State:        state,
			DbName:       dbName,
			TransactionTimestamp: &vttime.Time{
				Seconds: transactionTimeSeconds,
			},
			TimeUpdated: &vttime.Time{
				Seconds: timeUpdatedSeconds,
			},
			Message: message,
		}

		status.CopyStates, err = manager.getWorkflowCopyStates(ctx, tablet, id)
		if err != nil {
			return err
		}

		switch {
		case strings.Contains(strings.ToLower(status.Message), "error"):
			status.State = "Error"
		case status.State == "Running" && len(status.CopyStates) > 0:
			status.State = "Copying"
		case status.State == "Running" && int64(time.Now().Second())-timeUpdatedSeconds > 10:
			status.State = "Lagging"
		}

		shardStatusKey := fmt.Sprintf("%s/%s", tablet.Shard, tablet.AliasString())
		shardStatus, ok := workflow.ShardStatuses[shardStatusKey]
		if !ok {
			ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
			defer cancel()

			si, err := manager.ts.GetShard(ctx, req.Keyspace, tablet.Shard)
			if err != nil {
				return err
			}

			shardStatus = &vtctldatapb.Workflow_ShardReplicationStatus{
				PrimaryReplicationStatuses: nil,
				TabletControls:             si.TabletControls,
				IsPrimaryServing:           si.IsMasterServing,
			}

			workflow.ShardStatuses[shardStatusKey] = shardStatus
		}

		shardStatus.PrimaryReplicationStatuses = append(shardStatus.PrimaryReplicationStatuses, status)
		sourceShardsByWorkflow[workflow.Name].Insert(status.BinlogSource.Shard)
		targetShardsByWorkflow[workflow.Name].Insert(tablet.Shard)

		if ks, ok := sourceKeyspaceByWorkflow[workflow.Name]; ok && ks != status.BinlogSource.Keyspace {
			// error, this is impossible
		}

		sourceKeyspaceByWorkflow[workflow.Name] = status.BinlogSource.Keyspace

		if ks, ok := targetKeyspaceByWorkflow[workflow.Name]; ok && ks != tablet.Keyspace {
			// error, this is impossible
		}

		targetKeyspaceByWorkflow[workflow.Name] = tablet.Keyspace

		timeUpdated := time.Unix(timeUpdatedSeconds, 0)
		vreplicationLag := time.Since(timeUpdated)

		if currentMaxLag, ok := maxVReplicationLagByWorkflow[workflow.Name]; ok {
			if vreplicationLag.Seconds() > currentMaxLag {
				maxVReplicationLagByWorkflow[workflow.Name] = vreplicationLag.Seconds()
			}
		} else {
			maxVReplicationLagByWorkflow[workflow.Name] = vreplicationLag.Seconds()
		}

		return nil
	}

	for tablet, result := range results {
		qr := sqltypes.Proto3ToResult(result)

		// In the old implementation, we knew we had at most one (0 <= N <= 1)
		// workflow for each shard primary we queried. There might be multiple
		// rows (streams) comprising that workflow, so we would aggregate the
		// rows for a given primary into a single value ("the workflow",
		// ReplicationStatusResult in the old types).
		//
		// In this version, we have many (N >= 0) workflows for each shard
		// primary we queried, so we need to determine if each row corresponds
		// to a workflow we're already aggregating, or if it's a workflow we
		// haven't seen yet for that shard primary. We use the workflow name to
		// dedupe for this.
		for _, row := range qr.Rows {
			workflowName := row[1].ToString()
			workflow, ok := workflowsMap[workflowName]
			if !ok {
				workflow = &vtctldatapb.Workflow{
					Name:          workflowName,
					ShardStatuses: map[string]*vtctldatapb.Workflow_ShardReplicationStatus{},
				}

				workflowsMap[workflowName] = workflow
				sourceShardsByWorkflow[workflowName] = sets.NewString()
				targetShardsByWorkflow[workflowName] = sets.NewString()
			}

			if err := scanWorkflow(ctx, workflow, row, tablet); err != nil {
				return nil, err
			}
		}
	}

	workflows := make([]*vtctldatapb.Workflow, 0, len(workflowsMap))

	for name, workflow := range workflowsMap {
		sourceShards, ok := sourceShardsByWorkflow[name]
		if !ok {
			// error
		}

		sourceKeyspace, ok := sourceKeyspaceByWorkflow[name]

		targetShards, ok := targetShardsByWorkflow[name]
		if !ok {
			// error
		}

		targetKeyspace, ok := targetKeyspaceByWorkflow[name]

		maxVReplicationLag, ok := maxVReplicationLagByWorkflow[name]
		if !ok {
			// error
		}

		workflow.Source = &vtctldatapb.Workflow_ReplicationLocation{
			Keyspace: sourceKeyspace,
			Shards:   sourceShards.List(),
		}

		workflow.Target = &vtctldatapb.Workflow_ReplicationLocation{
			Keyspace: targetKeyspace,
			Shards:   targetShards.List(),
		}

		workflow.MaxVReplicationLag = int64(maxVReplicationLag)

		workflows = append(workflows, workflow)
	}

	return &vtctldatapb.GetWorkflowsResponse{
		Workflows: workflows,
	}, nil
}

func (manager *Manager) getWorkflowCopyStates(ctx context.Context, tablet *topo.TabletInfo, id int64) ([]*vtctldatapb.Workflow_ReplicationStatus_CopyState, error) {
	query := fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d", id)
	qr, err := manager.tmc.VReplicationExec(ctx, tablet.Tablet, query)
	if err != nil {
		return nil, err
	}

	result := sqltypes.Proto3ToResult(qr)
	if result == nil {
		return nil, nil
	}

	copyStates := make([]*vtctldatapb.Workflow_ReplicationStatus_CopyState, len(result.Rows))
	for i, row := range result.Rows {
		// These fields are technically varbinary, but this is close enough.
		copyStates[i] = &vtctldatapb.Workflow_ReplicationStatus_CopyState{
			Table:  row[0].ToString(),
			LastPk: row[1].ToString(),
		}
	}

	return copyStates, nil
}
