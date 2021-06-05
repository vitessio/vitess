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
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/workflow/vexec"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

var (
	// ErrInvalidWorkflow is a catchall error type for conditions that should be
	// impossible when operating on a workflow.
	ErrInvalidWorkflow = errors.New("invalid workflow")
	// ErrMultipleSourceKeyspaces occurs when a workflow somehow has multiple
	// source keyspaces across different shard primaries. This should be
	// impossible.
	ErrMultipleSourceKeyspaces = errors.New("multiple source keyspaces for a single workflow")
	// ErrMultipleTargetKeyspaces occurs when a workflow somehow has multiple
	// target keyspaces across different shard primaries. This should be
	// impossible.
	ErrMultipleTargetKeyspaces = errors.New("multiple target keyspaces for a single workflow")
)

// Server provides an API to work with Vitess workflows, like vreplication
// workflows (MoveTables, Reshard, etc) and schema migration workflows.
//
// NB: This is in alpha, and you probably don't want to depend on it (yet!).
// Currently, it provides only a read-only API to vreplication workflows. Write
// actions on vreplication workflows, and schema migration workflows entirely,
// are not yet supported, but planned.
type Server struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient
}

// NewServer returns a new server instance with the given topo.Server and
// TabletManagerClient.
func NewServer(ts *topo.Server, tmc tmclient.TabletManagerClient) *Server {
	return &Server{
		ts:  ts,
		tmc: tmc,
	}
}

// CheckReshardingJournalExistsOnTablet returns the journal (or an empty
// journal) and a boolean to indicate if the resharding_journal table exists on
// the given tablet.
//
// (TODO:@ajm188) This should not be part of the final public API, and should
// be un-exported after all places in package wrangler that call this have been
// migrated over.
func (s *Server) CheckReshardingJournalExistsOnTablet(ctx context.Context, tablet *topodatapb.Tablet, migrationID int64) (*binlogdatapb.Journal, bool, error) {
	var (
		journal binlogdatapb.Journal
		exists  bool
	)

	query := fmt.Sprintf("select val from _vt.resharding_journal where id=%v", migrationID)
	p3qr, err := s.tmc.VReplicationExec(ctx, tablet, query)
	if err != nil {
		return nil, false, err
	}

	if len(p3qr.Rows) != 0 {
		qr := sqltypes.Proto3ToResult(p3qr)
		if err := prototext.Unmarshal(qr.Rows[0][0].ToBytes(), &journal); err != nil {
			return nil, false, err
		}

		exists = true
	}

	return &journal, exists, nil
}

// GetCellsWithShardReadsSwitched returns the topo cells partitioned into two
// slices: one with the cells where shard reads have been switched for the given
// tablet type and one with the cells where shard reads have not been switched
// for the given tablet type.
//
// This function is for use in Reshard, and "switched reads" is defined as if
// any one of the source shards has the query service disabled in its tablet
// control record.
func (s *Server) GetCellsWithShardReadsSwitched(
	ctx context.Context,
	keyspace string,
	si *topo.ShardInfo,
	tabletType topodatapb.TabletType,
) (cellsSwitched []string, cellsNotSwitched []string, err error) {
	cells, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, nil, err
	}

	for _, cell := range cells {
		srvks, err := s.ts.GetSrvKeyspace(ctx, cell, keyspace)
		if err != nil {
			return nil, nil, err
		}

		// Checking one shard is enough.
		var (
			shardServedTypes []string
			found            bool
			noControls       bool
		)

		for _, partition := range srvks.GetPartitions() {
			if tabletType != partition.GetServedType() {
				continue
			}

			// If reads and writes are both switched it is possible that the
			// shard is not in the partition table.
			for _, shardReference := range partition.GetShardReferences() {
				if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
					found = true
					break
				}
			}

			// It is possible that there are no tablet controls if the target
			// shards are not yet serving, or once reads and writes are both
			// switched.
			if len(partition.GetShardTabletControls()) == 0 {
				noControls = true
				break
			}

			for _, tabletControl := range partition.GetShardTabletControls() {
				if key.KeyRangeEqual(tabletControl.GetKeyRange(), si.GetKeyRange()) {
					if !tabletControl.GetQueryServiceDisabled() {
						shardServedTypes = append(shardServedTypes, si.ShardName())
					}

					break
				}
			}
		}

		if found && (len(shardServedTypes) > 0 || noControls) {
			cellsNotSwitched = append(cellsNotSwitched, cell)
		} else {
			cellsSwitched = append(cellsSwitched, cell)
		}
	}

	return cellsSwitched, cellsNotSwitched, nil
}

// GetCellsWithTableReadsSwitched returns the topo cells partitioned into two
// slices: one with the cells where table reads have been switched for the given
// tablet type and one with the cells where table reads have not been switched
// for the given tablet type.
//
// This function is for use in MoveTables, and "switched reads" is defined as if
// the routing rule for a (table, tablet_type) is pointing to the target
// keyspace.
func (s *Server) GetCellsWithTableReadsSwitched(
	ctx context.Context,
	keyspace string,
	table string,
	tabletType topodatapb.TabletType,
) (cellsSwitched []string, cellsNotSwitched []string, err error) {
	cells, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return nil, nil, err
	}

	getKeyspace := func(ruleTarget string) (string, error) {
		arr := strings.Split(ruleTarget, ".")
		if len(arr) != 2 {
			return "", fmt.Errorf("rule target is not correctly formatted: %s", ruleTarget)
		}

		return arr[0], nil
	}

	for _, cell := range cells {
		srvVSchema, err := s.ts.GetSrvVSchema(ctx, cell)
		if err != nil {
			return nil, nil, err
		}

		var (
			found    bool
			switched bool
		)

		for _, rule := range srvVSchema.RoutingRules.Rules {
			ruleName := fmt.Sprintf("%s.%s@%s", keyspace, table, strings.ToLower(tabletType.String()))
			if rule.FromTable == ruleName {
				found = true

				for _, to := range rule.ToTables {
					ks, err := getKeyspace(to)
					if err != nil {
						log.Errorf(err.Error())
						return nil, nil, err
					}

					if ks == keyspace {
						switched = true
						break // if one table in the workflow switched, we are done.
					}
				}
			}

			if found {
				break
			}
		}

		if switched {
			cellsSwitched = append(cellsSwitched, cell)
		} else {
			cellsNotSwitched = append(cellsNotSwitched, cell)
		}
	}

	return cellsSwitched, cellsNotSwitched, nil
}

// GetWorkflows returns a list of all workflows that exist in a given keyspace,
// with some additional filtering depending on the request parameters (for
// example, ActiveOnly=true restricts the search to only workflows that are
// currently running).
//
// It has the same signature as the vtctlservicepb.VtctldServer's GetWorkflows
// rpc, and grpcvtctldserver delegates to this function.
func (s *Server) GetWorkflows(ctx context.Context, req *vtctldatapb.GetWorkflowsRequest) (*vtctldatapb.GetWorkflowsResponse, error) {
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

	vx := vexec.NewVExec(req.Keyspace, "", s.ts, s.tmc)
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
		if err := prototext.Unmarshal(row[2].ToBytes(), &bls); err != nil {
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

		stream := &vtctldatapb.Workflow_Stream{
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

		stream.CopyStates, err = s.getWorkflowCopyStates(ctx, tablet, id)
		if err != nil {
			return err
		}

		switch {
		case strings.Contains(strings.ToLower(stream.Message), "error"):
			stream.State = "Error"
		case stream.State == "Running" && len(stream.CopyStates) > 0:
			stream.State = "Copying"
		case stream.State == "Running" && int64(time.Now().Second())-timeUpdatedSeconds > 10:
			stream.State = "Lagging"
		}

		shardStreamKey := fmt.Sprintf("%s/%s", tablet.Shard, tablet.AliasString())
		shardStream, ok := workflow.ShardStreams[shardStreamKey]
		if !ok {
			ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
			defer cancel()

			si, err := s.ts.GetShard(ctx, req.Keyspace, tablet.Shard)
			if err != nil {
				return err
			}

			shardStream = &vtctldatapb.Workflow_ShardStream{
				Streams:          nil,
				TabletControls:   si.TabletControls,
				IsPrimaryServing: si.IsMasterServing,
			}

			workflow.ShardStreams[shardStreamKey] = shardStream
		}

		shardStream.Streams = append(shardStream.Streams, stream)
		sourceShardsByWorkflow[workflow.Name].Insert(stream.BinlogSource.Shard)
		targetShardsByWorkflow[workflow.Name].Insert(tablet.Shard)

		if ks, ok := sourceKeyspaceByWorkflow[workflow.Name]; ok && ks != stream.BinlogSource.Keyspace {
			return fmt.Errorf("%w: workflow = %v, ks1 = %v, ks2 = %v", ErrMultipleSourceKeyspaces, workflow.Name, ks, stream.BinlogSource.Keyspace)
		}

		sourceKeyspaceByWorkflow[workflow.Name] = stream.BinlogSource.Keyspace

		if ks, ok := targetKeyspaceByWorkflow[workflow.Name]; ok && ks != tablet.Keyspace {
			return fmt.Errorf("%w: workflow = %v, ks1 = %v, ks2 = %v", ErrMultipleTargetKeyspaces, workflow.Name, ks, tablet.Keyspace)
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
					Name:         workflowName,
					ShardStreams: map[string]*vtctldatapb.Workflow_ShardStream{},
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
			return nil, fmt.Errorf("%w: %s has no source shards", ErrInvalidWorkflow, name)
		}

		sourceKeyspace, ok := sourceKeyspaceByWorkflow[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s has no source keyspace", ErrInvalidWorkflow, name)
		}

		targetShards, ok := targetShardsByWorkflow[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s has no target shards", ErrInvalidWorkflow, name)
		}

		targetKeyspace, ok := targetKeyspaceByWorkflow[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s has no target keyspace", ErrInvalidWorkflow, name)
		}

		maxVReplicationLag, ok := maxVReplicationLagByWorkflow[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s has no tracked vreplication lag", ErrInvalidWorkflow, name)
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

func (s *Server) getWorkflowCopyStates(ctx context.Context, tablet *topo.TabletInfo, id int64) ([]*vtctldatapb.Workflow_Stream_CopyState, error) {
	query := fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d", id)
	qr, err := s.tmc.VReplicationExec(ctx, tablet.Tablet, query)
	if err != nil {
		return nil, err
	}

	result := sqltypes.Proto3ToResult(qr)
	if result == nil {
		return nil, nil
	}

	copyStates := make([]*vtctldatapb.Workflow_Stream_CopyState, len(result.Rows))
	for i, row := range result.Rows {
		// These fields are technically varbinary, but this is close enough.
		copyStates[i] = &vtctldatapb.Workflow_Stream_CopyState{
			Table:  row[0].ToString(),
			LastPk: row[1].ToString(),
		}
	}

	return copyStates, nil
}
