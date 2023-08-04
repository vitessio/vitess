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
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow/vexec"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

// TableCopyProgress stores the row counts and disk sizes of the source and target tables
type TableCopyProgress struct {
	TargetRowCount, TargetTableSize int64
	SourceRowCount, SourceTableSize int64
}

// CopyProgress stores the TableCopyProgress for all tables still being copied
type CopyProgress map[string]*TableCopyProgress

const (
	cannotSwitchError               = "workflow has errors"
	cannotSwitchCopyIncomplete      = "copy is still in progress"
	cannotSwitchHighLag             = "replication lag %ds is higher than allowed lag %ds"
	cannotSwitchFailedTabletRefresh = "could not refresh all of the tablets involved in the operation:\n%s"
	cannotSwitchFrozen              = "workflow is frozen"

	// Number of LOCK TABLES cycles to perform on the sources during SwitchWrites.
	lockTablesCycles = 2
	// Time to wait between LOCK TABLES cycles on the sources during SwitchWrites.
	lockTablesCycleDelay = time.Duration(100 * time.Millisecond)

	// Default duration used for lag, timeout, etc.
	defaultDuration = 30 * time.Second
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
	ErrMultipleTargetKeyspaces   = errors.New("multiple target keyspaces for a single workflow")
	ErrWorkflowNotFullySwitched  = errors.New("cannot complete workflow because you have not yet switched all read and write traffic")
	ErrWorkflowPartiallySwitched = errors.New("cannot cancel workflow because you have already switched some or all read and write traffic")
)

// Server provides an API to work with Vitess workflows, like vreplication
// workflows (MoveTables, Reshard, etc) and schema migration workflows.
type Server struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient
	// Limt the number of concurrent background goroutines if needed.
	sem *semaphore.Weighted
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
		qrBytes, err := qr.Rows[0][0].ToBytes()
		if err != nil {
			return nil, false, err
		}
		if err := prototext.Unmarshal(qrBytes, &journal); err != nil {
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

func (s *Server) GetWorkflow(ctx context.Context, keyspace, workflow string) (*vtctldatapb.Workflow, error) {
	res, err := s.GetWorkflows(ctx, &vtctldatapb.GetWorkflowsRequest{
		Keyspace: keyspace,
		Workflow: workflow,
	})
	if err != nil {
		return nil, err
	}
	if len(res.Workflows) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected number of workflows returned; expected 1, got %d", len(res.Workflows))
	}
	return res.Workflows[0], nil
}

// GetWorkflows returns a list of all workflows that exist in a given keyspace,
// with some additional filtering depending on the request parameters (for
// example, ActiveOnly=true restricts the search to only workflows that are
// currently running).
//
// It has the same signature as the vtctlservicepb.VtctldServer's GetWorkflows
// rpc, and grpcvtctldserver delegates to this function.
func (s *Server) GetWorkflows(ctx context.Context, req *vtctldatapb.GetWorkflowsRequest) (*vtctldatapb.GetWorkflowsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.GetWorkflows")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("active_only", req.ActiveOnly)

	where := ""
	predicates := []string{}
	if req.ActiveOnly {
		predicates = append(predicates, "state <> 'Stopped'")
	}
	if req.Workflow != "" {
		predicates = append(predicates, fmt.Sprintf("workflow = '%s'", req.Workflow))
	}
	if len(predicates) > 0 {
		where = fmt.Sprintf("WHERE %s", strings.Join(predicates, " AND "))
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
			message,
			tags,
			workflow_type,
			workflow_sub_type
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

	m := sync.Mutex{} // guards access to the following maps during concurrent calls to scanWorkflow
	workflowsMap := make(map[string]*vtctldatapb.Workflow, len(results))
	sourceKeyspaceByWorkflow := make(map[string]string, len(results))
	sourceShardsByWorkflow := make(map[string]sets.Set[string], len(results))
	targetKeyspaceByWorkflow := make(map[string]string, len(results))
	targetShardsByWorkflow := make(map[string]sets.Set[string], len(results))
	maxVReplicationLagByWorkflow := make(map[string]float64, len(results))

	// We guarantee the following invariants when this function is called for a
	// given workflow:
	// - workflow.Name != "" (more precisely, ".Name is set 'properly'")
	// - workflowsMap[workflow.Name] == workflow
	// - sourceShardsByWorkflow[workflow.Name] != nil
	// - targetShardsByWorkflow[workflow.Name] != nil
	// - workflow.ShardStatuses != nil
	scanWorkflow := func(ctx context.Context, workflow *vtctldatapb.Workflow, row sqltypes.RowNamedValues, tablet *topo.TabletInfo) error {
		span, ctx := trace.NewSpan(ctx, "workflow.Server.scanWorkflow")
		defer span.Finish()

		span.Annotate("keyspace", req.Keyspace)
		span.Annotate("shard", tablet.Shard)
		span.Annotate("active_only", req.ActiveOnly)
		span.Annotate("workflow", workflow.Name)
		span.Annotate("tablet_alias", tablet.AliasString())

		id, err := evalengine.ToInt64(row["id"])
		if err != nil {
			return err
		}

		var bls binlogdatapb.BinlogSource
		rowBytes, err := row["source"].ToBytes()
		if err != nil {
			return err
		}
		if err := prototext.Unmarshal(rowBytes, &bls); err != nil {
			return err
		}

		pos := row["pos"].ToString()
		stopPos := row["stop_pos"].ToString()
		state := row["state"].ToString()
		dbName := row["db_name"].ToString()

		timeUpdatedSeconds, err := evalengine.ToInt64(row["time_updated"])
		if err != nil {
			return err
		}

		transactionTimeSeconds, err := evalengine.ToInt64(row["transaction_timestamp"])
		if err != nil {
			return err
		}

		message := row["message"].ToString()

		tags := row["tags"].ToString()
		var tagArray []string
		if tags != "" {
			tagArray = strings.Split(tags, ",")
		}
		workflowType, _ := row["workflow_type"].ToInt32()
		workflowSubType, _ := row["workflow_sub_type"].ToInt32()
		stream := &vtctldatapb.Workflow_Stream{
			Id:           id,
			Shard:        tablet.Shard,
			Tablet:       tablet.Alias,
			BinlogSource: &bls,
			Position:     pos,
			StopPosition: stopPos,
			State:        state,
			DbName:       dbName,
			TransactionTimestamp: &vttimepb.Time{
				Seconds: transactionTimeSeconds,
			},
			TimeUpdated: &vttimepb.Time{
				Seconds: timeUpdatedSeconds,
			},
			Message: message,
			Tags:    tagArray,
		}
		workflow.WorkflowType = binlogdatapb.VReplicationWorkflowType_name[workflowType]
		workflow.WorkflowSubType = binlogdatapb.VReplicationWorkflowSubType_name[workflowSubType]
		stream.CopyStates, err = s.getWorkflowCopyStates(ctx, tablet, id)
		if err != nil {
			return err
		}

		span.Annotate("num_copy_states", len(stream.CopyStates))

		switch {
		case strings.Contains(strings.ToLower(stream.Message), "error"):
			stream.State = binlogdatapb.VReplicationWorkflowState_Error.String()
		case stream.State == binlogdatapb.VReplicationWorkflowState_Running.String() && len(stream.CopyStates) > 0:
			stream.State = binlogdatapb.VReplicationWorkflowState_Copying.String()
		case stream.State == binlogdatapb.VReplicationWorkflowState_Running.String() && int64(time.Now().Second())-timeUpdatedSeconds > 10:
			stream.State = binlogdatapb.VReplicationWorkflowState_Lagging.String()
		}

		// At this point, we're going to start modifying the maps defined
		// outside this function, as well as fields on the passed-in Workflow
		// pointer. Since we're running concurrently, take the lock.
		//
		// We've already made the remote call to getCopyStates, so synchronizing
		// here shouldn't hurt too badly, performance-wise.
		m.Lock()
		defer m.Unlock()

		shardStreamKey := fmt.Sprintf("%s/%s", tablet.Shard, tablet.AliasString())
		shardStream, ok := workflow.ShardStreams[shardStreamKey]
		if !ok {
			ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
			defer cancel()

			si, err := s.ts.GetShard(ctx, req.Keyspace, tablet.Shard)
			if err != nil {
				return err
			}

			shardStream = &vtctldatapb.Workflow_ShardStream{
				Streams:          nil,
				TabletControls:   si.TabletControls,
				IsPrimaryServing: si.IsPrimaryServing,
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

	var (
		scanWorkflowWg     sync.WaitGroup
		scanWorkflowErrors concurrency.FirstErrorRecorder
	)

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
		for _, row := range qr.Named().Rows {
			workflowName := row["workflow"].ToString()
			workflow, ok := workflowsMap[workflowName]
			if !ok {
				workflow = &vtctldatapb.Workflow{
					Name:         workflowName,
					ShardStreams: map[string]*vtctldatapb.Workflow_ShardStream{},
				}

				workflowsMap[workflowName] = workflow
				sourceShardsByWorkflow[workflowName] = sets.New[string]()
				targetShardsByWorkflow[workflowName] = sets.New[string]()
			}

			scanWorkflowWg.Add(1)
			go func(ctx context.Context, workflow *vtctldatapb.Workflow, row sqltypes.RowNamedValues, tablet *topo.TabletInfo) {
				defer scanWorkflowWg.Done()
				if err := scanWorkflow(ctx, workflow, row, tablet); err != nil {
					scanWorkflowErrors.RecordError(err)
				}
			}(ctx, workflow, row, tablet)
		}
	}

	scanWorkflowWg.Wait()
	if scanWorkflowErrors.HasErrors() {
		return nil, scanWorkflowErrors.Error()
	}

	var (
		fetchLogsWG  sync.WaitGroup
		vrepLogQuery = strings.TrimSpace(`
SELECT
	id,
	vrepl_id,
	type,
	state,
	message,
	created_at,
	updated_at,
	count
FROM
	_vt.vreplication_log
ORDER BY
	vrepl_id ASC,
	id ASC
`)
	)

	fetchStreamLogs := func(ctx context.Context, workflow *vtctldatapb.Workflow) {
		span, ctx := trace.NewSpan(ctx, "workflow.Server.scanWorkflow")
		defer span.Finish()

		span.Annotate("keyspace", req.Keyspace)
		span.Annotate("workflow", workflow.Name)

		results, err := vx.WithWorkflow(workflow.Name).QueryContext(ctx, vrepLogQuery)
		if err != nil {
			// Note that we do not return here. If there are any query results
			// in the map (i.e. some tablets returned successfully), we will
			// still try to read log rows from them on a best-effort basis. But,
			// we will also pre-emptively record the top-level fetch error on
			// every stream in every shard in the workflow. Further processing
			// below may override the error message for certain streams.
			for _, streams := range workflow.ShardStreams {
				for _, stream := range streams.Streams {
					stream.LogFetchError = err.Error()
				}
			}
		}

		for target, p3qr := range results {
			qr := sqltypes.Proto3ToResult(p3qr)
			shardStreamKey := fmt.Sprintf("%s/%s", target.Shard, target.AliasString())

			ss, ok := workflow.ShardStreams[shardStreamKey]
			if !ok || ss == nil {
				continue
			}

			streams := ss.Streams
			streamIdx := 0
			markErrors := func(err error) {
				if streamIdx >= len(streams) {
					return
				}

				streams[streamIdx].LogFetchError = err.Error()
			}

			for _, row := range qr.Rows {
				id, err := evalengine.ToInt64(row[0])
				if err != nil {
					markErrors(err)
					continue
				}

				streamID, err := evalengine.ToInt64(row[1])
				if err != nil {
					markErrors(err)
					continue
				}

				typ := row[2].ToString()
				state := row[3].ToString()
				message := row[4].ToString()

				createdAt, err := time.Parse("2006-01-02 15:04:05", row[5].ToString())
				if err != nil {
					markErrors(err)
					continue
				}

				updatedAt, err := time.Parse("2006-01-02 15:04:05", row[6].ToString())
				if err != nil {
					markErrors(err)
					continue
				}

				count, err := evalengine.ToInt64(row[7])
				if err != nil {
					markErrors(err)
					continue
				}

				streamLog := &vtctldatapb.Workflow_Stream_Log{
					Id:       id,
					StreamId: streamID,
					Type:     typ,
					State:    state,
					CreatedAt: &vttimepb.Time{
						Seconds: createdAt.Unix(),
					},
					UpdatedAt: &vttimepb.Time{
						Seconds: updatedAt.Unix(),
					},
					Message: message,
					Count:   count,
				}

				// Earlier, in the main loop where we called scanWorkflow for
				// each _vt.vreplication row, we also sorted each ShardStreams
				// slice by ascending id, and our _vt.vreplication_log query
				// ordered by (stream_id ASC, id ASC), so we can walk the
				// streams in index order in O(n) amortized over all the rows
				// for this tablet.
				for streamIdx < len(streams) {
					stream := streams[streamIdx]
					if stream.Id < streamLog.StreamId {
						streamIdx++
						continue
					}

					if stream.Id > streamLog.StreamId {
						log.Warningf("Found stream log for nonexistent stream: %+v", streamLog)
						break
					}

					// stream.Id == streamLog.StreamId
					stream.Logs = append(stream.Logs, streamLog)
					break
				}
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
			Shards:   sets.List(sourceShards),
		}

		workflow.Target = &vtctldatapb.Workflow_ReplicationLocation{
			Keyspace: targetKeyspace,
			Shards:   sets.List(targetShards),
		}

		workflow.MaxVReplicationLag = int64(maxVReplicationLag)

		// Sort shard streams by stream_id ASC, to support an optimization
		// in fetchStreamLogs below.
		for _, shardStreams := range workflow.ShardStreams {
			sort.Slice(shardStreams.Streams, func(i, j int) bool {
				return shardStreams.Streams[i].Id < shardStreams.Streams[j].Id
			})
		}

		workflows = append(workflows, workflow)

		// Fetch logs for all streams associated with this workflow in the background.
		fetchLogsWG.Add(1)
		go func(ctx context.Context, workflow *vtctldatapb.Workflow) {
			defer fetchLogsWG.Done()
			fetchStreamLogs(ctx, workflow)
		}(ctx, workflow)
	}

	// Wait for all the log fetchers to finish.
	fetchLogsWG.Wait()

	return &vtctldatapb.GetWorkflowsResponse{
		Workflows: workflows,
	}, nil
}

func (s *Server) getWorkflowState(ctx context.Context, targetKeyspace, workflowName string) (*trafficSwitcher, *State, error) {
	ts, err := s.buildTrafficSwitcher(ctx, targetKeyspace, workflowName)

	if err != nil {
		log.Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, nil, err
	}

	state := &State{
		Workflow:           workflowName,
		SourceKeyspace:     ts.SourceKeyspaceName(),
		TargetKeyspace:     targetKeyspace,
		IsPartialMigration: ts.isPartialMigration,
	}

	var (
		reverse        bool
		sourceKeyspace string
	)

	// We reverse writes by using the source_keyspace.workflowname_reverse workflow
	// spec, so we need to use the source of the reverse workflow, which is the
	// target of the workflow initiated by the user for checking routing rules.
	// Similarly we use a target shard of the reverse workflow as the original
	// source to check if writes have been switched.
	if strings.HasSuffix(workflowName, "_reverse") {
		reverse = true
		// Flip the source and target keyspaces.
		sourceKeyspace = state.TargetKeyspace
		targetKeyspace = state.SourceKeyspace
		workflowName = ReverseWorkflowName(workflowName)
	} else {
		sourceKeyspace = state.SourceKeyspace
	}
	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		state.WorkflowType = TypeMoveTables

		// We assume a consistent state, so only choose routing rule for one table.
		if len(ts.Tables()) == 0 {
			return nil, nil, fmt.Errorf("no tables in workflow %s.%s", targetKeyspace, workflowName)

		}
		table := ts.Tables()[0]

		if ts.isPartialMigration { // shard level traffic switching is all or nothing
			shardRoutingRules, err := s.ts.GetShardRoutingRules(ctx)
			if err != nil {
				return nil, nil, err
			}

			rules := shardRoutingRules.Rules
			for _, rule := range rules {
				switch rule.ToKeyspace {
				case sourceKeyspace:
					state.ShardsNotYetSwitched = append(state.ShardsNotYetSwitched, rule.Shard)
				case targetKeyspace:
					state.ShardsAlreadySwitched = append(state.ShardsAlreadySwitched, rule.Shard)
				default:
					// Not a relevant rule.
				}
			}
		} else {
			state.RdonlyCellsSwitched, state.RdonlyCellsNotSwitched, err = s.GetCellsWithTableReadsSwitched(ctx, targetKeyspace, table, topodatapb.TabletType_RDONLY)
			if err != nil {
				return nil, nil, err
			}

			state.ReplicaCellsSwitched, state.ReplicaCellsNotSwitched, err = s.GetCellsWithTableReadsSwitched(ctx, targetKeyspace, table, topodatapb.TabletType_REPLICA)
			if err != nil {
				return nil, nil, err
			}
			globalRules, err := topotools.GetRoutingRules(ctx, ts.TopoServer())
			if err != nil {
				return nil, nil, err
			}
			for _, table := range ts.Tables() {
				rr := globalRules[table]
				// If a rule exists for the table and points to the target keyspace, then
				// writes have been switched.
				if len(rr) > 0 && rr[0] == fmt.Sprintf("%s.%s", targetKeyspace, table) {
					state.WritesSwitched = true
					break
				}
			}
		}
	} else {
		state.WorkflowType = TypeReshard

		// We assume a consistent state, so only choose one shard.
		var shard *topo.ShardInfo
		if reverse {
			shard = ts.TargetShards()[0]
		} else {
			shard = ts.SourceShards()[0]
		}

		state.RdonlyCellsSwitched, state.RdonlyCellsNotSwitched, err = s.GetCellsWithShardReadsSwitched(ctx, targetKeyspace, shard, topodatapb.TabletType_RDONLY)
		if err != nil {
			return nil, nil, err
		}

		state.ReplicaCellsSwitched, state.ReplicaCellsNotSwitched, err = s.GetCellsWithShardReadsSwitched(ctx, targetKeyspace, shard, topodatapb.TabletType_REPLICA)
		if err != nil {
			return nil, nil, err
		}

		if !shard.IsPrimaryServing {
			state.WritesSwitched = true
		}
	}

	return ts, state, nil
}

func (s *Server) getWorkflowCopyStates(ctx context.Context, tablet *topo.TabletInfo, id int64) ([]*vtctldatapb.Workflow_Stream_CopyState, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.getWorkflowCopyStates")
	defer span.Finish()

	span.Annotate("keyspace", tablet.Keyspace)
	span.Annotate("shard", tablet.Shard)
	span.Annotate("tablet_alias", tablet.AliasString())
	span.Annotate("vrepl_id", id)

	query := fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d and id in (select max(id) from _vt.copy_state where vrepl_id = %d group by vrepl_id, table_name)", id, id)
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

// MoveTablesCreate is part of the vtctlservicepb.VtctldServer interface.
// It passes the embedded TabletRequest object to the given keyspace's
// target primary tablets that will be executing the workflow.
func (s *Server) MoveTablesCreate(ctx context.Context, req *vtctldatapb.MoveTablesCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.MoveTablesCreate")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("cells", req.Cells)
	span.Annotate("tablet_types", req.TabletTypes)
	span.Annotate("on_ddl", req.OnDdl)

	sourceKeyspace := req.SourceKeyspace
	targetKeyspace := req.TargetKeyspace
	//FIXME validate tableSpecs, allTables, excludeTables
	var (
		tables       = req.IncludeTables
		externalTopo *topo.Server
		sourceTopo   *topo.Server = s.ts
		err          error
	)

	// When the source is an external cluster mounted using the Mount command.
	if req.ExternalClusterName != "" {
		externalTopo, err = s.ts.OpenExternalVitessClusterServer(ctx, req.ExternalClusterName)
		if err != nil {
			return nil, err
		}
		sourceTopo = externalTopo
		log.Infof("Successfully opened external topo: %+v", externalTopo)
	}

	var vschema *vschemapb.Keyspace
	vschema, err = s.ts.GetVSchema(ctx, targetKeyspace)
	if err != nil {
		return nil, err
	}
	if vschema == nil {
		return nil, fmt.Errorf("no vschema found for target keyspace %s", targetKeyspace)
	}
	ksTables, err := getTablesInKeyspace(ctx, sourceTopo, s.tmc, sourceKeyspace)
	if err != nil {
		return nil, err
	}
	if len(tables) > 0 {
		err = s.validateSourceTablesExist(ctx, sourceKeyspace, ksTables, tables)
		if err != nil {
			return nil, err
		}
	} else {
		if req.AllTables {
			tables = ksTables
		} else {
			return nil, fmt.Errorf("no tables to move")
		}
	}
	if len(req.ExcludeTables) > 0 {
		err = s.validateSourceTablesExist(ctx, sourceKeyspace, ksTables, req.ExcludeTables)
		if err != nil {
			return nil, err
		}
	}
	var tables2 []string
	for _, t := range tables {
		if shouldInclude(t, req.ExcludeTables) {
			tables2 = append(tables2, t)
		}
	}
	tables = tables2
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables to move")
	}
	log.Infof("Found tables to move: %s", strings.Join(tables, ","))

	if !vschema.Sharded {
		if err := s.addTablesToVSchema(ctx, sourceKeyspace, vschema, tables, externalTopo == nil); err != nil {
			return nil, err
		}
	}
	if externalTopo == nil {
		// Save routing rules before vschema. If we save vschema first, and routing rules
		// fails to save, we may generate duplicate table errors.
		rules, err := topotools.GetRoutingRules(ctx, s.ts)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			toSource := []string{sourceKeyspace + "." + table}
			rules[table] = toSource
			rules[table+"@replica"] = toSource
			rules[table+"@rdonly"] = toSource
			rules[targetKeyspace+"."+table] = toSource
			rules[targetKeyspace+"."+table+"@replica"] = toSource
			rules[targetKeyspace+"."+table+"@rdonly"] = toSource
			rules[targetKeyspace+"."+table] = toSource
			rules[sourceKeyspace+"."+table+"@replica"] = toSource
			rules[sourceKeyspace+"."+table+"@rdonly"] = toSource
		}
		if err := topotools.SaveRoutingRules(ctx, s.ts, rules); err != nil {
			return nil, err
		}

		if vschema != nil {
			// We added to the vschema.
			if err := s.ts.SaveVSchema(ctx, targetKeyspace, vschema); err != nil {
				return nil, err
			}
		}
	}
	if err := s.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:                  req.Workflow,
		MaterializationIntent:     vtctldatapb.MaterializationIntent_MOVETABLES,
		SourceKeyspace:            sourceKeyspace,
		TargetKeyspace:            targetKeyspace,
		Cell:                      strings.Join(req.Cells, ","),
		TabletTypes:               topoproto.MakeStringTypeCSV(req.TabletTypes),
		TabletSelectionPreference: req.TabletSelectionPreference,
		StopAfterCopy:             req.StopAfterCopy,
		ExternalCluster:           req.ExternalClusterName,
		SourceShards:              req.SourceShards,
		OnDdl:                     req.OnDdl,
		DeferSecondaryKeys:        req.DeferSecondaryKeys,
	}
	if req.SourceTimeZone != "" {
		ms.SourceTimeZone = req.SourceTimeZone
		ms.TargetTimeZone = "UTC"
	}
	createDDLMode := createDDLAsCopy
	if req.DropForeignKeys {
		createDDLMode = createDDLAsCopyDropForeignKeys
	}

	for _, table := range tables {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v", sqlparser.NewIdentifierCS(table))
		ms.TableSettings = append(ms.TableSettings, &vtctldatapb.TableMaterializeSettings{
			TargetTable:      table,
			SourceExpression: buf.String(),
			CreateDdl:        createDDLMode,
		})
	}
	mz := &materializer{
		ctx:      ctx,
		ts:       s.ts,
		sourceTs: sourceTopo,
		tmc:      s.tmc,
		ms:       ms,
	}
	err = mz.prepareMaterializerStreams(req)
	if err != nil {
		return nil, err
	}

	if ms.SourceTimeZone != "" {
		if err := mz.checkTZConversion(ctx, ms.SourceTimeZone); err != nil {
			return nil, err
		}
	}

	tabletShards, err := s.collectTargetStreams(ctx, mz)
	if err != nil {
		return nil, err
	}

	migrationID, err := getMigrationID(targetKeyspace, tabletShards)
	if err != nil {
		return nil, err
	}

	if mz.ms.ExternalCluster == "" {
		exists, tablets, err := s.checkIfPreviousJournalExists(ctx, mz, migrationID)
		if err != nil {
			return nil, err
		}
		if exists {
			log.Errorf("Found a previous journal entry for %d", migrationID)
			msg := fmt.Sprintf("found an entry from a previous run for migration id %d in _vt.resharding_journal on tablets %s, ",
				migrationID, strings.Join(tablets, ","))
			msg += fmt.Sprintf("please review and delete it before proceeding and then start the workflow using: MoveTables --workflow %s --target-keyspace %s start",
				req.Workflow, req.TargetKeyspace)
			return nil, fmt.Errorf(msg)
		}
	}

	if req.AutoStart {
		if err := mz.startStreams(ctx); err != nil {
			return nil, err
		}
	}

	return s.WorkflowStatus(ctx, &vtctldatapb.WorkflowStatusRequest{
		Keyspace: targetKeyspace,
		Workflow: req.Workflow,
	})
}

// MoveTablesComplete is part of the vtctlservicepb.VtctldServer interface.
// It cleans up a successful MoveTables workflow and its related artifacts.
func (s *Server) MoveTablesComplete(ctx context.Context, req *vtctldatapb.MoveTablesCompleteRequest) (*vtctldatapb.MoveTablesCompleteResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.MoveTablesComplete")
	defer span.Finish()

	ts, state, err := s.getWorkflowState(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	var summary string
	if req.DryRun {
		summary = fmt.Sprintf("Complete dry run results for workflow %s.%s at %v", req.TargetKeyspace, req.Workflow, time.Now().UTC().Format(time.RFC822))
	} else {
		summary = fmt.Sprintf("Successfully completed the %s workflow in the %s keyspace", req.Workflow, req.TargetKeyspace)
	}
	var dryRunResults *[]string

	if state.WorkflowType == TypeMigrate {
		dryRunResults, err = s.finalizeMigrateWorkflow(ctx, req.TargetKeyspace, req.Workflow, strings.Join(ts.tables, ","),
			false, req.KeepData, req.KeepRoutingRules, req.DryRun)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to finalize the %s workflow in the %s keyspace",
				req.Workflow, req.TargetKeyspace)
		}
		resp := &vtctldatapb.MoveTablesCompleteResponse{
			Summary: summary,
		}
		if dryRunResults != nil {
			resp.DryRunResults = *dryRunResults
		}
		return resp, nil
	}

	if !state.WritesSwitched || len(state.ReplicaCellsNotSwitched) > 0 || len(state.RdonlyCellsNotSwitched) > 0 {
		return nil, ErrWorkflowNotFullySwitched
	}
	var renameTable TableRemovalType
	if req.RenameTables {
		renameTable = RenameTable
	} else {
		renameTable = DropTable
	}
	if dryRunResults, err = s.dropSources(ctx, ts, renameTable, req.KeepData, req.KeepRoutingRules, false, req.DryRun); err != nil {
		return nil, err
	}

	resp := &vtctldatapb.MoveTablesCompleteResponse{
		Summary: summary,
	}
	if dryRunResults != nil {
		resp.DryRunResults = *dryRunResults
	}

	return resp, nil
}

// WorkflowDelete is part of the vtctlservicepb.VtctldServer interface.
// It passes on the request to the target primary tablets that are
// participating in the given workflow.
func (s *Server) WorkflowDelete(ctx context.Context, req *vtctldatapb.WorkflowDeleteRequest) (*vtctldatapb.WorkflowDeleteResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.WorkflowDelete")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("workflow", req.Workflow)

	// Cleanup related data and artifacts.
	if _, err := s.DropTargets(ctx, req.Keyspace, req.Workflow, req.KeepData, req.KeepRoutingRules, false); err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, vterrors.Wrapf(err, "%s keyspace does not exist", req.Keyspace)
		}
		return nil, err
	}

	deleteReq := &tabletmanagerdatapb.DeleteVReplicationWorkflowRequest{
		Workflow: req.Workflow,
	}
	vx := vexec.NewVExec(req.Keyspace, req.Workflow, s.ts, s.tmc)
	callback := func(ctx context.Context, tablet *topo.TabletInfo) (*querypb.QueryResult, error) {
		res, err := s.tmc.DeleteVReplicationWorkflow(ctx, tablet.Tablet, deleteReq)
		if err != nil {
			return nil, err
		}
		// Best effort cleanup and optimization of related data.
		s.deleteWorkflowVDiffData(ctx, tablet.Tablet, req.Workflow)
		s.optimizeCopyStateTable(tablet.Tablet)
		return res.Result, err
	}
	res, err := vx.CallbackContext(ctx, callback)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("the %s workflow does not exist in the %s keyspace", req.Workflow, req.Keyspace)
	}

	response := &vtctldatapb.WorkflowDeleteResponse{}
	response.Summary = fmt.Sprintf("Successfully cancelled the %s workflow in the %s keyspace", req.Workflow, req.Keyspace)
	details := make([]*vtctldatapb.WorkflowDeleteResponse_TabletInfo, 0, len(res))
	for tinfo, tres := range res {
		result := &vtctldatapb.WorkflowDeleteResponse_TabletInfo{
			Tablet:  tinfo.Alias,
			Deleted: tres.RowsAffected > 0, // Can be more than one with shard merges
		}
		details = append(details, result)
	}
	response.Details = details
	return response, nil
}

func (s *Server) WorkflowStatus(ctx context.Context, req *vtctldatapb.WorkflowStatusRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	ts, state, err := s.getWorkflowState(ctx, req.Keyspace, req.Workflow)
	if err != nil {
		return nil, err
	}
	copyProgress, err := s.GetCopyProgress(ctx, ts, state)
	if err != nil {
		return nil, err
	}
	resp := &vtctldatapb.WorkflowStatusResponse{}
	if copyProgress != nil {
		resp.TableCopyState = make(map[string]*vtctldatapb.WorkflowStatusResponse_TableCopyState, len(*copyProgress))
		// We sort the tables for intuitive and consistent output.
		var tables []string
		for table := range *copyProgress {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		var progress TableCopyProgress
		for _, table := range tables {
			var rowCountPct, tableSizePct float32
			resp.TableCopyState[table] = &vtctldatapb.WorkflowStatusResponse_TableCopyState{}
			progress = *(*copyProgress)[table]
			if progress.SourceRowCount > 0 {
				rowCountPct = float32(100.0 * float64(progress.TargetRowCount) / float64(progress.SourceRowCount))
			}
			if progress.SourceTableSize > 0 {
				tableSizePct = float32(100.0 * float64(progress.TargetTableSize) / float64(progress.SourceTableSize))
			}
			resp.TableCopyState[table].RowsCopied = progress.TargetRowCount
			resp.TableCopyState[table].RowsTotal = progress.SourceRowCount
			resp.TableCopyState[table].RowsPercentage = rowCountPct
			resp.TableCopyState[table].BytesCopied = progress.TargetTableSize
			resp.TableCopyState[table].BytesTotal = progress.SourceTableSize
			resp.TableCopyState[table].BytesPercentage = tableSizePct
		}
	}

	workflow, err := s.GetWorkflow(ctx, req.Keyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	// The stream key is target keyspace/tablet alias, e.g. 0/test-0000000100.
	// We sort the keys for intuitive and consistent output.
	streamKeys := make([]string, 0, len(workflow.ShardStreams))
	for streamKey := range workflow.ShardStreams {
		streamKeys = append(streamKeys, streamKey)
	}
	sort.Strings(streamKeys)
	resp.ShardStreams = make(map[string]*vtctldatapb.WorkflowStatusResponse_ShardStreams, len(streamKeys))
	for _, streamKey := range streamKeys {
		streams := workflow.ShardStreams[streamKey].GetStreams()
		keyParts := strings.Split(streamKey, "/")
		if len(keyParts) != 2 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected stream key format in: %s ; expect <keyspace>/<tablet-alias>",
				streamKey)
		}
		// We want to use target keyspace/shard as the map key for the
		// response, e.g. customer/-80.
		ksShard := fmt.Sprintf("%s/%s", req.Keyspace, keyParts[0])
		resp.ShardStreams[ksShard] = &vtctldatapb.WorkflowStatusResponse_ShardStreams{}
		resp.ShardStreams[ksShard].Streams = make([]*vtctldatapb.WorkflowStatusResponse_ShardStreamState, len(streams))
		for i, st := range streams {
			info := []string{}
			ts := &vtctldatapb.WorkflowStatusResponse_ShardStreamState{}
			if st.State == binlogdatapb.VReplicationWorkflowState_Error.String() {
				info = append(info, st.Message)
			} else if st.Position == "" {
				info = append(info, "VStream has not started")
			} else {
				now := time.Now().Nanosecond()
				updateLag := int64(now) - st.TimeUpdated.Seconds
				if updateLag > 0*1e9 {
					info = append(info, "VStream may not be running")
				}
				txLag := int64(now) - st.TransactionTimestamp.Seconds
				info = append(info, fmt.Sprintf("VStream Lag: %ds", txLag/1e9))
				if st.TransactionTimestamp.Seconds > 0 { // if no events occur after copy phase, TransactionTimeStamp can be 0
					info = append(info, fmt.Sprintf("; Tx time: %s.", time.Unix(st.TransactionTimestamp.Seconds, 0).Format(time.ANSIC)))
				}
			}
			ts.Id = int32(st.Id)
			ts.Tablet = st.Tablet
			ts.SourceShard = fmt.Sprintf("%s/%s", st.BinlogSource.Keyspace, st.BinlogSource.Shard)
			ts.Position = st.Position
			ts.Status = st.State
			ts.Info = strings.Join(info, "; ")
			resp.ShardStreams[ksShard].Streams[i] = ts
		}
	}

	return resp, nil
}

// GetCopyProgress returns the progress of all tables being copied in the
// workflow.
func (s *Server) GetCopyProgress(ctx context.Context, ts *trafficSwitcher, state *State) (*CopyProgress, error) {
	getTablesQuery := "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d"
	getRowCountQuery := "select table_name, table_rows, data_length from information_schema.tables where table_schema = %s and table_name in (%s)"
	tables := make(map[string]bool)
	const MaxRows = 1000
	sourcePrimaries := make(map[*topodatapb.TabletAlias]bool)
	for _, target := range ts.targets {
		for id, bls := range target.Sources {
			query := fmt.Sprintf(getTablesQuery, id)
			p3qr, err := s.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, true, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
				Query:   []byte(query),
				MaxRows: MaxRows,
			})
			if err != nil {
				return nil, err
			}
			if len(p3qr.Rows) < 1 {
				continue
			}
			qr := sqltypes.Proto3ToResult(p3qr)
			for i := 0; i < len(p3qr.Rows); i++ {
				tables[qr.Rows[i][0].ToString()] = true
			}
			sourcesi, err := s.ts.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			found := false
			for existingSource := range sourcePrimaries {
				if existingSource.Uid == sourcesi.PrimaryAlias.Uid {
					found = true
				}
			}
			if !found {
				sourcePrimaries[sourcesi.PrimaryAlias] = true
			}
		}
	}
	if len(tables) == 0 {
		return nil, nil
	}
	var tableList []string
	targetRowCounts := make(map[string]int64)
	sourceRowCounts := make(map[string]int64)
	targetTableSizes := make(map[string]int64)
	sourceTableSizes := make(map[string]int64)

	for table := range tables {
		tableList = append(tableList, encodeString(table))
		targetRowCounts[table] = 0
		sourceRowCounts[table] = 0
		targetTableSizes[table] = 0
		sourceTableSizes[table] = 0
	}

	var getTableMetrics = func(tablet *topodatapb.Tablet, query string, rowCounts *map[string]int64, tableSizes *map[string]int64) error {
		p3qr, err := s.tmc.ExecuteFetchAsDba(ctx, tablet, true, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   []byte(query),
			MaxRows: uint64(len(tables)),
		})
		if err != nil {
			return err
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		for i := 0; i < len(qr.Rows); i++ {
			table := qr.Rows[i][0].ToString()
			rowCount, err := evalengine.ToInt64(qr.Rows[i][1])
			if err != nil {
				return err
			}
			tableSize, err := evalengine.ToInt64(qr.Rows[i][2])
			if err != nil {
				return err
			}
			(*rowCounts)[table] += rowCount
			(*tableSizes)[table] += tableSize
		}
		return nil
	}
	sourceDbName := ""
	for _, tsSource := range ts.sources {
		sourceDbName = tsSource.GetPrimary().DbName()
		break
	}
	if sourceDbName == "" {
		return nil, fmt.Errorf("no sources found for workflow %s.%s", state.TargetKeyspace, state.Workflow)
	}
	targetDbName := ""
	for _, tsTarget := range ts.targets {
		targetDbName = tsTarget.GetPrimary().DbName()
		break
	}
	if sourceDbName == "" || targetDbName == "" {
		return nil, fmt.Errorf("workflow %s.%s is incorrectly configured", state.TargetKeyspace, state.Workflow)
	}
	sort.Strings(tableList) // sort list for repeatability for mocking in tests
	tablesStr := strings.Join(tableList, ",")
	query := fmt.Sprintf(getRowCountQuery, encodeString(targetDbName), tablesStr)
	for _, target := range ts.targets {
		tablet := target.GetPrimary().Tablet
		if err := getTableMetrics(tablet, query, &targetRowCounts, &targetTableSizes); err != nil {
			return nil, err
		}
	}

	query = fmt.Sprintf(getRowCountQuery, encodeString(sourceDbName), tablesStr)
	for source := range sourcePrimaries {
		ti, err := s.ts.GetTablet(ctx, source)
		tablet := ti.Tablet
		if err != nil {
			return nil, err
		}
		if err := getTableMetrics(tablet, query, &sourceRowCounts, &sourceTableSizes); err != nil {
			return nil, err
		}
	}

	copyProgress := CopyProgress{}
	for table, rowCount := range targetRowCounts {
		copyProgress[table] = &TableCopyProgress{
			TargetRowCount:  rowCount,
			TargetTableSize: targetTableSizes[table],
			SourceRowCount:  sourceRowCounts[table],
			SourceTableSize: sourceTableSizes[table],
		}
	}
	return &copyProgress, nil
}

// WorkflowUpdate is part of the vtctlservicepb.VtctldServer interface.
// It passes the embedded TabletRequest object to the given keyspace's
// target primary tablets that are participating in the given workflow.
func (s *Server) WorkflowUpdate(ctx context.Context, req *vtctldatapb.WorkflowUpdateRequest) (*vtctldatapb.WorkflowUpdateResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.WorkflowUpdate")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("workflow", req.TabletRequest.Workflow)
	span.Annotate("cells", req.TabletRequest.Cells)
	span.Annotate("tablet_types", req.TabletRequest.TabletTypes)
	span.Annotate("on_ddl", req.TabletRequest.OnDdl)

	vx := vexec.NewVExec(req.Keyspace, req.TabletRequest.Workflow, s.ts, s.tmc)
	callback := func(ctx context.Context, tablet *topo.TabletInfo) (*querypb.QueryResult, error) {
		res, err := s.tmc.UpdateVReplicationWorkflow(ctx, tablet.Tablet, req.TabletRequest)
		if err != nil {
			return nil, err
		}
		return res.Result, err
	}
	res, err := vx.CallbackContext(ctx, callback)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, vterrors.Wrapf(err, "%s keyspace does not exist", req.Keyspace)
		}
		return nil, err
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("the %s workflow does not exist in the %s keyspace", req.TabletRequest.Workflow, req.Keyspace)
	}

	response := &vtctldatapb.WorkflowUpdateResponse{}
	response.Summary = fmt.Sprintf("Successfully updated the %s workflow on (%d) target primary tablets in the %s keyspace", req.TabletRequest.Workflow, len(res), req.Keyspace)
	details := make([]*vtctldatapb.WorkflowUpdateResponse_TabletInfo, 0, len(res))
	for tinfo, tres := range res {
		result := &vtctldatapb.WorkflowUpdateResponse_TabletInfo{
			Tablet:  tinfo.Alias,
			Changed: tres.RowsAffected > 0, // Can be more than one with shard merges
		}
		details = append(details, result)
	}
	response.Details = details
	return response, nil
}

// validateSourceTablesExist validates that tables provided are present
// in the source keyspace.
func (s *Server) validateSourceTablesExist(ctx context.Context, sourceKeyspace string, ksTables, tables []string) error {
	var missingTables []string
	for _, table := range tables {
		if schema.IsInternalOperationTableName(table) {
			continue
		}
		found := false

		for _, ksTable := range ksTables {
			if table == ksTable {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, table)
		}
	}
	if len(missingTables) > 0 {
		return fmt.Errorf("table(s) not found in source keyspace %s: %s", sourceKeyspace, strings.Join(missingTables, ","))
	}
	return nil
}

// addTablesToVSchema adds tables to an (unsharded) vschema if they are not already defined.
// If copyVSchema is true then we copy over the vschema table definitions from the source,
// otherwise we create empty ones.
// For a migrate workflow we do not copy the vschema since the source keyspace is just a
// proxy to import data into Vitess.
func (s *Server) addTablesToVSchema(ctx context.Context, sourceKeyspace string, targetVSchema *vschemapb.Keyspace, tables []string, copyVSchema bool) error {
	if targetVSchema.Tables == nil {
		targetVSchema.Tables = make(map[string]*vschemapb.Table)
	}
	if copyVSchema {
		srcVSchema, err := s.ts.GetVSchema(ctx, sourceKeyspace)
		if err != nil {
			return vterrors.Wrapf(err, "failed to get vschema for source keyspace %s", sourceKeyspace)
		}
		for _, table := range tables {
			srcTable, sok := srcVSchema.Tables[table]
			if _, tok := targetVSchema.Tables[table]; sok && !tok {
				targetVSchema.Tables[table] = srcTable
				// If going from sharded to unsharded, then we need to remove the
				// column vindexes as they are not valid for unsharded tables.
				if srcVSchema.Sharded {
					targetVSchema.Tables[table].ColumnVindexes = nil
				}
			}
		}
	}
	// Ensure that each table at least has an empty definition on the target.
	for _, table := range tables {
		if _, tok := targetVSchema.Tables[table]; !tok {
			targetVSchema.Tables[table] = &vschemapb.Table{}
		}
	}
	return nil
}

func (s *Server) collectTargetStreams(ctx context.Context, mz *materializer) ([]string, error) {
	var shardTablets []string
	var mu sync.Mutex
	err := mz.forAllTargets(func(target *topo.ShardInfo) error {
		var qrproto *querypb.QueryResult
		var id int64
		var err error
		targetPrimary, err := s.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}
		query := fmt.Sprintf("select id from _vt.vreplication where db_name=%s and workflow=%s", encodeString(targetPrimary.DbName()), encodeString(mz.ms.Workflow))
		if qrproto, err = s.tmc.VReplicationExec(ctx, targetPrimary.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetPrimary.Tablet, query)
		}
		qr := sqltypes.Proto3ToResult(qrproto)
		for i := 0; i < len(qr.Rows); i++ {
			id, err = evalengine.ToInt64(qr.Rows[i][0])
			if err != nil {
				return err
			}
			mu.Lock()
			shardTablets = append(shardTablets, fmt.Sprintf("%s:%d", target.ShardName(), id))
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return shardTablets, nil
}

func (s *Server) checkIfPreviousJournalExists(ctx context.Context, mz *materializer, migrationID int64) (bool, []string, error) {
	forAllSources := func(f func(*topo.ShardInfo) error) error {
		var wg sync.WaitGroup
		allErrors := &concurrency.AllErrorRecorder{}
		for _, sourceShard := range mz.sourceShards {
			wg.Add(1)
			go func(sourceShard *topo.ShardInfo) {
				defer wg.Done()

				if err := f(sourceShard); err != nil {
					allErrors.RecordError(err)
				}
			}(sourceShard)
		}
		wg.Wait()
		return allErrors.AggrError(vterrors.Aggregate)
	}

	var (
		mu      sync.Mutex
		exists  bool
		tablets []string
	)

	err := forAllSources(func(si *topo.ShardInfo) error {
		tablet, err := s.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return err
		}
		if tablet == nil {
			return nil
		}
		_, exists, err = s.CheckReshardingJournalExistsOnTablet(ctx, tablet.Tablet, migrationID)
		if err != nil {
			return err
		}
		if exists {
			mu.Lock()
			defer mu.Unlock()
			tablets = append(tablets, tablet.AliasString())
		}
		return nil
	})
	return exists, tablets, err
}

// deleteWorkflowVDiffData cleans up any potential VDiff related data associated
// with the workflow on the given tablet.
func (s *Server) deleteWorkflowVDiffData(ctx context.Context, tablet *topodatapb.Tablet, workflow string) {
	sqlDeleteVDiffs := `delete from vd, vdt, vdl using _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						inner join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
						where vd.keyspace = %s and vd.workflow = %s`
	query := fmt.Sprintf(sqlDeleteVDiffs, encodeString(tablet.Keyspace), encodeString(workflow))
	rows := -1
	if _, err := s.tmc.ExecuteFetchAsAllPrivs(ctx, tablet, &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
		Query:   []byte(query),
		MaxRows: uint64(rows),
	}); err != nil {
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Num != mysql.ERNoSuchTable { // the tables may not exist if no vdiffs have been run
			log.Errorf("Error deleting vdiff data for %s.%s workflow: %v", tablet.Keyspace, workflow, err)
		}
	}
}

// optimizeCopyStateTable rebuilds the copy_state table to ensure the on-disk
// structures are minimal and optimized and resets the auto-inc value for
// subsequent inserts.
// This helps to ensure that the size, storage, and performance related factors
// for the table remain optimal over time and that we don't ever exhaust the
// available auto-inc values for the table.
// Note: it's not critical that this executes successfully any given time, it's
// only important that we try to do this periodically so that things stay in an
// optimal state over long periods of time. For this reason, the work is done
// asynchronously in the background on the given tablet and any failures are
// logged as warnings. Because it's done in the background we use the AllPrivs
// account to be sure that we don't execute the writes if READ_ONLY is set on
// the MySQL instance.
func (s *Server) optimizeCopyStateTable(tablet *topodatapb.Tablet) {
	if s.sem != nil {
		if !s.sem.TryAcquire(1) {
			log.Warningf("Deferring work to optimize the copy_state table on %q due to hitting the maximum concurrent background job limit.",
				tablet.Alias.String())
			return
		}
	}
	go func() {
		defer func() {
			if s.sem != nil {
				s.sem.Release(1)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		sqlOptimizeTable := "optimize table _vt.copy_state"
		if _, err := s.tmc.ExecuteFetchAsAllPrivs(ctx, tablet, &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
			Query:   []byte(sqlOptimizeTable),
			MaxRows: uint64(100), // always produces 1+rows with notes and status
		}); err != nil {
			if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Num == mysql.ERNoSuchTable { // the table may not exist
				return
			}
			log.Warningf("Failed to optimize the copy_state table on %q: %v", tablet.Alias.String(), err)
		}
		// This will automatically set the value to 1 or the current max value in the
		// table, whichever is greater.
		sqlResetAutoInc := "alter table _vt.copy_state auto_increment = 1"
		if _, err := s.tmc.ExecuteFetchAsAllPrivs(ctx, tablet, &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
			Query:   []byte(sqlResetAutoInc),
			MaxRows: uint64(0),
		}); err != nil {
			log.Warningf("Failed to reset the auto_increment value for the copy_state table on %q: %v",
				tablet.Alias.String(), err)
		}
	}()
}

// DropTargets cleans up target tables, shards and denied tables if a MoveTables/Reshard
// is cancelled.
func (s *Server) DropTargets(ctx context.Context, targetKeyspace, workflow string, keepData, keepRoutingRules, dryRun bool) (*[]string, error) {
	ts, state, err := s.getWorkflowState(ctx, targetKeyspace, workflow)
	if err != nil {
		log.Errorf("Failed to get VReplication workflow state for %s.%s: %v", targetKeyspace, workflow, err)
		return nil, err
	}

	// Return an error if the workflow traffic is partially switched.
	if state.WritesSwitched || len(state.ReplicaCellsSwitched) > 0 || len(state.RdonlyCellsSwitched) > 0 {
		return nil, ErrWorkflowPartiallySwitched
	}

	if state.WorkflowType == TypeMigrate {
		_, err := s.finalizeMigrateWorkflow(ctx, targetKeyspace, workflow, "", true, keepData, keepRoutingRules, dryRun)
		return nil, err
	}

	ts.keepRoutingRules = keepRoutingRules
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{s: s, ts: ts}
	}
	var tctx context.Context
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "DropTargets")
	if lockErr != nil {
		ts.Logger().Errorf("Source LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer sourceUnlock(&err)
	ctx = tctx

	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "DropTargets")
		if lockErr != nil {
			ts.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
			return nil, lockErr
		}
		defer targetUnlock(&err)
		ctx = tctx
	}
	if !keepData {
		switch ts.MigrationType() {
		case binlogdatapb.MigrationType_TABLES:
			if err := sw.removeTargetTables(ctx); err != nil {
				return nil, err
			}
			if err := sw.dropSourceDeniedTables(ctx); err != nil {
				return nil, err
			}
		case binlogdatapb.MigrationType_SHARDS:
			if err := sw.dropTargetShards(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := s.dropRelatedArtifacts(ctx, keepRoutingRules, sw); err != nil {
		return nil, err
	}
	if err := ts.TopoServer().RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}
	return sw.logs(), nil
}

func (s *Server) buildTrafficSwitcher(ctx context.Context, targetKeyspace, workflowName string) (*trafficSwitcher, error) {
	tgtInfo, err := BuildTargets(ctx, s.ts, s.tmc, targetKeyspace, workflowName)
	if err != nil {
		log.Infof("Error building targets: %s", err)
		return nil, err
	}
	targets, frozen, optCells, optTabletTypes := tgtInfo.Targets, tgtInfo.Frozen, tgtInfo.OptCells, tgtInfo.OptTabletTypes

	ts := &trafficSwitcher{
		ws:              s,
		logger:          logutil.NewConsoleLogger(),
		workflow:        workflowName,
		reverseWorkflow: ReverseWorkflowName(workflowName),
		id:              HashStreams(targetKeyspace, targets),
		targets:         targets,
		sources:         make(map[string]*MigrationSource),
		targetKeyspace:  targetKeyspace,
		frozen:          frozen,
		optCells:        optCells,
		optTabletTypes:  optTabletTypes,
		workflowType:    tgtInfo.WorkflowType,
		workflowSubType: tgtInfo.WorkflowSubType,
	}
	log.Infof("Migration ID for workflow %s: %d", workflowName, ts.id)
	sourceTopo := s.ts

	// Build the sources.
	for _, target := range targets {
		for _, bls := range target.Sources {
			if ts.sourceKeyspace == "" {
				ts.sourceKeyspace = bls.Keyspace
				ts.sourceTimeZone = bls.SourceTimeZone
				ts.targetTimeZone = bls.TargetTimeZone
				ts.externalCluster = bls.ExternalCluster
				if ts.externalCluster != "" {
					externalTopo, err := s.ts.OpenExternalVitessClusterServer(ctx, ts.externalCluster)
					if err != nil {
						return nil, err
					}
					sourceTopo = externalTopo
					ts.externalTopo = externalTopo
				}
			} else if ts.sourceKeyspace != bls.Keyspace {
				return nil, fmt.Errorf("source keyspaces are mismatched across streams: %v vs %v", ts.sourceKeyspace, bls.Keyspace)
			}

			if ts.tables == nil {
				for _, rule := range bls.Filter.Rules {
					ts.tables = append(ts.tables, rule.Match)
				}
				sort.Strings(ts.tables)
			} else {
				var tables []string
				for _, rule := range bls.Filter.Rules {
					tables = append(tables, rule.Match)
				}
				sort.Strings(tables)
				if !reflect.DeepEqual(ts.tables, tables) {
					return nil, fmt.Errorf("table lists are mismatched across streams: %v vs %v", ts.tables, tables)
				}
			}

			if _, ok := ts.sources[bls.Shard]; ok {
				continue
			}
			sourcesi, err := sourceTopo.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			if sourcesi.PrimaryAlias == nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "source shard %s/%s currently has no primary tablet",
					bls.Keyspace, bls.Shard)
			}
			sourcePrimary, err := sourceTopo.GetTablet(ctx, sourcesi.PrimaryAlias)
			if err != nil {
				return nil, err
			}
			ts.sources[bls.Shard] = NewMigrationSource(sourcesi, sourcePrimary)
		}
	}
	if ts.sourceKeyspace != ts.targetKeyspace || ts.externalCluster != "" {
		ts.migrationType = binlogdatapb.MigrationType_TABLES
	} else {
		// TODO(sougou): for shard migration, validate that source and target combined
		// keyranges match.
		ts.migrationType = binlogdatapb.MigrationType_SHARDS
		for sourceShard := range ts.sources {
			if _, ok := ts.targets[sourceShard]; ok {
				// If shards are overlapping, then this is a table migration.
				ts.migrationType = binlogdatapb.MigrationType_TABLES
				break
			}
		}
	}
	vs, err := sourceTopo.GetVSchema(ctx, ts.sourceKeyspace)
	if err != nil {
		return nil, err
	}
	ts.sourceKSSchema, err = vindexes.BuildKeyspaceSchema(vs, ts.sourceKeyspace)
	if err != nil {
		return nil, err
	}

	sourceShards, targetShards := ts.getSourceAndTargetShardsNames()

	ts.isPartialMigration, err = ts.isPartialMoveTables(sourceShards, targetShards)
	if err != nil {
		return nil, err
	}
	if ts.isPartialMigration {
		log.Infof("Migration is partial, for shards %+v", sourceShards)
	}
	return ts, nil
}

func (s *Server) dropRelatedArtifacts(ctx context.Context, keepRoutingRules bool, sw iswitcher) error {
	if err := sw.dropSourceReverseVReplicationStreams(ctx); err != nil {
		return err
	}
	if !keepRoutingRules {
		if err := sw.deleteRoutingRules(ctx); err != nil {
			return err
		}
		if err := sw.deleteShardRoutingRules(ctx); err != nil {
			return err
		}
	}

	return nil
}

// dropSources cleans up source tables, shards and denied tables after a
// MoveTables/Reshard is completed.
func (s *Server) dropSources(ctx context.Context, ts *trafficSwitcher, removalType TableRemovalType, keepData, keepRoutingRules, force, dryRun bool) (*[]string, error) {
	var (
		sw  iswitcher
		err error
	)
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, s: s}
	}
	var tctx context.Context
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "DropSources")
	if lockErr != nil {
		ts.Logger().Errorf("Source LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer sourceUnlock(&err)
	ctx = tctx
	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "DropSources")
		if lockErr != nil {
			ts.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
			return nil, lockErr
		}
		defer targetUnlock(&err)
		ctx = tctx
	}
	if !force {
		if err := sw.validateWorkflowHasCompleted(ctx); err != nil {
			ts.Logger().Errorf("Workflow has not completed, cannot DropSources: %v", err)
			return nil, err
		}
	}
	if !keepData {
		switch ts.MigrationType() {
		case binlogdatapb.MigrationType_TABLES:
			log.Infof("Deleting tables")
			if err := sw.removeSourceTables(ctx, removalType); err != nil {
				return nil, err
			}
			if err := sw.dropSourceDeniedTables(ctx); err != nil {
				return nil, err
			}

		case binlogdatapb.MigrationType_SHARDS:
			log.Infof("Removing shards")
			if err := sw.dropSourceShards(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := s.dropArtifacts(ctx, keepRoutingRules, sw); err != nil {
		return nil, err
	}
	if err := ts.TopoServer().RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}

	return sw.logs(), nil
}

func (s *Server) dropArtifacts(ctx context.Context, keepRoutingRules bool, sw iswitcher) error {
	if err := sw.dropSourceReverseVReplicationStreams(ctx); err != nil {
		return err
	}
	if err := sw.dropTargetVReplicationStreams(ctx); err != nil {
		return err
	}
	if !keepRoutingRules {
		if err := sw.deleteRoutingRules(ctx); err != nil {
			return err
		}
		if err := sw.deleteShardRoutingRules(ctx); err != nil {
			return err
		}
	}

	return nil
}

// DeleteShard will do all the necessary changes in the topology server
// to entirely remove a shard.
func (s *Server) DeleteShard(ctx context.Context, keyspace, shard string, recursive, evenIfServing bool) error {
	// Read the Shard object. If it's not there, try to clean up
	// the topology anyway.
	shardInfo, err := s.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			log.Infof("Shard %v/%v doesn't seem to exist, cleaning up any potential leftover", keyspace, shard)
			return s.ts.DeleteShard(ctx, keyspace, shard)
		}
		return err
	}

	servingCells, err := s.ts.GetShardServingCells(ctx, shardInfo)
	if err != nil {
		return err
	}
	// Check the Serving map for the shard, we don't want to
	// remove a serving shard if not absolutely sure.
	if !evenIfServing && len(servingCells) > 0 {
		return fmt.Errorf("shard %v/%v is still serving, cannot delete it, use the even-if-serving flag if needed", keyspace, shard)
	}

	cells, err := s.ts.GetCellInfoNames(ctx)
	if err != nil {
		return err
	}

	// Go through all the cells.
	for _, cell := range cells {
		var aliases []*topodatapb.TabletAlias

		// Get the ShardReplication object for that cell. Try
		// to find all tablets that may belong to our shard.
		sri, err := s.ts.GetShardReplication(ctx, cell, keyspace, shard)
		switch {
		case topo.IsErrType(err, topo.NoNode):
			// No ShardReplication object. It means the
			// topo is inconsistent. Let's read all the
			// tablets for that cell, and if we find any
			// in our keyspace / shard, either abort or
			// try to delete them.
			aliases, err = s.ts.GetTabletAliasesByCell(ctx, cell)
			if err != nil {
				return fmt.Errorf("GetTabletsByCell(%v) failed: %v", cell, err)
			}
		case err == nil:
			// We found a ShardReplication object. We
			// trust it to have all tablet records.
			aliases = make([]*topodatapb.TabletAlias, len(sri.Nodes))
			for i, n := range sri.Nodes {
				aliases[i] = n.TabletAlias
			}
		default:
			return fmt.Errorf("GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
		}

		// Get the corresponding Tablet records. Note
		// GetTabletMap ignores ErrNoNode, and it's good for
		// our purpose, it means a tablet was deleted but is
		// still referenced.
		tabletMap, err := s.ts.GetTabletMap(ctx, aliases)
		if err != nil {
			return fmt.Errorf("GetTabletMap() failed: %v", err)
		}

		// Remove the tablets that don't belong to our
		// keyspace/shard from the map.
		for a, ti := range tabletMap {
			if ti.Keyspace != keyspace || ti.Shard != shard {
				delete(tabletMap, a)
			}
		}

		// Now see if we need to DeleteTablet, and if we can, do it.
		if len(tabletMap) > 0 {
			if !recursive {
				return fmt.Errorf("shard %v/%v still has %v tablets in cell %v; use --recursive or remove them manually", keyspace, shard, len(tabletMap), cell)
			}

			log.Infof("Deleting all tablets in shard %v/%v cell %v", keyspace, shard, cell)
			for tabletAlias, tabletInfo := range tabletMap {
				// We don't care about scrapping or updating the replication graph,
				// because we're about to delete the entire replication graph.
				log.Infof("Deleting tablet %v", tabletAlias)
				if err := s.ts.DeleteTablet(ctx, tabletInfo.Alias); err != nil && !topo.IsErrType(err, topo.NoNode) {
					// We don't want to continue if a DeleteTablet fails for
					// any good reason (other than missing tablet, in which
					// case it's just a topology server inconsistency we can
					// ignore). If we continue and delete the replication
					// graph, the tablet record will be orphaned, since
					// we'll no longer know it belongs to this shard.
					//
					// If the problem is temporary, or resolved externally, re-running
					// DeleteShard will skip over tablets that were already deleted.
					return fmt.Errorf("can't delete tablet %v: %v", tabletAlias, err)
				}
			}
		}
	}

	// Try to remove the replication graph and serving graph in each cell,
	// regardless of its existence.
	for _, cell := range cells {
		if err := s.ts.DeleteShardReplication(ctx, cell, keyspace, shard); err != nil && !topo.IsErrType(err, topo.NoNode) {
			log.Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}
	}

	return s.ts.DeleteShard(ctx, keyspace, shard)
}

// updateShardRecords updates the shard records based on 'from' or 'to' direction.
func (s *Server) updateShardRecords(ctx context.Context, keyspace string, shards []*topo.ShardInfo, cells []string, servedType topodatapb.TabletType, isFrom bool, clearSourceShards bool) (err error) {
	return topotools.UpdateShardRecords(ctx, s.ts, s.tmc, keyspace, shards, cells, servedType, isFrom, clearSourceShards, nil)
}

// refreshPrimaryTablets will just RPC-ping all the primary tablets with RefreshState
func (s *Server) refreshPrimaryTablets(ctx context.Context, shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			log.Infof("RefreshState primary %v", topoproto.TabletAliasString(si.PrimaryAlias))
			ti, err := s.ts.GetTablet(ctx, si.PrimaryAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			if err := s.tmc.RefreshState(ctx, ti.Tablet); err != nil {
				rec.RecordError(err)
			} else {
				log.Infof("%v responded", topoproto.TabletAliasString(si.PrimaryAlias))
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// finalizeMigrateWorkflow deletes the streams for the Migrate workflow.
// We only cleanup the target for external sources.
func (s *Server) finalizeMigrateWorkflow(ctx context.Context, targetKeyspace, workflow, tableSpecs string, cancel, keepData, keepRoutingRules, dryRun bool) (*[]string, error) {
	ts, err := s.buildTrafficSwitcher(ctx, targetKeyspace, workflow)
	if err != nil {
		ts.Logger().Errorf("buildTrafficSwitcher failed: %v", err)
		return nil, err
	}
	var sw iswitcher
	if dryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{s: s, ts: ts}
	}
	var tctx context.Context
	tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "completeMigrateWorkflow")
	if lockErr != nil {
		ts.Logger().Errorf("Target LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer targetUnlock(&err)
	ctx = tctx
	if err := sw.dropTargetVReplicationStreams(ctx); err != nil {
		return nil, err
	}
	if !cancel {
		if err := sw.addParticipatingTablesToKeyspace(ctx, targetKeyspace, tableSpecs); err != nil {
			return nil, err
		}
		if err := ts.TopoServer().RebuildSrvVSchema(ctx, nil); err != nil {
			return nil, err
		}
	}
	log.Infof("cancel is %t, keepData %t", cancel, keepData)
	if cancel && !keepData {
		if err := sw.removeTargetTables(ctx); err != nil {
			return nil, err
		}
	}
	return sw.logs(), nil
}

// WorkflowSwitchTraffic switches traffic in the direction passed for specified tablet types.
func (s *Server) WorkflowSwitchTraffic(ctx context.Context, req *vtctldatapb.WorkflowSwitchTrafficRequest) (*vtctldatapb.WorkflowSwitchTrafficResponse, error) {
	var (
		dryRunResults                     []string
		rdDryRunResults, wrDryRunResults  *[]string
		hasReplica, hasRdonly, hasPrimary bool
	)

	timeout, set, err := protoutil.DurationFromProto(req.Timeout)
	if err != nil {
		err = vterrors.Wrapf(err, "unable to parse Timeout into a valid duration")
		return nil, err
	}
	if !set {
		timeout = defaultDuration
	}

	ts, startState, err := s.getWorkflowState(ctx, req.Keyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	if startState.WorkflowType == TypeMigrate {
		return nil, fmt.Errorf("invalid action for Migrate workflow: SwitchTraffic")
	}

	maxReplicationLagAllowed, set, err := protoutil.DurationFromProto(req.MaxReplicationLagAllowed)
	if err != nil {
		err = vterrors.Wrapf(err, "unable to parse MaxReplicationLagAllowed into a valid duration")
		return nil, err
	}
	if !set {
		maxReplicationLagAllowed = defaultDuration
	}

	direction := TrafficSwitchDirection(req.Direction)
	if direction == DirectionBackward {
		ts, startState, err = s.getWorkflowState(ctx, startState.SourceKeyspace, ts.reverseWorkflow)
		if err != nil {
			return nil, err
		}
	}
	reason, err := s.canSwitch(ctx, ts, startState, direction, int64(maxReplicationLagAllowed.Seconds()))
	if err != nil {
		return nil, err
	}
	if reason != "" {
		return nil, fmt.Errorf("cannot switch traffic for workflow %s at this time: %s", startState.Workflow, reason)
	}

	hasReplica, hasRdonly, hasPrimary, err = parseTabletTypes(req.TabletTypes)
	if err != nil {
		return nil, err
	}
	if hasReplica || hasRdonly {
		if rdDryRunResults, err = s.switchReads(ctx, req, ts, startState, timeout, false, direction); err != nil {
			return nil, err
		}
	}
	if rdDryRunResults != nil {
		dryRunResults = append(dryRunResults, *rdDryRunResults...)
	}
	if hasPrimary {
		if _, wrDryRunResults, err = s.switchWrites(ctx, req, ts, timeout, false, req.EnableReverseReplication); err != nil {
			return nil, err
		}
	}

	if wrDryRunResults != nil {
		dryRunResults = append(dryRunResults, *wrDryRunResults...)
	}
	if req.DryRun && len(dryRunResults) == 0 {
		dryRunResults = append(dryRunResults, "No changes required")
	}
	cmd := "SwitchTraffic"
	if direction == DirectionBackward {
		cmd = "ReverseTraffic"
	}
	resp := &vtctldatapb.WorkflowSwitchTrafficResponse{}
	if req.DryRun {
		resp.Summary = fmt.Sprintf("%s dry run results for workflow %s.%s at %v", cmd, req.Keyspace, req.Workflow, time.Now().UTC().Format(time.RFC822))
		resp.DryRunResults = dryRunResults
	} else {
		resp.Summary = fmt.Sprintf("%s was successful for workflow %s.%s", cmd, req.Keyspace, req.Workflow)
		// Reload the state after the SwitchTraffic operation
		// and return that as a string.
		keyspace := req.Keyspace
		workflow := req.Workflow
		if direction == DirectionBackward {
			keyspace = startState.SourceKeyspace
			workflow = ts.reverseWorkflow
		}
		resp.StartState = startState.String()
		_, currentState, err := s.getWorkflowState(ctx, keyspace, workflow)
		if err != nil {
			resp.CurrentState = fmt.Sprintf("Error reloading workflow state after switching traffic: %v", err)
		} else {
			resp.CurrentState = currentState.String()
		}
	}
	return resp, nil
}

// switchReads is a generic way of switching read traffic for a workflow.
func (s *Server) switchReads(ctx context.Context, req *vtctldatapb.WorkflowSwitchTrafficRequest, ts *trafficSwitcher, state *State, timeout time.Duration, cancel bool, direction TrafficSwitchDirection) (*[]string, error) {
	roTypesToSwitchStr := topoproto.MakeStringTypeCSV(req.TabletTypes)
	var hasReplica, hasRdonly bool
	for _, roType := range req.TabletTypes {
		switch roType {
		case topodatapb.TabletType_REPLICA:
			hasReplica = true
		case topodatapb.TabletType_RDONLY:
			hasRdonly = true
		}
	}

	log.Infof("Switching reads: %s.%s tablet types: %s, cells: %s, workflow state: %s", ts.targetKeyspace, ts.workflow, roTypesToSwitchStr, ts.optCells, state.String())
	if !hasReplica && !hasRdonly {
		return nil, fmt.Errorf("tablet types must be REPLICA or RDONLY: %s", roTypesToSwitchStr)
	}
	if !ts.isPartialMigration { // shard level traffic switching is all or nothing
		if direction == DirectionBackward && hasReplica && len(state.ReplicaCellsSwitched) == 0 {
			return nil, fmt.Errorf("requesting reversal of read traffic for REPLICAs but REPLICA reads have not been switched")
		}
		if direction == DirectionBackward && hasRdonly && len(state.RdonlyCellsSwitched) == 0 {
			return nil, fmt.Errorf("requesting reversal of SwitchReads for RDONLYs but RDONLY reads have not been switched")
		}
	}
	var cells []string = req.Cells
	// If no cells were provided in the command then use the value from the workflow.
	if len(cells) == 0 && ts.optCells != "" {
		cells = strings.Split(strings.TrimSpace(ts.optCells), ",")
	}

	// If there are no rdonly tablets in the cells ask to switch rdonly tablets as well so that routing rules
	// are updated for rdonly as well. Otherwise vitess will not know that the workflow has completed and will
	// incorrectly report that not all reads have been switched. User currently is forced to switch non-existent
	// rdonly tablets.
	if hasReplica && !hasRdonly {
		var err error
		rdonlyTabletsExist, err := topotools.DoCellsHaveRdonlyTablets(ctx, s.ts, cells)
		if err != nil {
			return nil, err
		}
		if rdonlyTabletsExist {
			return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "requesting reversal of SwitchReads for REPLICAs but RDONLY tablets also exist in the cells")
		}
	}

	// If journals exist notify user and fail.
	journalsExist, _, err := ts.checkJournals(ctx)
	if err != nil {
		ts.Logger().Errorf("checkJournals failed: %v", err)
		return nil, err
	}
	if journalsExist {
		log.Infof("Found a previous journal entry for %d", ts.id)
	}
	var sw iswitcher
	if req.DryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, s: s}
	}

	if err := ts.validate(ctx); err != nil {
		ts.Logger().Errorf("validate failed: %v", err)
		return nil, err
	}

	// For reads, locking the source keyspace is sufficient.
	ctx, unlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "SwitchReads")
	if lockErr != nil {
		ts.Logger().Errorf("LockKeyspace failed: %v", lockErr)
		return nil, lockErr
	}
	defer unlock(&err)

	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		if ts.isPartialMigration {
			ts.Logger().Infof("Partial migration, skipping switchTableReads as traffic is all or nothing per shard and overridden for reads AND writes in the ShardRoutingRule created when switching writes.")
		} else if err := sw.switchTableReads(ctx, cells, req.TabletTypes, direction); err != nil {
			ts.Logger().Errorf("switchTableReads failed: %v", err)
			return nil, err
		}
		return sw.logs(), nil
	}
	ts.Logger().Infof("About to switchShardReads: %+v, %+s, %+v", cells, roTypesToSwitchStr, direction)
	if err := sw.switchShardReads(ctx, cells, req.TabletTypes, direction); err != nil {
		ts.Logger().Errorf("switchShardReads failed: %v", err)
		return nil, err
	}

	ts.Logger().Infof("switchShardReads Completed: %+v, %+s, %+v", cells, roTypesToSwitchStr, direction)
	if err := s.ts.ValidateSrvKeyspace(ctx, ts.targetKeyspace, strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "After switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.targetKeyspace, strings.Join(cells, ","))
		ts.Logger().Errorf("%w", err2)
		return nil, err2
	}
	return sw.logs(), nil
}

// switchWrites is a generic way of migrating write traffic for a workflow.
func (s *Server) switchWrites(ctx context.Context, req *vtctldatapb.WorkflowSwitchTrafficRequest, ts *trafficSwitcher, timeout time.Duration,
	cancel, reverseReplication bool) (journalID int64, dryRunResults *[]string, err error) {

	var sw iswitcher
	if req.DryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, s: s}
	}

	if ts.frozen {
		ts.Logger().Warningf("Writes have already been switched for workflow %s, nothing to do here", ts.WorkflowName())
		return 0, sw.logs(), nil
	}

	if err := ts.validate(ctx); err != nil {
		ts.Logger().Errorf("validate failed: %v", err)
		return 0, nil, err
	}

	if reverseReplication {
		err := areTabletsAvailableToStreamFrom(ctx, req, ts, ts.TargetKeyspaceName(), ts.TargetShards())
		if err != nil {
			return 0, nil, err
		}
	}

	// Need to lock both source and target keyspaces.
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "SwitchWrites")
	if lockErr != nil {
		ts.Logger().Errorf("LockKeyspace failed: %v", lockErr)
		return 0, nil, lockErr
	}
	ctx = tctx
	defer sourceUnlock(&err)
	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "SwitchWrites")
		if lockErr != nil {
			ts.Logger().Errorf("LockKeyspace failed: %v", lockErr)
			return 0, nil, lockErr
		}
		ctx = tctx
		defer targetUnlock(&err)
	}

	// If no journals exist, sourceWorkflows will be initialized by sm.MigrateStreams.
	journalsExist, sourceWorkflows, err := ts.checkJournals(ctx)
	if err != nil {
		ts.Logger().Errorf("checkJournals failed: %v", err)
		return 0, nil, err
	}
	if !journalsExist {
		ts.Logger().Infof("No previous journals were found. Proceeding normally.")
		sm, err := BuildStreamMigrator(ctx, ts, cancel)
		if err != nil {
			ts.Logger().Errorf("buildStreamMigrater failed: %v", err)
			return 0, nil, err
		}
		if cancel {
			sw.cancelMigration(ctx, sm)
			return 0, sw.logs(), nil
		}

		ts.Logger().Infof("Stopping streams")
		sourceWorkflows, err = sw.stopStreams(ctx, sm)
		if err != nil {
			ts.Logger().Errorf("stopStreams failed: %v", err)
			for key, streams := range sm.Streams() {
				for _, stream := range streams {
					ts.Logger().Errorf("stream in stopStreams: key %s shard %s stream %+v", key, stream.BinlogSource.Shard, stream.BinlogSource)
				}
			}
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.Logger().Infof("Stopping source writes")
		if err := sw.stopSourceWrites(ctx); err != nil {
			ts.Logger().Errorf("stopSourceWrites failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
			ts.Logger().Infof("Executing LOCK TABLES on source tables %d times", lockTablesCycles)
			// Doing this twice with a pause in-between to catch any writes that may have raced in between
			// the tablet's deny list check and the first mysqld side table lock.
			for cnt := 1; cnt <= lockTablesCycles; cnt++ {
				if err := ts.executeLockTablesOnSource(ctx); err != nil {
					ts.Logger().Errorf("Failed to execute LOCK TABLES (attempt %d of %d) on sources: %v", cnt, lockTablesCycles, err)
					sw.cancelMigration(ctx, sm)
					return 0, nil, err
				}
				// No need to UNLOCK the tables as the connection was closed once the locks were acquired
				// and thus the locks released.
				time.Sleep(lockTablesCycleDelay)
			}
		}

		ts.Logger().Infof("Waiting for streams to catchup")
		if err := sw.waitForCatchup(ctx, timeout); err != nil {
			ts.Logger().Errorf("waitForCatchup failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.Logger().Infof("Migrating streams")
		if err := sw.migrateStreams(ctx, sm); err != nil {
			ts.Logger().Errorf("migrateStreams failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.Logger().Infof("Resetting sequences")
		if err := sw.resetSequences(ctx); err != nil {
			ts.Logger().Errorf("resetSequences failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}

		ts.Logger().Infof("Creating reverse streams")
		if err := sw.createReverseVReplication(ctx); err != nil {
			ts.Logger().Errorf("createReverseVReplication failed: %v", err)
			sw.cancelMigration(ctx, sm)
			return 0, nil, err
		}
	} else {
		if cancel {
			err := fmt.Errorf("traffic switching has reached the point of no return, cannot cancel")
			ts.Logger().Errorf("%v", err)
			return 0, nil, err
		}
		ts.Logger().Infof("Journals were found. Completing the left over steps.")
		// Need to gather positions in case all journals were not created.
		if err := ts.gatherPositions(ctx); err != nil {
			ts.Logger().Errorf("gatherPositions failed: %v", err)
			return 0, nil, err
		}
	}

	// This is the point of no return. Once a journal is created,
	// traffic can be redirected to target shards.
	if err := sw.createJournals(ctx, sourceWorkflows); err != nil {
		ts.Logger().Errorf("createJournals failed: %v", err)
		return 0, nil, err
	}
	if err := sw.allowTargetWrites(ctx); err != nil {
		ts.Logger().Errorf("allowTargetWrites failed: %v", err)
		return 0, nil, err
	}
	if err := sw.changeRouting(ctx); err != nil {
		ts.Logger().Errorf("changeRouting failed: %v", err)
		return 0, nil, err
	}
	if err := sw.streamMigraterfinalize(ctx, ts, sourceWorkflows); err != nil {
		ts.Logger().Errorf("finalize failed: %v", err)
		return 0, nil, err
	}
	if reverseReplication {
		if err := sw.startReverseVReplication(ctx); err != nil {
			ts.Logger().Errorf("startReverseVReplication failed: %v", err)
			return 0, nil, err
		}
	}

	if err := sw.freezeTargetVReplication(ctx); err != nil {
		ts.Logger().Errorf("deleteTargetVReplication failed: %v", err)
		return 0, nil, err
	}

	return ts.id, sw.logs(), nil
}

func (s *Server) canSwitch(ctx context.Context, ts *trafficSwitcher, state *State, direction TrafficSwitchDirection, maxAllowedReplLagSecs int64) (reason string, err error) {
	if direction == DirectionForward && state.WritesSwitched ||
		direction == DirectionBackward && !state.WritesSwitched {
		log.Infof("writes already switched no need to check lag")
		return "", nil
	}
	wf, err := s.GetWorkflow(ctx, state.TargetKeyspace, state.Workflow)
	if err != nil {
		return "", err
	}
	for _, stream := range wf.ShardStreams {
		for _, st := range stream.GetStreams() {
			if st.Message == Frozen {
				return cannotSwitchFrozen, nil
			}
			// If no new events have been replicated after the copy phase then it will be 0.
			if vreplLag := time.Now().Unix() - st.TimeUpdated.Seconds; vreplLag > maxAllowedReplLagSecs {
				return fmt.Sprintf(cannotSwitchHighLag, vreplLag, maxAllowedReplLagSecs), nil
			}
			switch st.State {
			case binlogdatapb.VReplicationWorkflowState_Copying.String():
				return cannotSwitchCopyIncomplete, nil
			case binlogdatapb.VReplicationWorkflowState_Error.String():
				return cannotSwitchError, nil
			}
		}
	}

	// Ensure that the tablets on both sides are in good shape as we make this same call in the
	// process and an error will cause us to backout.
	refreshErrors := strings.Builder{}
	var m sync.Mutex
	var wg sync.WaitGroup
	rtbsCtx, cancel := context.WithTimeout(ctx, shardTabletRefreshTimeout)
	defer cancel()
	refreshTablets := func(shards []*topo.ShardInfo, stype string) {
		defer wg.Done()
		for _, si := range shards {
			if partial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, s.ts, s.tmc, si, nil, ts.Logger()); err != nil || partial {
				m.Lock()
				refreshErrors.WriteString(fmt.Sprintf("failed to successfully refresh all tablets in the %s/%s %s shard (%v):\n  %v\n",
					si.Keyspace(), si.ShardName(), stype, err, partialDetails))
				m.Unlock()
			}
		}
	}
	wg.Add(1)
	go refreshTablets(ts.SourceShards(), "source")
	wg.Add(1)
	go refreshTablets(ts.TargetShards(), "target")
	wg.Wait()
	if refreshErrors.Len() > 0 {
		return fmt.Sprintf(cannotSwitchFailedTabletRefresh, refreshErrors.String()), nil
	}
	return "", nil
}

// VReplicationExec executes a query remotely using the DBA pool.
func (s *Server) VReplicationExec(ctx context.Context, tabletAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	ti, err := s.ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return nil, err
	}
	return s.tmc.VReplicationExec(ctx, ti.Tablet, query)
}
