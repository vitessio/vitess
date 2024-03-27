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
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vtctl/workflow/common"
	"vitess.io/vitess/go/vt/vtctl/workflow/vexec"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
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

// tableCopyProgress stores the row counts and disk sizes of the source and target tables
type tableCopyProgress struct {
	TargetRowCount, TargetTableSize int64
	SourceRowCount, SourceTableSize int64
}

// copyProgress stores the tableCopyProgress for all tables still being copied
type copyProgress map[string]*tableCopyProgress

// sequenceMetadata contains all of the relevant metadata for a sequence that
// is being used by a table involved in a vreplication workflow.
type sequenceMetadata struct {
	// The name of the sequence table.
	backingTableName string
	// The keyspace where the backing table lives.
	backingTableKeyspace string
	// The dbName in use by the keyspace where the backing table lives.
	backingTableDBName string
	// The name of the table using the sequence.
	usingTableName string
	// The dbName in use by the keyspace where the using table lives.
	usingTableDBName string
	// The using table definition.
	usingTableDefinition *vschemapb.Table
}

// vdiffOutput holds the data from all shards that is needed to generate
// the full summary results of the vdiff in the vdiff show command output.
type vdiffOutput struct {
	mu        sync.Mutex
	responses map[string]*tabletmanagerdatapb.VDiffResponse
	err       error
}

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
	// Limit the number of concurrent background goroutines if needed.
	sem *semaphore.Weighted
	env *vtenv.Environment
}

// NewServer returns a new server instance with the given topo.Server and
// TabletManagerClient.
func NewServer(env *vtenv.Environment, ts *topo.Server, tmc tmclient.TabletManagerClient) *Server {
	return &Server{
		ts:  ts,
		tmc: tmc,
		env: env,
	}
}

func (s *Server) SQLParser() *sqlparser.Parser {
	return s.env.Parser()
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
			return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rule target is not correctly formatted: %s", ruleTarget)
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

func (s *Server) GetWorkflow(ctx context.Context, keyspace, workflow string, includeLogs bool, shards []string) (*vtctldatapb.Workflow, error) {
	res, err := s.GetWorkflows(ctx, &vtctldatapb.GetWorkflowsRequest{
		Keyspace:    keyspace,
		Workflow:    workflow,
		IncludeLogs: includeLogs,
		Shards:      shards,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s workflow not found in the %s keyspace", workflow, keyspace)
	}
	if len(res.Workflows) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected number of workflows returned for %s.%s; expected 1, got %d",
			keyspace, workflow, len(res.Workflows))
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
	span.Annotate("workflow", req.Workflow)
	span.Annotate("active_only", req.ActiveOnly)
	span.Annotate("include_logs", req.IncludeLogs)
	span.Annotate("shards", req.Shards)

	readReq := &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{}
	if req.Workflow != "" {
		readReq.IncludeWorkflows = []string{req.Workflow}
	}
	if req.ActiveOnly {
		readReq.ExcludeStates = []binlogdatapb.VReplicationWorkflowState{binlogdatapb.VReplicationWorkflowState_Stopped}
	}

	// Guards access to the maps used throughout.
	m := sync.Mutex{}

	shards, err := common.GetShards(ctx, s.ts, req.Keyspace, req.Shards)
	if err != nil {
		return nil, err
	}
	results := make(map[*topo.TabletInfo]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, len(shards))
	readWorkflowsEg, readWorkflowsCtx := errgroup.WithContext(ctx)
	for _, shard := range shards {
		readWorkflowsEg.Go(func() error {
			si, err := s.ts.GetShard(readWorkflowsCtx, req.Keyspace, shard)
			if err != nil {
				return err
			}
			if si.PrimaryAlias == nil {
				return fmt.Errorf("%w %s/%s", vexec.ErrNoShardPrimary, req.Keyspace, shard)
			}
			primary, err := s.ts.GetTablet(readWorkflowsCtx, si.PrimaryAlias)
			if err != nil {
				return err
			}
			if primary == nil {
				return fmt.Errorf("%w %s/%s: tablet %v not found", vexec.ErrNoShardPrimary, req.Keyspace, shard, topoproto.TabletAliasString(si.PrimaryAlias))
			}
			// Clone the request so that we can set the correct DB name for tablet.
			req := readReq.CloneVT()
			wres, err := s.tmc.ReadVReplicationWorkflows(readWorkflowsCtx, primary.Tablet, req)
			if err != nil {
				return err
			}
			m.Lock()
			defer m.Unlock()
			results[primary] = wres
			return nil
		})
	}
	if readWorkflowsEg.Wait() != nil {
		return nil, err
	}

	copyStatesByShardStreamId := make(map[string][]*vtctldatapb.Workflow_Stream_CopyState, len(results))

	fetchCopyStates := func(ctx context.Context, tablet *topo.TabletInfo, streamIds []int32) error {
		span, ctx := trace.NewSpan(ctx, "workflow.Server.fetchCopyStates")
		defer span.Finish()

		span.Annotate("keyspace", req.Keyspace)
		span.Annotate("shard", tablet.Shard)
		span.Annotate("tablet_alias", tablet.AliasString())

		copyStates, err := s.getWorkflowCopyStates(ctx, tablet, streamIds)
		if err != nil {
			return err
		}

		m.Lock()
		defer m.Unlock()

		for _, copyState := range copyStates {
			shardStreamId := fmt.Sprintf("%s/%d", tablet.Shard, copyState.StreamId)
			copyStatesByShardStreamId[shardStreamId] = append(
				copyStatesByShardStreamId[shardStreamId],
				copyState,
			)
		}

		return nil
	}

	fetchCopyStatesEg, fetchCopyStatesCtx := errgroup.WithContext(ctx)
	for tablet, result := range results {
		tablet := tablet // loop closure

		streamIds := make([]int32, 0, len(result.Workflows))
		for _, wf := range result.Workflows {
			for _, stream := range wf.Streams {
				streamIds = append(streamIds, stream.Id)
			}
		}

		if len(streamIds) == 0 {
			continue
		}

		fetchCopyStatesEg.Go(func() error {
			return fetchCopyStates(fetchCopyStatesCtx, tablet, streamIds)
		})
	}

	if err := fetchCopyStatesEg.Wait(); err != nil {
		return nil, err
	}

	workflowsMap := make(map[string]*vtctldatapb.Workflow, len(results))
	sourceKeyspaceByWorkflow := make(map[string]string, len(results))
	sourceShardsByWorkflow := make(map[string]sets.Set[string], len(results))
	targetKeyspaceByWorkflow := make(map[string]string, len(results))
	targetShardsByWorkflow := make(map[string]sets.Set[string], len(results))
	maxVReplicationLagByWorkflow := make(map[string]float64, len(results))
	maxVReplicationTransactionLagByWorkflow := make(map[string]float64, len(results))

	// We guarantee the following invariants when this function is called for a
	// given workflow:
	// - workflow.Name != "" (more precisely, ".Name is set 'properly'")
	// - workflowsMap[workflow.Name] == workflow
	// - sourceShardsByWorkflow[workflow.Name] != nil
	// - targetShardsByWorkflow[workflow.Name] != nil
	// - workflow.ShardStatuses != nil
	scanWorkflow := func(ctx context.Context, workflow *vtctldatapb.Workflow, res *tabletmanagerdatapb.ReadVReplicationWorkflowResponse, tablet *topo.TabletInfo) error {
		// This is not called concurrently, but we still protect the maps to ensure
		// that we're concurrency-safe in the face of future changes (e.g. where other
		// things are running concurrently with this which also access these maps).
		m.Lock()
		defer m.Unlock()
		for _, rstream := range res.Streams {
			// The value in the pos column can be compressed and thus not
			// have a valid GTID consisting of valid UTF-8 characters so we
			// have to decode it so that it's properly decompressed first
			// when needed.
			pos := rstream.Pos
			if pos != "" {
				mpos, err := binlogplayer.DecodePosition(pos)
				if err != nil {
					return err
				}
				pos = mpos.String()
			}

			cells := strings.Split(res.Cells, ",")
			for i := range cells {
				cells[i] = strings.TrimSpace(cells[i])
			}

			stream := &vtctldatapb.Workflow_Stream{
				Id:                        int64(rstream.Id),
				Shard:                     tablet.Shard,
				Tablet:                    tablet.Alias,
				BinlogSource:              rstream.Bls,
				Position:                  pos,
				StopPosition:              rstream.StopPos,
				State:                     rstream.State.String(),
				DbName:                    tablet.DbName(),
				TabletTypes:               res.TabletTypes,
				TabletSelectionPreference: res.TabletSelectionPreference,
				Cells:                     cells,
				TransactionTimestamp:      rstream.TransactionTimestamp,
				TimeUpdated:               rstream.TimeUpdated,
				Message:                   rstream.Message,
				Tags:                      strings.Split(res.Tags, ","),
				RowsCopied:                rstream.RowsCopied,
				ThrottlerStatus: &vtctldatapb.Workflow_Stream_ThrottlerStatus{
					ComponentThrottled: rstream.ComponentThrottled,
					TimeThrottled:      rstream.TimeThrottled,
				},
			}

			// Merge in copy states, which we've already fetched.
			shardStreamId := fmt.Sprintf("%s/%d", tablet.Shard, stream.Id)
			if copyState, ok := copyStatesByShardStreamId[shardStreamId]; ok {
				stream.CopyStates = copyState
			}

			if rstream.TimeUpdated == nil {
				rstream.TimeUpdated = &vttimepb.Time{}
			}

			switch {
			case strings.Contains(strings.ToLower(stream.Message), "error"):
				stream.State = binlogdatapb.VReplicationWorkflowState_Error.String()
			case stream.State == binlogdatapb.VReplicationWorkflowState_Running.String() && len(stream.CopyStates) > 0:
				stream.State = binlogdatapb.VReplicationWorkflowState_Copying.String()
			case stream.State == binlogdatapb.VReplicationWorkflowState_Running.String() && int64(time.Now().Second())-rstream.TimeUpdated.Seconds > 10:
				stream.State = binlogdatapb.VReplicationWorkflowState_Lagging.String()
			}

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
				return vterrors.Wrapf(ErrMultipleSourceKeyspaces, "workflow = %v, ks1 = %v, ks2 = %v", workflow.Name, ks, stream.BinlogSource.Keyspace)
			}

			sourceKeyspaceByWorkflow[workflow.Name] = stream.BinlogSource.Keyspace

			if ks, ok := targetKeyspaceByWorkflow[workflow.Name]; ok && ks != tablet.Keyspace {
				return vterrors.Wrapf(ErrMultipleTargetKeyspaces, "workflow = %v, ks1 = %v, ks2 = %v", workflow.Name, ks, tablet.Keyspace)
			}

			targetKeyspaceByWorkflow[workflow.Name] = tablet.Keyspace

			if stream.TimeUpdated == nil {
				stream.TimeUpdated = &vttimepb.Time{}
			}
			timeUpdated := time.Unix(stream.TimeUpdated.Seconds, 0)
			vreplicationLag := time.Since(timeUpdated)

			// MaxVReplicationLag represents the time since we last processed any event
			// in the workflow.
			if currentMaxLag, ok := maxVReplicationLagByWorkflow[workflow.Name]; ok {
				if vreplicationLag.Seconds() > currentMaxLag {
					maxVReplicationLagByWorkflow[workflow.Name] = vreplicationLag.Seconds()
				}
			} else {
				maxVReplicationLagByWorkflow[workflow.Name] = vreplicationLag.Seconds()
			}

			workflow.WorkflowType = res.WorkflowType.String()
			workflow.WorkflowSubType = res.WorkflowSubType.String()
			workflow.DeferSecondaryKeys = res.DeferSecondaryKeys

			// MaxVReplicationTransactionLag estimates the actual statement processing lag
			// between the source and the target. If we are still processing source events it
			// is the difference b/w current time and the timestamp of the last event. If
			// heartbeats are more recent than the last event, then the lag is the time since
			// the last heartbeat as there can be an actual event immediately after the
			// heartbeat, but which has not yet been processed on the target.
			// We don't allow switching during the copy phase, so in that case we just return
			// a large lag. All timestamps are in seconds since epoch.
			if _, ok := maxVReplicationTransactionLagByWorkflow[workflow.Name]; !ok {
				maxVReplicationTransactionLagByWorkflow[workflow.Name] = 0
			}
			if rstream.TransactionTimestamp == nil {
				rstream.TransactionTimestamp = &vttimepb.Time{}
			}
			lastTransactionTime := rstream.TransactionTimestamp.Seconds
			if rstream.TimeHeartbeat == nil {
				rstream.TimeHeartbeat = &vttimepb.Time{}
			}
			lastHeartbeatTime := rstream.TimeHeartbeat.Seconds
			if stream.State == binlogdatapb.VReplicationWorkflowState_Copying.String() {
				maxVReplicationTransactionLagByWorkflow[workflow.Name] = math.MaxInt64
			} else {
				if lastTransactionTime == 0 /* no new events after copy */ ||
					lastHeartbeatTime > lastTransactionTime /* no recent transactions, so all caught up */ {

					lastTransactionTime = lastHeartbeatTime
				}
				now := time.Now().Unix() /* seconds since epoch */
				transactionReplicationLag := float64(now - lastTransactionTime)
				if transactionReplicationLag > maxVReplicationTransactionLagByWorkflow[workflow.Name] {
					maxVReplicationTransactionLagByWorkflow[workflow.Name] = transactionReplicationLag
				}
			}
		}

		return nil
	}

	for tablet, result := range results {
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
		for _, wfres := range result.Workflows {
			workflowName := wfres.Workflow
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

			if err := scanWorkflow(ctx, workflow, wfres, tablet); err != nil {
				return nil, err
			}
		}
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
WHERE vrepl_id IN %a
ORDER BY
	vrepl_id ASC,
	id ASC
`)
	)

	fetchStreamLogs := func(ctx context.Context, workflow *vtctldatapb.Workflow) {
		span, ctx := trace.NewSpan(ctx, "workflow.Server.fetchStreamLogs")
		defer span.Finish()

		span.Annotate("keyspace", req.Keyspace)
		span.Annotate("workflow", workflow.Name)

		vreplIDs := make([]int64, 0, len(workflow.ShardStreams))
		for _, shardStream := range maps.Values(workflow.ShardStreams) {
			for _, stream := range shardStream.Streams {
				vreplIDs = append(vreplIDs, stream.Id)
			}
		}
		idsBV, err := sqltypes.BuildBindVariable(vreplIDs)
		if err != nil {
			return
		}

		query, err := sqlparser.ParseAndBind(vrepLogQuery, idsBV)
		if err != nil {
			return
		}

		vx := vexec.NewVExec(req.Keyspace, workflow.Name, s.ts, s.tmc, s.SQLParser())
		results, err := vx.QueryContext(ctx, query)
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
				id, err := row[0].ToCastInt64()
				if err != nil {
					markErrors(err)
					continue
				}

				streamID, err := row[1].ToCastInt64()
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

				count, err := row[7].ToCastInt64()
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
						// This can happen on manual/failed workflow cleanup so keep going.
						continue
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
			return nil, vterrors.Wrapf(ErrInvalidWorkflow, "%s has no source shards", name)
		}

		sourceKeyspace, ok := sourceKeyspaceByWorkflow[name]
		if !ok {
			return nil, vterrors.Wrapf(ErrInvalidWorkflow, "%s has no source keyspace", name)
		}

		targetShards, ok := targetShardsByWorkflow[name]
		if !ok {
			return nil, vterrors.Wrapf(ErrInvalidWorkflow, "%s has no target shards", name)
		}

		targetKeyspace, ok := targetKeyspaceByWorkflow[name]
		if !ok {
			return nil, vterrors.Wrapf(ErrInvalidWorkflow, "%s has no target keyspace", name)
		}

		maxVReplicationLag, ok := maxVReplicationLagByWorkflow[name]
		if !ok {
			return nil, vterrors.Wrapf(ErrInvalidWorkflow, "%s has no tracked vreplication lag", name)
		}

		maxVReplicationTransactionLag, ok := maxVReplicationTransactionLagByWorkflow[name]
		if !ok {
			return nil, vterrors.Wrapf(ErrInvalidWorkflow, "%s has no tracked vreplication transaction lag", name)
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
		workflow.MaxVReplicationTransactionLag = int64(maxVReplicationTransactionLag)

		// Sort shard streams by stream_id ASC, to support an optimization
		// in fetchStreamLogs below.
		for _, shardStreams := range workflow.ShardStreams {
			sort.Slice(shardStreams.Streams, func(i, j int) bool {
				return shardStreams.Streams[i].Id < shardStreams.Streams[j].Id
			})
		}

		workflows = append(workflows, workflow)

		if req.IncludeLogs {
			// Fetch logs for all streams associated with this workflow in the background.
			fetchLogsWG.Add(1)
			go func(ctx context.Context, workflow *vtctldatapb.Workflow) {
				defer fetchLogsWG.Done()
				fetchStreamLogs(ctx, workflow)
			}(ctx, workflow)
		}
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

	if ts.workflowType == binlogdatapb.VReplicationWorkflowType_CreateLookupIndex {
		// Nothing left to do.
		return ts, state, nil
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
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no tables in workflow %s.%s", targetKeyspace, workflowName)
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
	if ts.workflowType == binlogdatapb.VReplicationWorkflowType_Migrate {
		state.WorkflowType = TypeMigrate
	}

	return ts, state, nil
}

func (s *Server) getWorkflowCopyStates(ctx context.Context, tablet *topo.TabletInfo, streamIds []int32) ([]*vtctldatapb.Workflow_Stream_CopyState, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.getWorkflowCopyStates")
	defer span.Finish()

	span.Annotate("keyspace", tablet.Keyspace)
	span.Annotate("shard", tablet.Shard)
	span.Annotate("tablet_alias", tablet.AliasString())
	span.Annotate("stream_ids", fmt.Sprintf("%#v", streamIds))

	idsBV, err := sqltypes.BuildBindVariable(streamIds)
	if err != nil {
		return nil, err
	}
	query, err := sqlparser.ParseAndBind("select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in %a and id in (select max(id) from _vt.copy_state where vrepl_id in %a group by vrepl_id, table_name)",
		idsBV, idsBV)
	if err != nil {
		return nil, err
	}
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
		streamId, err := row[0].ToInt64()
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to cast vrepl_id to int64: %v", err)
		}
		// These string fields are technically varbinary, but this is close enough.
		copyStates[i] = &vtctldatapb.Workflow_Stream_CopyState{
			StreamId: streamId,
			Table:    row[1].ToString(),
			LastPk:   row[2].ToString(),
		}
	}

	return copyStates, nil
}

// LookupVindexCreate creates the lookup vindex in the specified
// keyspace and creates a VReplication workflow to backfill that
// vindex from the keyspace to the target/lookup table specified.
func (s *Server) LookupVindexCreate(ctx context.Context, req *vtctldatapb.LookupVindexCreateRequest) (*vtctldatapb.LookupVindexCreateResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.LookupVindexCreate")
	defer span.Finish()

	span.Annotate("workflow", req.Workflow)
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("continue_after_copy_with_owner", req.ContinueAfterCopyWithOwner)
	span.Annotate("cells", req.Cells)
	span.Annotate("tablet_types", req.TabletTypes)

	ms, sourceVSchema, targetVSchema, err := s.prepareCreateLookup(ctx, req.Workflow, req.Keyspace, req.Vindex, req.ContinueAfterCopyWithOwner)
	if err != nil {
		return nil, err
	}
	if err := s.ts.SaveVSchema(ctx, ms.TargetKeyspace, targetVSchema); err != nil {
		return nil, err
	}

	ms.TabletTypes = topoproto.MakeStringTypeCSV(req.TabletTypes)
	ms.TabletSelectionPreference = req.TabletSelectionPreference
	if err := s.Materialize(ctx, ms); err != nil {
		return nil, err
	}
	if err := s.ts.SaveVSchema(ctx, req.Keyspace, sourceVSchema); err != nil {
		return nil, err
	}
	if err := s.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		return nil, err
	}

	return &vtctldatapb.LookupVindexCreateResponse{}, nil
}

// LookupVindexExternalize externalizes a lookup vindex that's
// finished backfilling or has caught up. If the vindex has an
// owner then the workflow will also be deleted.
func (s *Server) LookupVindexExternalize(ctx context.Context, req *vtctldatapb.LookupVindexExternalizeRequest) (*vtctldatapb.LookupVindexExternalizeResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.LookupVindexExternalize")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("name", req.Name)
	span.Annotate("table_keyspace", req.TableKeyspace)

	// Find the lookup vindex by by name.
	sourceVschema, err := s.ts.GetVSchema(ctx, req.Keyspace)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get vschema for the %s keyspace", req.Keyspace)
	}
	vindex := sourceVschema.Vindexes[req.Name]
	if vindex == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vindex %s not found in the %s keyspace", req.Name, req.Keyspace)
	}

	targetShards, err := s.ts.GetServingShards(ctx, req.TableKeyspace)
	if err != nil {
		return nil, err
	}

	// Create a parallelizer function.
	forAllTargets := func(f func(*topo.ShardInfo) error) error {
		var wg sync.WaitGroup
		allErrors := &concurrency.AllErrorRecorder{}
		for _, targetShard := range targetShards {
			wg.Add(1)
			go func(targetShard *topo.ShardInfo) {
				defer wg.Done()

				if err := f(targetShard); err != nil {
					allErrors.RecordError(err)
				}
			}(targetShard)
		}
		wg.Wait()
		return allErrors.AggrError(vterrors.Aggregate)
	}

	err = forAllTargets(func(targetShard *topo.ShardInfo) error {
		targetPrimary, err := s.ts.GetTablet(ctx, targetShard.PrimaryAlias)
		if err != nil {
			return err
		}
		res, err := s.tmc.ReadVReplicationWorkflow(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
			Workflow: req.Name,
		})
		if err != nil {
			return err
		}
		if res == nil {
			return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "workflow %s not found on %v", req.Name, targetPrimary.Alias)
		}
		for _, stream := range res.Streams {
			if stream.Bls.Filter == nil || len(stream.Bls.Filter.Rules) != 1 {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid binlog source")
			}
			if vindex.Owner == "" || !stream.Bls.StopAfterCopy {
				// If there's no owner or we've requested that the workflow NOT be stopped
				// after the copy phase completes, then all streams need to be running.
				if stream.State != binlogdatapb.VReplicationWorkflowState_Running {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "stream %d for %v.%v is not in Running state: %v", stream.Id, targetShard.Keyspace(), targetShard.ShardName(), stream.State)
				}
			} else {
				// If there is an owner, all streams need to be stopped after copy.
				if stream.State != binlogdatapb.VReplicationWorkflowState_Stopped || !strings.Contains(stream.Message, "Stopped after copy") {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "stream %d for %v.%v is not in Stopped after copy state: %v, %v", stream.Id, targetShard.Keyspace(), targetShard.ShardName(), stream.State, stream.Message)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	resp := &vtctldatapb.LookupVindexExternalizeResponse{}

	if vindex.Owner != "" {
		// If there is an owner, we have to delete the streams. Once we externalize it
		// the VTGate will now be responsible for keeping the lookup table up to date
		// with the owner table.
		if _, derr := s.WorkflowDelete(ctx, &vtctldatapb.WorkflowDeleteRequest{
			Keyspace:         req.TableKeyspace,
			Workflow:         req.Name,
			KeepData:         true, // Not relevant
			KeepRoutingRules: true, // Not relevant
		}); derr != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to delete workflow %s: %v", req.Name, derr)
		}
		resp.WorkflowDeleted = true
	}

	// Remove the write_only param and save the source vschema.
	delete(vindex.Params, "write_only")
	if err := s.ts.SaveVSchema(ctx, req.Keyspace, sourceVschema); err != nil {
		return nil, err
	}
	return resp, s.ts.RebuildSrvVSchema(ctx, nil)
}

// Materialize performs the steps needed to materialize a list of
// tables based on the materialization specs.
func (s *Server) Materialize(ctx context.Context, ms *vtctldatapb.MaterializeSettings) error {
	mz := &materializer{
		ctx:      ctx,
		ts:       s.ts,
		sourceTs: s.ts,
		tmc:      s.tmc,
		ms:       ms,
		env:      s.env,
	}

	tt, err := topoproto.ParseTabletTypes(ms.TabletTypes)
	if err != nil {
		return err
	}

	cells := strings.Split(ms.Cell, ",")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}

	err = mz.createWorkflowStreams(&tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
		Workflow:                  ms.Workflow,
		Cells:                     strings.Split(ms.Cell, ","),
		TabletTypes:               tt,
		TabletSelectionPreference: ms.TabletSelectionPreference,
		WorkflowType:              mz.getWorkflowType(),
		DeferSecondaryKeys:        ms.DeferSecondaryKeys,
		AutoStart:                 true,
		StopAfterCopy:             ms.StopAfterCopy,
	})
	if err != nil {
		return err
	}
	return mz.startStreams(ctx)
}

// MoveTablesCreate is part of the vtctlservicepb.VtctldServer interface.
// It passes the embedded TabletRequest object to the given keyspace's
// target primary tablets that will be executing the workflow.
func (s *Server) MoveTablesCreate(ctx context.Context, req *vtctldatapb.MoveTablesCreateRequest) (res *vtctldatapb.WorkflowStatusResponse, err error) {
	return s.moveTablesCreate(ctx, req, binlogdatapb.VReplicationWorkflowType_MoveTables)
}

func (s *Server) moveTablesCreate(ctx context.Context, req *vtctldatapb.MoveTablesCreateRequest,
	workflowType binlogdatapb.VReplicationWorkflowType,
) (res *vtctldatapb.WorkflowStatusResponse, err error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.moveTablesCreate")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("workflow_type", workflowType)
	span.Annotate("cells", req.Cells)
	span.Annotate("tablet_types", req.TabletTypes)
	span.Annotate("on_ddl", req.OnDdl)

	sourceKeyspace := req.SourceKeyspace
	targetKeyspace := req.TargetKeyspace
	// FIXME validate tableSpecs, allTables, excludeTables
	var (
		tables       = req.IncludeTables
		externalTopo *topo.Server
		sourceTopo   = s.ts
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
	var origVSchema *vschemapb.Keyspace // If we need to rollback a failed create
	vschema, err = s.ts.GetVSchema(ctx, targetKeyspace)
	if err != nil {
		return nil, err
	}
	if vschema == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no vschema found for target keyspace %s", targetKeyspace)
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
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no tables to move")
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
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no tables to move")
	}
	log.Infof("Found tables to move: %s", strings.Join(tables, ","))

	if !vschema.Sharded {
		// Save the original in case we need to restore it for a late failure
		// in the defer().
		origVSchema = vschema.CloneVT()
		if err := s.addTablesToVSchema(ctx, sourceKeyspace, vschema, tables, externalTopo == nil); err != nil {
			return nil, err
		}
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
		AtomicCopy:                req.AtomicCopy,
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
		ctx:          ctx,
		ts:           s.ts,
		sourceTs:     sourceTopo,
		tmc:          s.tmc,
		ms:           ms,
		workflowType: workflowType,
		env:          s.env,
	}
	err = mz.createWorkflowStreams(&tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
		Workflow:                  req.Workflow,
		Cells:                     req.Cells,
		TabletTypes:               req.TabletTypes,
		TabletSelectionPreference: req.TabletSelectionPreference,
		WorkflowType:              mz.workflowType,
		DeferSecondaryKeys:        req.DeferSecondaryKeys,
		AutoStart:                 req.AutoStart,
		StopAfterCopy:             req.StopAfterCopy,
	})
	if err != nil {
		return nil, err
	}

	// If we get an error after this point, where the vreplication streams/records
	// have been created, then we clean up the workflow's artifacts.
	defer func() {
		if err != nil {
			ts, cerr := s.buildTrafficSwitcher(ctx, ms.TargetKeyspace, ms.Workflow)
			if cerr != nil {
				err = vterrors.Wrapf(err, "failed to cleanup workflow artifacts: %v", cerr)
			}
			if cerr := s.dropArtifacts(ctx, false, &switcher{s: s, ts: ts}); cerr != nil {
				err = vterrors.Wrapf(err, "failed to cleanup workflow artifacts: %v", cerr)
			}
			if origVSchema == nil { // There's no previous version to restore
				return
			}
			if cerr := s.ts.SaveVSchema(ctx, targetKeyspace, origVSchema); cerr != nil {
				err = vterrors.Wrapf(err, "failed to restore original target vschema: %v", cerr)
			}
		}
	}()

	// Now that the streams have been successfully created, let's put the associated
	// routing rules in place.
	if externalTopo == nil {
		if req.NoRoutingRules {
			log.Warningf("Found --no-routing-rules flag, not creating routing rules for workflow %s.%s", targetKeyspace, req.Workflow)
		} else {
			// Save routing rules before vschema. If we save vschema first, and routing
			// rules fails to save, we may generate duplicate table errors.
			if mz.isPartial {
				if err := createDefaultShardRoutingRules(mz.ctx, mz.ms, mz.ts); err != nil {
					return nil, err
				}
			}

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
		}

		// We added to the vschema.
		if err := s.ts.SaveVSchema(ctx, targetKeyspace, vschema); err != nil {
			return nil, err
		}
	}
	if err := s.ts.RebuildSrvVSchema(ctx, nil); err != nil {
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
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, msg)
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
// Note: this is currently re-used for Reshard as well.
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

// ReshardCreate is part of the vtctlservicepb.VtctldServer interface.
func (s *Server) ReshardCreate(ctx context.Context, req *vtctldatapb.ReshardCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.ReshardCreate")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("source_shards", req.SourceShards)
	span.Annotate("target_shards", req.TargetShards)
	span.Annotate("cells", req.Cells)
	span.Annotate("tablet_types", req.TabletTypes)
	span.Annotate("on_ddl", req.OnDdl)

	keyspace := req.Keyspace
	cells := req.Cells
	// TODO: validate workflow does not exist.

	if err := s.ts.ValidateSrvKeyspace(ctx, keyspace, strings.Join(cells, ",")); err != nil {
		err2 := vterrors.Wrapf(err, "SrvKeyspace for keyspace %s is corrupt for cell(s) %s", keyspace, cells)
		log.Errorf("%w", err2)
		return nil, err
	}
	tabletTypesStr := discovery.BuildTabletTypesString(req.TabletTypes, req.TabletSelectionPreference)
	rs, err := s.buildResharder(ctx, keyspace, req.Workflow, req.SourceShards, req.TargetShards, strings.Join(cells, ","), tabletTypesStr)
	if err != nil {
		return nil, vterrors.Wrap(err, "buildResharder")
	}
	rs.onDDL = req.OnDdl
	rs.stopAfterCopy = req.StopAfterCopy
	rs.deferSecondaryKeys = req.DeferSecondaryKeys
	if !req.SkipSchemaCopy {
		if err := rs.copySchema(ctx); err != nil {
			return nil, vterrors.Wrap(err, "copySchema")
		}
	}
	if err := rs.createStreams(ctx); err != nil {
		return nil, vterrors.Wrap(err, "createStreams")
	}

	if req.AutoStart {
		if err := rs.startStreams(ctx); err != nil {
			return nil, vterrors.Wrap(err, "startStreams")
		}
	} else {
		log.Warningf("Streams will not be started since --auto-start is set to false")
	}
	return s.WorkflowStatus(ctx, &vtctldatapb.WorkflowStatusRequest{
		Keyspace: req.Keyspace,
		Workflow: req.Workflow,
	})
}

// VDiffCreate is part of the vtctlservicepb.VtctldServer interface.
// It passes on the request to the target primary tablets that are
// participating in the given workflow and VDiff.
func (s *Server) VDiffCreate(ctx context.Context, req *vtctldatapb.VDiffCreateRequest) (*vtctldatapb.VDiffCreateResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.VDiffCreate")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("uuid", req.Uuid)
	span.Annotate("source_cells", req.SourceCells)
	span.Annotate("target_cells", req.TargetCells)
	span.Annotate("tablet_types", req.TabletTypes)
	span.Annotate("tables", req.Tables)
	span.Annotate("auto_retry", req.AutoRetry)
	span.Annotate("max_diff_duration", req.MaxDiffDuration)

	tabletTypesStr := discovery.BuildTabletTypesString(req.TabletTypes, req.TabletSelectionPreference)

	// This is a pointer so there's no ZeroValue in the message
	// and an older v18 client will not provide it.
	if req.MaxDiffDuration == nil {
		req.MaxDiffDuration = &vttimepb.Duration{}
	}
	// The other vttime.Duration vars should not be nil as the
	// client should always provide them, but we check anyway to
	// be safe.
	if req.FilteredReplicationWaitTime == nil {
		req.FilteredReplicationWaitTime = &vttimepb.Duration{}
	}
	if req.WaitUpdateInterval == nil {
		req.WaitUpdateInterval = &vttimepb.Duration{}
	}

	options := &tabletmanagerdatapb.VDiffOptions{
		PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{
			TabletTypes: tabletTypesStr,
			SourceCell:  strings.Join(req.SourceCells, ","),
			TargetCell:  strings.Join(req.TargetCells, ","),
		},
		CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
			Tables:                strings.Join(req.Tables, ","),
			AutoRetry:             req.AutoRetry,
			MaxRows:               req.Limit,
			TimeoutSeconds:        req.FilteredReplicationWaitTime.Seconds,
			MaxExtraRowsToCompare: req.MaxExtraRowsToCompare,
			UpdateTableStats:      req.UpdateTableStats,
			MaxDiffSeconds:        req.MaxDiffDuration.Seconds,
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			OnlyPks:       req.OnlyPKs,
			DebugQuery:    req.DebugQuery,
			MaxSampleRows: req.MaxReportSampleRows,
		},
	}

	tabletreq := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  req.TargetKeyspace,
		Workflow:  req.Workflow,
		Action:    string(vdiff.CreateAction),
		Options:   options,
		VdiffUuid: req.Uuid,
	}

	ts, err := s.buildTrafficSwitcher(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}
	if ts.frozen {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid VDiff run: writes have been already been switched for workflow %s.%s",
			req.TargetKeyspace, req.Workflow)
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		log.Errorf("Error executing vdiff create action: %v", err)
		return nil, err
	}

	return &vtctldatapb.VDiffCreateResponse{
		UUID: req.Uuid,
	}, nil
}

// VDiffDelete is part of the vtctlservicepb.VtctldServer interface.
func (s *Server) VDiffDelete(ctx context.Context, req *vtctldatapb.VDiffDeleteRequest) (*vtctldatapb.VDiffDeleteResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.VDiffDelete")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("argument", req.Arg)

	tabletreq := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  req.TargetKeyspace,
		Workflow:  req.Workflow,
		Action:    string(vdiff.DeleteAction),
		ActionArg: req.Arg,
	}

	ts, err := s.buildTrafficSwitcher(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		log.Errorf("Error executing vdiff delete action: %v", err)
		return nil, err
	}

	return &vtctldatapb.VDiffDeleteResponse{}, nil
}

// VDiffResume is part of the vtctlservicepb.VtctldServer interface.
func (s *Server) VDiffResume(ctx context.Context, req *vtctldatapb.VDiffResumeRequest) (*vtctldatapb.VDiffResumeResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.VDiffResume")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("uuid", req.Uuid)

	tabletreq := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  req.TargetKeyspace,
		Workflow:  req.Workflow,
		Action:    string(vdiff.ResumeAction),
		VdiffUuid: req.Uuid,
	}

	ts, err := s.buildTrafficSwitcher(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		log.Errorf("Error executing vdiff resume action: %v", err)
		return nil, err
	}

	return &vtctldatapb.VDiffResumeResponse{}, nil
}

// VDiffShow is part of the vtctlservicepb.VtctldServer interface.
func (s *Server) VDiffShow(ctx context.Context, req *vtctldatapb.VDiffShowRequest) (*vtctldatapb.VDiffShowResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.VDiffShow")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("argument", req.Arg)

	tabletreq := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  req.TargetKeyspace,
		Workflow:  req.Workflow,
		Action:    string(vdiff.ShowAction),
		ActionArg: req.Arg,
	}

	ts, err := s.buildTrafficSwitcher(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	output := &vdiffOutput{
		responses: make(map[string]*tabletmanagerdatapb.VDiffResponse, len(ts.targets)),
		err:       nil,
	}
	output.err = ts.ForAllTargets(func(target *MigrationTarget) error {
		resp, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		output.mu.Lock()
		defer output.mu.Unlock()
		output.responses[target.GetShard().ShardName()] = resp
		return err
	})
	if output.err != nil {
		log.Errorf("Error executing vdiff show action: %v", output.err)
		return nil, output.err
	}
	return &vtctldatapb.VDiffShowResponse{
		TabletResponses: output.responses,
	}, nil
}

// VDiffStop is part of the vtctlservicepb.VtctldServer interface.
func (s *Server) VDiffStop(ctx context.Context, req *vtctldatapb.VDiffStopRequest) (*vtctldatapb.VDiffStopResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.VDiffStop")
	defer span.Finish()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("uuid", req.Uuid)

	tabletreq := &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  req.TargetKeyspace,
		Workflow:  req.Workflow,
		Action:    string(vdiff.StopAction),
		VdiffUuid: req.Uuid,
	}

	ts, err := s.buildTrafficSwitcher(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		log.Errorf("Error executing vdiff stop action: %v", err)
		return nil, err
	}

	return &vtctldatapb.VDiffStopResponse{}, nil
}

// WorkflowDelete is part of the vtctlservicepb.VtctldServer interface.
// It passes on the request to the target primary tablets that are
// participating in the given workflow.
func (s *Server) WorkflowDelete(ctx context.Context, req *vtctldatapb.WorkflowDeleteRequest) (*vtctldatapb.WorkflowDeleteResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.WorkflowDelete")
	defer span.Finish()

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("keep_data", req.KeepData)
	span.Annotate("keep_routing_rules", req.KeepRoutingRules)
	span.Annotate("shards", req.Shards)

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
	vx := vexec.NewVExec(req.Keyspace, req.Workflow, s.ts, s.tmc, s.env.Parser())
	vx.SetShardSubset(req.Shards)
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
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the %s workflow does not exist in the %s keyspace", req.Workflow, req.Keyspace)
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
	resp := &vtctldatapb.WorkflowStatusResponse{
		TrafficState: state.String(),
	}
	if copyProgress != nil {
		resp.TableCopyState = make(map[string]*vtctldatapb.WorkflowStatusResponse_TableCopyState, len(*copyProgress))
		// We sort the tables for intuitive and consistent output.
		var tables []string
		for table := range *copyProgress {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		var progress tableCopyProgress
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

	workflow, err := s.GetWorkflow(ctx, req.Keyspace, req.Workflow, false, req.Shards)
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
				if st.TransactionTimestamp == nil {
					st.TransactionTimestamp = &vttimepb.Time{}
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
func (s *Server) GetCopyProgress(ctx context.Context, ts *trafficSwitcher, state *State) (*copyProgress, error) {
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

	getTableMetrics := func(tablet *topodatapb.Tablet, query string, rowCounts *map[string]int64, tableSizes *map[string]int64) error {
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
			rowCount, err := qr.Rows[i][1].ToCastInt64()
			if err != nil {
				return err
			}
			tableSize, err := qr.Rows[i][2].ToCastInt64()
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
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no sources found for workflow %s.%s", state.TargetKeyspace, state.Workflow)
	}
	targetDbName := ""
	for _, tsTarget := range ts.targets {
		targetDbName = tsTarget.GetPrimary().DbName()
		break
	}
	if sourceDbName == "" || targetDbName == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "workflow %s.%s is incorrectly configured", state.TargetKeyspace, state.Workflow)
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

	copyProgress := copyProgress{}
	for table, rowCount := range targetRowCounts {
		copyProgress[table] = &tableCopyProgress{
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
	span.Annotate("state", req.TabletRequest.State)
	span.Annotate("shards", req.TabletRequest.Shards)

	vx := vexec.NewVExec(req.Keyspace, req.TabletRequest.Workflow, s.ts, s.tmc, s.env.Parser())
	vx.SetShardSubset(req.TabletRequest.Shards)
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
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the %s workflow does not exist in the %s keyspace", req.TabletRequest.Workflow, req.Keyspace)
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
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "table(s) not found in source keyspace %s: %s", sourceKeyspace, strings.Join(missingTables, ","))
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
		var err error
		targetPrimary, err := s.ts.GetTablet(ctx, target.PrimaryAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.PrimaryAlias)
		}
		res, err := s.tmc.ReadVReplicationWorkflow(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
			Workflow: mz.ms.Workflow,
		})
		if err != nil {
			return vterrors.Wrapf(err, "failed to read vreplication workflow on %+v", targetPrimary.Tablet)
		}
		for _, stream := range res.Streams {
			mu.Lock()
			shardTablets = append(shardTablets, fmt.Sprintf("%s:%d", target.ShardName(), stream.Id))
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
	if _, err := s.tmc.VDiff(ctx, tablet, &tabletmanagerdatapb.VDiffRequest{
		Keyspace:  tablet.Keyspace,
		Workflow:  workflow,
		Action:    string(vdiff.DeleteAction),
		ActionArg: vdiff.AllActionArg,
	}); err != nil {
		log.Errorf("Error deleting vdiff data for %s.%s workflow: %v", tablet.Keyspace, workflow, err)
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
			if sqlErr, ok := err.(*sqlerror.SQLError); ok && sqlErr.Num == sqlerror.ERNoSuchTable { // the table may not exist
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

	// There is nothing to drop for a LookupVindex workflow.
	if ts.workflowType == binlogdatapb.VReplicationWorkflowType_CreateLookupIndex {
		return nil, nil
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
			if err := sw.dropTargetDeniedTables(ctx); err != nil {
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
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "source keyspaces are mismatched across streams: %v vs %v", ts.sourceKeyspace, bls.Keyspace)
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
					return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "table lists are mismatched across streams: %v vs %v", ts.tables, tables)
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
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "source shard %s/%s currently has no primary tablet",
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
	ts.sourceKSSchema, err = vindexes.BuildKeyspaceSchema(vs, ts.sourceKeyspace, s.env.Parser())
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
			if err := sw.dropTargetDeniedTables(ctx); err != nil {
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
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %v/%v is still serving, cannot delete it, use the even-if-serving flag if needed", keyspace, shard)
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
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "GetTabletsByCell(%v) failed: %v", cell, err)
			}
		case err == nil:
			// We found a ShardReplication object. We
			// trust it to have all tablet records.
			aliases = make([]*topodatapb.TabletAlias, len(sri.Nodes))
			for i, n := range sri.Nodes {
				aliases[i] = n.TabletAlias
			}
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "GetShardReplication(%v, %v, %v) failed: %v", cell, keyspace, shard, err)
		}

		// Get the corresponding Tablet records. Note
		// GetTabletMap ignores ErrNoNode, and it's good for
		// our purpose, it means a tablet was deleted but is
		// still referenced.
		tabletMap, err := s.ts.GetTabletMap(ctx, aliases, nil)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "GetTabletMap() failed: %v", err)
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
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %v/%v still has %v tablets in cell %v; use --recursive or remove them manually", keyspace, shard, len(tabletMap), cell)
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
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't delete tablet %v: %v", tabletAlias, err)
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
func (s *Server) updateShardRecords(ctx context.Context, keyspace string, shards []*topo.ShardInfo, cells []string,
	servedType topodatapb.TabletType, isFrom bool, clearSourceShards bool, logger logutil.Logger,
) (err error) {
	return topotools.UpdateShardRecords(ctx, s.ts, s.tmc, keyspace, shards, cells, servedType, isFrom, clearSourceShards, logger)
}

// refreshPrimaryTablets will just RPC-ping all the primary tablets with RefreshState
func (s *Server) refreshPrimaryTablets(ctx context.Context, shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
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
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid action for Migrate workflow: SwitchTraffic")
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
	reason, err := s.canSwitch(ctx, ts, startState, direction, int64(maxReplicationLagAllowed.Seconds()), req.Shards)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot switch traffic for workflow %s at this time: %s", startState.Workflow, reason)
	}
	hasReplica, hasRdonly, hasPrimary, err = parseTabletTypes(req.TabletTypes)
	if err != nil {
		return nil, err
	}
	if hasReplica || hasRdonly {
		if rdDryRunResults, err = s.switchReads(ctx, req, ts, startState, timeout, false, direction); err != nil {
			return nil, err
		}
		log.Infof("Switch Reads done for workflow %s.%s", req.Keyspace, req.Workflow)
	}
	if rdDryRunResults != nil {
		dryRunResults = append(dryRunResults, *rdDryRunResults...)
	}
	if hasPrimary {
		if _, wrDryRunResults, err = s.switchWrites(ctx, req, ts, timeout, false); err != nil {
			return nil, err
		}
		log.Infof("Switch Writes done for workflow %s.%s", req.Keyspace, req.Workflow)
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
	log.Infof("%s done for workflow %s.%s", cmd, req.Keyspace, req.Workflow)
	resp := &vtctldatapb.WorkflowSwitchTrafficResponse{}
	if req.DryRun {
		resp.Summary = fmt.Sprintf("%s dry run results for workflow %s.%s at %v", cmd, req.Keyspace, req.Workflow, time.Now().UTC().Format(time.RFC822))
		resp.DryRunResults = dryRunResults
	} else {
		log.Infof("SwitchTraffic done for workflow %s.%s", req.Keyspace, req.Workflow)
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
		log.Infof("Before reloading workflow state after switching traffic: %+v\n", resp.StartState)
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
	var roTabletTypes []topodatapb.TabletType
	// When we are switching all traffic we also get the primary tablet type, which we need to
	// filter out for switching reads.
	for _, tabletType := range req.TabletTypes {
		if tabletType != topodatapb.TabletType_PRIMARY {
			roTabletTypes = append(roTabletTypes, tabletType)
		}
	}

	roTypesToSwitchStr := topoproto.MakeStringTypeCSV(roTabletTypes)
	var switchReplica, switchRdonly bool
	for _, roType := range roTabletTypes {
		switch roType {
		case topodatapb.TabletType_REPLICA:
			switchReplica = true
		case topodatapb.TabletType_RDONLY:
			switchRdonly = true
		}
	}

	cellsStr := strings.Join(req.Cells, ",")

	// Consistently handle errors by logging and returning them.
	handleError := func(message string, err error) (*[]string, error) {
		werr := vterrors.Wrapf(err, message)
		ts.Logger().Error(werr)
		return nil, werr
	}

	log.Infof("Switching reads: %s.%s tablet types: %s, cells: %s, workflow state: %s", ts.targetKeyspace, ts.workflow, roTypesToSwitchStr, cellsStr, state.String())
	if !switchReplica && !switchRdonly {
		return handleError("invalid tablet types", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "tablet types must be REPLICA or RDONLY: %s", roTypesToSwitchStr))
	}
	if !ts.isPartialMigration { // shard level traffic switching is all or nothing
		if direction == DirectionBackward && switchReplica && len(state.ReplicaCellsSwitched) == 0 {
			return handleError("invalid request", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requesting reversal of read traffic for REPLICAs but REPLICA reads have not been switched"))
		}
		if direction == DirectionBackward && switchRdonly && len(state.RdonlyCellsSwitched) == 0 {
			return handleError("invalid request", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requesting reversal of SwitchReads for RDONLYs but RDONLY reads have not been switched"))
		}
	}

	// If there are no rdonly tablets in the cells ask to switch rdonly tablets as well so that routing rules
	// are updated for rdonly as well. Otherwise vitess will not know that the workflow has completed and will
	// incorrectly report that not all reads have been switched. User currently is forced to switch non-existent
	// rdonly tablets.
	if switchReplica && !switchRdonly {
		var err error
		rdonlyTabletsExist, err := topotools.DoCellsHaveRdonlyTablets(ctx, s.ts, req.Cells)
		if err != nil {
			return nil, err
		}
		if !rdonlyTabletsExist {
			roTabletTypes = append(roTabletTypes, topodatapb.TabletType_RDONLY)
		}
	}

	// If journals exist notify user and fail.
	journalsExist, _, err := ts.checkJournals(ctx)
	if err != nil {
		return handleError(fmt.Sprintf("failed to read journal in the %s keyspace", ts.SourceKeyspaceName()), err)
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
		return handleError("workflow validation failed", err)
	}

	// For reads, locking the source keyspace is sufficient.
	ctx, unlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "SwitchReads")
	if lockErr != nil {
		return handleError(fmt.Sprintf("failed to lock the %s keyspace", ts.SourceKeyspaceName()), lockErr)
	}
	defer unlock(&err)

	if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		if ts.isPartialMigration {
			ts.Logger().Infof("Partial migration, skipping switchTableReads as traffic is all or nothing per shard and overridden for reads AND writes in the ShardRoutingRule created when switching writes.")
		} else if err := sw.switchTableReads(ctx, req.Cells, roTabletTypes, direction); err != nil {
			return handleError("failed to switch read traffic for the tables", err)
		}
		return sw.logs(), nil
	}
	ts.Logger().Infof("About to switchShardReads: cells: %s, tablet types: %s, direction: %d", cellsStr, roTypesToSwitchStr, direction)
	if err := sw.switchShardReads(ctx, req.Cells, roTabletTypes, direction); err != nil {
		return handleError("failed to switch read traffic for the shards", err)
	}

	ts.Logger().Infof("switchShardReads Completed: cells: %s, tablet types: %s, direction: %d", cellsStr, roTypesToSwitchStr, direction)
	if err := s.ts.ValidateSrvKeyspace(ctx, ts.targetKeyspace, cellsStr); err != nil {
		err2 := vterrors.Wrapf(err, "after switching shard reads, found SrvKeyspace for %s is corrupt in cell %s",
			ts.targetKeyspace, cellsStr)
		return handleError("failed to validate SrvKeyspace record", err2)
	}
	return sw.logs(), nil
}

// switchWrites is a generic way of migrating write traffic for a workflow.
func (s *Server) switchWrites(ctx context.Context, req *vtctldatapb.WorkflowSwitchTrafficRequest, ts *trafficSwitcher, timeout time.Duration,
	cancel bool,
) (journalID int64, dryRunResults *[]string, err error) {
	var sw iswitcher
	if req.DryRun {
		sw = &switcherDryRun{ts: ts, drLog: NewLogRecorder()}
	} else {
		sw = &switcher{ts: ts, s: s}
	}

	// Consistently handle errors by logging and returning them.
	handleError := func(message string, err error) (int64, *[]string, error) {
		werr := vterrors.Wrapf(err, message)
		ts.Logger().Error(werr)
		return 0, nil, werr
	}

	if ts.frozen {
		ts.Logger().Warningf("Writes have already been switched for workflow %s, nothing to do here", ts.WorkflowName())
		return 0, sw.logs(), nil
	}

	if err := ts.validate(ctx); err != nil {
		return handleError("workflow validation failed", err)
	}

	if req.EnableReverseReplication {
		if err := areTabletsAvailableToStreamFrom(ctx, req, ts, ts.TargetKeyspaceName(), ts.TargetShards()); err != nil {
			return handleError(fmt.Sprintf("no tablets were available to stream from in the %s keyspace", ts.SourceKeyspaceName()), err)
		}
	}

	// Need to lock both source and target keyspaces.
	tctx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "SwitchWrites")
	if lockErr != nil {
		return handleError(fmt.Sprintf("failed to lock the %s keyspace", ts.SourceKeyspaceName()), lockErr)
	}
	ctx = tctx
	defer sourceUnlock(&err)
	if ts.TargetKeyspaceName() != ts.SourceKeyspaceName() {
		tctx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "SwitchWrites")
		if lockErr != nil {
			return handleError(fmt.Sprintf("failed to lock the %s keyspace", ts.TargetKeyspaceName()), lockErr)
		}
		ctx = tctx
		defer targetUnlock(&err)
	}

	// Find out if the target is using any sequence tables for auto_increment
	// value generation. If so, then we'll need to ensure that they are
	// initialized properly before allowing new writes on the target.
	sequenceMetadata := make(map[string]*sequenceMetadata)
	// For sharded to sharded migrations the sequence must already be setup.
	// For reshards the sequence usage is not changed.
	if req.InitializeTargetSequences && ts.workflowType == binlogdatapb.VReplicationWorkflowType_MoveTables &&
		ts.SourceKeyspaceSchema() != nil && ts.SourceKeyspaceSchema().Keyspace != nil &&
		!ts.SourceKeyspaceSchema().Keyspace.Sharded {
		sequenceMetadata, err = ts.getTargetSequenceMetadata(ctx)
		if err != nil {
			return handleError(fmt.Sprintf("failed to get the sequence information in the %s keyspace", ts.TargetKeyspaceName()), err)
		}
	}

	// If no journals exist, sourceWorkflows will be initialized by sm.MigrateStreams.
	journalsExist, sourceWorkflows, err := ts.checkJournals(ctx)
	if err != nil {
		return handleError(fmt.Sprintf("failed to read journal in the %s keyspace", ts.SourceKeyspaceName()), err)
	}
	if !journalsExist {
		ts.Logger().Infof("No previous journals were found. Proceeding normally.")
		sm, err := BuildStreamMigrator(ctx, ts, cancel, s.env.Parser())
		if err != nil {
			return handleError("failed to migrate the workflow streams", err)
		}
		if cancel {
			sw.cancelMigration(ctx, sm)
			return 0, sw.logs(), nil
		}

		ts.Logger().Infof("Stopping streams")
		sourceWorkflows, err = sw.stopStreams(ctx, sm)
		if err != nil {
			for key, streams := range sm.Streams() {
				for _, stream := range streams {
					ts.Logger().Errorf("stream in stopStreams: key %s shard %s stream %+v", key, stream.BinlogSource.Shard, stream.BinlogSource)
				}
			}
			sw.cancelMigration(ctx, sm)
			return handleError("failed to stop the workflow streams", err)
		}

		ts.Logger().Infof("Stopping source writes")
		if err := sw.stopSourceWrites(ctx); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError(fmt.Sprintf("failed to stop writes in the %s keyspace", ts.SourceKeyspaceName()), err)
		}

		if ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
			ts.Logger().Infof("Executing LOCK TABLES on source tables %d times", lockTablesCycles)
			// Doing this twice with a pause in-between to catch any writes that may have raced in between
			// the tablet's deny list check and the first mysqld side table lock.
			for cnt := 1; cnt <= lockTablesCycles; cnt++ {
				if err := ts.executeLockTablesOnSource(ctx); err != nil {
					sw.cancelMigration(ctx, sm)
					return handleError(fmt.Sprintf("failed to execute LOCK TABLES (attempt %d of %d) on sources", cnt, lockTablesCycles), err)
				}
				// No need to UNLOCK the tables as the connection was closed once the locks were acquired
				// and thus the locks released.
				time.Sleep(lockTablesCycleDelay)
			}
		}

		ts.Logger().Infof("Waiting for streams to catchup")
		if err := sw.waitForCatchup(ctx, timeout); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to sync up replication between the source and target", err)
		}

		ts.Logger().Infof("Migrating streams")
		if err := sw.migrateStreams(ctx, sm); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to migrate the workflow streams", err)
		}

		ts.Logger().Infof("Resetting sequences")
		if err := sw.resetSequences(ctx); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to reset the sequences", err)
		}

		ts.Logger().Infof("Creating reverse streams")
		if err := sw.createReverseVReplication(ctx); err != nil {
			sw.cancelMigration(ctx, sm)
			return handleError("failed to create the reverse vreplication streams", err)
		}

		// Initialize any target sequences, if there are any, before allowing new writes.
		if req.InitializeTargetSequences && len(sequenceMetadata) > 0 {
			ts.Logger().Infof("Initializing target sequences")
			// Writes are blocked so we can safely initialize the sequence tables but
			// we also want to use a shorter timeout than the parent context.
			// We use at most half of the overall timeout.
			initSeqCtx, cancel := context.WithTimeout(ctx, timeout/2)
			defer cancel()
			if err := sw.initializeTargetSequences(initSeqCtx, sequenceMetadata); err != nil {
				sw.cancelMigration(ctx, sm)
				return handleError(fmt.Sprintf("failed to initialize the sequences used in the %s keyspace", ts.TargetKeyspaceName()), err)
			}
		}
	} else {
		if cancel {
			return handleError("invalid cancel", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "traffic switching has reached the point of no return, cannot cancel"))
		}
		ts.Logger().Infof("Journals were found. Completing the left over steps.")
		// Need to gather positions in case all journals were not created.
		if err := ts.gatherPositions(ctx); err != nil {
			return handleError("failed to gather replication positions", err)
		}
	}

	// This is the point of no return. Once a journal is created,
	// traffic can be redirected to target shards.
	if err := sw.createJournals(ctx, sourceWorkflows); err != nil {
		return handleError("failed to create the journal", err)
	}
	if err := sw.allowTargetWrites(ctx); err != nil {
		return handleError(fmt.Sprintf("failed to allow writes in the %s keyspace", ts.TargetKeyspaceName()), err)
	}
	if err := sw.changeRouting(ctx); err != nil {
		return handleError("failed to update the routing rules", err)
	}
	if err := sw.streamMigraterfinalize(ctx, ts, sourceWorkflows); err != nil {
		return handleError("failed to finalize the traffic switch", err)
	}
	if req.EnableReverseReplication {
		if err := sw.startReverseVReplication(ctx); err != nil {
			return handleError("failed to start the reverse workflow", err)
		}
	}

	if err := sw.freezeTargetVReplication(ctx); err != nil {
		return handleError(fmt.Sprintf("failed to freeze the workflow in the %s keyspace", ts.TargetKeyspaceName()), err)
	}

	return ts.id, sw.logs(), nil
}

func (s *Server) canSwitch(ctx context.Context, ts *trafficSwitcher, state *State, direction TrafficSwitchDirection,
	maxAllowedReplLagSecs int64, shards []string) (reason string, err error) {
	if direction == DirectionForward && state.WritesSwitched ||
		direction == DirectionBackward && !state.WritesSwitched {
		log.Infof("writes already switched no need to check lag")
		return "", nil
	}
	wf, err := s.GetWorkflow(ctx, state.TargetKeyspace, state.Workflow, false, shards)
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

// CopySchemaShard copies the schema from a source tablet to the
// specified shard.  The schema is applied directly on the primary of
// the destination shard, and is propagated to the replicas through
// binlogs.
func (s *Server) CopySchemaShard(ctx context.Context, sourceTabletAlias *topodatapb.TabletAlias, tables, excludeTables []string, includeViews bool, destKeyspace, destShard string, waitReplicasTimeout time.Duration, skipVerify bool) error {
	destShardInfo, err := s.ts.GetShard(ctx, destKeyspace, destShard)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "GetShard(%v, %v) failed: %v", destKeyspace, destShard, err)
	}

	if destShardInfo.PrimaryAlias == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no primary in shard record %v/%v. Consider running 'vtctl InitShardPrimary' in case of a new shard or reparenting the shard to fix the topology data", destKeyspace, destShard)
	}

	diffs, err := schematools.CompareSchemas(ctx, s.ts, s.tmc, sourceTabletAlias, destShardInfo.PrimaryAlias, tables, excludeTables, includeViews)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "CopySchemaShard failed because schemas could not be compared initially: %v", err)
	}
	if diffs == nil {
		// Return early because dest has already the same schema as source.
		return nil
	}

	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}
	sourceSd, err := schematools.GetSchema(ctx, s.ts, s.tmc, sourceTabletAlias, req)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "GetSchema(%v, %v, %v, %v) failed: %v", sourceTabletAlias, tables, excludeTables, includeViews, err)
	}

	createSQLstmts := tmutils.SchemaDefinitionToSQLStrings(sourceSd)

	destTabletInfo, err := s.ts.GetTablet(ctx, destShardInfo.PrimaryAlias)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "GetTablet(%v) failed: %v", destShardInfo.PrimaryAlias, err)
	}
	for _, createSQL := range createSQLstmts {
		err = s.applySQLShard(ctx, destTabletInfo, createSQL)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "creating a table failed."+
				" Most likely some tables already exist on the destination and differ from the source."+
				" Please remove all to be copied tables from the destination manually and run this command again."+
				" Full error: %v", err)
		}
	}

	// Remember the replication position after all the above were applied.
	destPrimaryPos, err := s.tmc.PrimaryPosition(ctx, destTabletInfo.Tablet)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "CopySchemaShard: can't get replication position after schema applied: %v", err)
	}

	// Although the copy was successful, we have to verify it to catch the case
	// where the database already existed on the destination, but with different
	// options e.g. a different character set.
	// In that case, MySQL would have skipped our CREATE DATABASE IF NOT EXISTS
	// statement.
	if !skipVerify {
		diffs, err = schematools.CompareSchemas(ctx, s.ts, s.tmc, sourceTabletAlias, destShardInfo.PrimaryAlias, tables, excludeTables, includeViews)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "CopySchemaShard failed because schemas could not be compared finally: %v", err)
		}
		if diffs != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "CopySchemaShard was not successful because the schemas between the two tablets %v and %v differ: %v", sourceTabletAlias, destShardInfo.PrimaryAlias, diffs)
		}
	}

	// Notify Replicas to reload schema. This is best-effort.
	reloadCtx, cancel := context.WithTimeout(ctx, waitReplicasTimeout)
	defer cancel()
	_, ok := schematools.ReloadShard(reloadCtx, s.ts, s.tmc, logutil.NewMemoryLogger(), destKeyspace, destShard, destPrimaryPos, nil, true)
	if !ok {
		log.Error(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "CopySchemaShard: failed to reload schema on all replicas"))
	}

	return err
}

// applySQLShard applies a given SQL change on a given tablet alias. It allows executing arbitrary
// SQL statements, but doesn't return any results, so it's only useful for SQL statements
// that would be run for their effects (e.g., CREATE).
// It works by applying the SQL statement on the shard's primary tablet with replication turned on.
// Thus it should be used only for changes that can be applied on a live instance without causing issues;
// it shouldn't be used for anything that will require a pivot.
// The SQL statement string is expected to have {{.DatabaseName}} in place of the actual db name.
func (s *Server) applySQLShard(ctx context.Context, tabletInfo *topo.TabletInfo, change string) error {
	filledChange, err := fillStringTemplate(change, map[string]string{"DatabaseName": tabletInfo.DbName()})
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "fillStringTemplate failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Need to make sure that replication is enabled since we're only applying
	// the statement on primaries.
	_, err = s.tmc.ApplySchema(ctx, tabletInfo.Tablet, &tmutils.SchemaChange{
		SQL:              filledChange,
		Force:            false,
		AllowReplication: true,
		SQLMode:          vreplication.SQLMode,
	})
	return err
}

// fillStringTemplate returns the string template filled.
func fillStringTemplate(tmpl string, vars any) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	var data strings.Builder
	if err := myTemplate.Execute(&data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}

// prepareCreateLookup performs the preparatory steps for creating a
// Lookup Vindex.
func (s *Server) prepareCreateLookup(ctx context.Context, workflow, keyspace string, specs *vschemapb.Keyspace, continueAfterCopyWithOwner bool) (ms *vtctldatapb.MaterializeSettings, sourceVSchema, targetVSchema *vschemapb.Keyspace, err error) {
	// Important variables are pulled out here.
	var (
		vindexName        string
		vindex            *vschemapb.Vindex
		targetKeyspace    string
		targetTableName   string
		vindexFromCols    []string
		vindexToCol       string
		vindexIgnoreNulls bool

		sourceTableName string
		// sourceTable is the supplied table info.
		sourceTable *vschemapb.Table
		// sourceVSchemaTable is the table info present in the vschema.
		sourceVSchemaTable *vschemapb.Table
		// sourceVindexColumns are computed from the input sourceTable.
		sourceVindexColumns []string

		// Target table info.
		createDDL        string
		materializeQuery string
	)

	// Validate input vindex.
	if specs == nil {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no vindex provided")
	}
	if len(specs.Vindexes) != 1 {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "only one vindex must be specified")
	}
	vindexName = maps.Keys(specs.Vindexes)[0]
	vindex = maps.Values(specs.Vindexes)[0]
	if !strings.Contains(vindex.Type, "lookup") {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vindex %s is not a lookup type", vindex.Type)
	}
	targetKeyspace, targetTableName, err = s.env.Parser().ParseTable(vindex.Params["table"])
	if err != nil || targetKeyspace == "" {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vindex table name (%s) must be in the form <keyspace>.<table>", vindex.Params["table"])
	}
	vindexFromCols = strings.Split(vindex.Params["from"], ",")
	for i, col := range vindexFromCols {
		vindexFromCols[i] = strings.TrimSpace(col)
	}
	if strings.Contains(vindex.Type, "unique") {
		if len(vindexFromCols) != 1 {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unique vindex 'from' should have only one column")
		}
	} else {
		if len(vindexFromCols) < 2 {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "non-unique vindex 'from' should have more than one column")
		}
	}
	vindexToCol = vindex.Params["to"]
	// Make the vindex write_only. If one exists already in the vschema,
	// it will need to match this vindex exactly, including the write_only setting.
	vindex.Params["write_only"] = "true"
	// See if we can create the vindex without errors.
	if _, err := vindexes.CreateVindex(vindex.Type, vindexName, vindex.Params); err != nil {
		return nil, nil, nil, err
	}
	if ignoreNullsStr, ok := vindex.Params["ignore_nulls"]; ok {
		// This mirrors the behavior of vindexes.boolFromMap().
		switch ignoreNullsStr {
		case "true":
			vindexIgnoreNulls = true
		case "false":
			vindexIgnoreNulls = false
		default:
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ignore_nulls (%s) value must be 'true' or 'false'",
				ignoreNullsStr)
		}
	}

	// Validate input table.
	if len(specs.Tables) < 1 || len(specs.Tables) > 2 {
		return nil, nil, nil, fmt.Errorf("one or two tables must be specified")
	}
	// Loop executes once or twice.
	for tableName, table := range specs.Tables {
		if len(table.ColumnVindexes) != 1 {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "exactly one ColumnVindex must be specified for the %s table", tableName)
		}
		if tableName != targetTableName { // This is the source table.
			sourceTableName = tableName
			sourceTable = table
			continue
		}
		// This is a primary vindex definition for the target table
		// which allows you to override the vindex type used.
		var vindexCols []string
		if len(table.ColumnVindexes[0].Columns) != 0 {
			vindexCols = table.ColumnVindexes[0].Columns
		} else {
			if table.ColumnVindexes[0].Column == "" {
				return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at least one column must be specified in ColumnVindexes for the %s table", tableName)
			}
			vindexCols = []string{table.ColumnVindexes[0].Column}
		}
		if !slices.Equal(vindexCols, vindexFromCols) {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "columns in the lookup table %s primary vindex (%s) don't match the 'from' columns specified (%s)",
				tableName, strings.Join(vindexCols, ","), strings.Join(vindexFromCols, ","))
		}
	}

	// Validate input table and vindex consistency.
	if sourceTable == nil || len(sourceTable.ColumnVindexes) != 1 {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No ColumnVindex found for the owner table in the %s keyspace", keyspace)
	}
	if sourceTable.ColumnVindexes[0].Name != vindexName {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ColumnVindex name (%s) must match vindex name (%s)", sourceTable.ColumnVindexes[0].Name, vindexName)
	}
	if vindex.Owner != "" && vindex.Owner != sourceTableName {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vindex owner (%s) must match table name (%s)", vindex.Owner, sourceTableName)
	}
	if len(sourceTable.ColumnVindexes[0].Columns) != 0 {
		sourceVindexColumns = sourceTable.ColumnVindexes[0].Columns
	} else {
		if sourceTable.ColumnVindexes[0].Column == "" {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at least one column must be specified in ColumnVindexes for the %s table", sourceTableName)
		}
		sourceVindexColumns = []string{sourceTable.ColumnVindexes[0].Column}
	}
	if len(sourceVindexColumns) != len(vindexFromCols) {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "length of table columns (%d) differs from length of vindex columns (%d)", len(sourceVindexColumns), len(vindexFromCols))
	}

	// Validate against source vschema.
	sourceVSchema, err = s.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, nil, nil, err
	}
	if sourceVSchema.Vindexes == nil {
		sourceVSchema.Vindexes = make(map[string]*vschemapb.Vindex)
	}
	// If source and target keyspaces are the same, make vschemas point
	// to the same object.
	if keyspace == targetKeyspace {
		targetVSchema = sourceVSchema
	} else {
		targetVSchema, err = s.ts.GetVSchema(ctx, targetKeyspace)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if targetVSchema.Vindexes == nil {
		targetVSchema.Vindexes = make(map[string]*vschemapb.Vindex)
	}
	if targetVSchema.Tables == nil {
		targetVSchema.Tables = make(map[string]*vschemapb.Table)
	}
	if existing, ok := sourceVSchema.Vindexes[vindexName]; ok {
		if !proto.Equal(existing, vindex) {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "a conflicting vindex named %s already exists in the %s keyspace", vindexName, keyspace)
		}
	}
	sourceVSchemaTable = sourceVSchema.Tables[sourceTableName]
	if sourceVSchemaTable == nil && !schema.IsInternalOperationTableName(sourceTableName) {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %s not found in the %s keyspace", sourceTableName, keyspace)
	}
	for _, colVindex := range sourceVSchemaTable.ColumnVindexes {
		// For a conflict, the vindex name and column should match.
		if colVindex.Name != vindexName {
			continue
		}
		colName := colVindex.Column
		if len(colVindex.Columns) != 0 {
			colName = colVindex.Columns[0]
		}
		if colName == sourceVindexColumns[0] {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "a conflicting ColumnVindex on column %s in table %s already exists in the %s keyspace",
				colName, sourceTableName, keyspace)
		}
	}

	// Validate against source schema.
	sourceShards, err := s.ts.GetServingShards(ctx, keyspace)
	if err != nil {
		return nil, nil, nil, err
	}
	onesource := sourceShards[0]
	if onesource.PrimaryAlias == nil {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "source shard %s has no primary", onesource.ShardName())
	}
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{sourceTableName}}
	tableSchema, err := schematools.GetSchema(ctx, s.ts, s.tmc, onesource.PrimaryAlias, req)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(tableSchema.TableDefinitions) != 1 {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected number of tables (%d) returned from %s schema", len(tableSchema.TableDefinitions), keyspace)
	}

	// Generate "create table" statement.
	lines := strings.Split(tableSchema.TableDefinitions[0].Schema, "\n")
	if len(lines) < 3 {
		// Should never happen.
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "schema looks incorrect: %s, expecting at least four lines", tableSchema.TableDefinitions[0].Schema)
	}
	var modified []string
	modified = append(modified, strings.Replace(lines[0], sourceTableName, targetTableName, 1))
	for i := range sourceVindexColumns {
		line, err := generateColDef(lines, sourceVindexColumns[i], vindexFromCols[i])
		if err != nil {
			return nil, nil, nil, err
		}
		modified = append(modified, line)
	}

	if vindex.Params["data_type"] == "" || strings.EqualFold(vindex.Type, "consistent_lookup_unique") || strings.EqualFold(vindex.Type, "consistent_lookup") {
		modified = append(modified, fmt.Sprintf("  %s varbinary(128),", sqlescape.EscapeID(vindexToCol)))
	} else {
		modified = append(modified, fmt.Sprintf("  %s %s,", sqlescape.EscapeID(vindexToCol), sqlescape.EscapeID(vindex.Params["data_type"])))
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "  PRIMARY KEY (")
	prefix := ""
	for _, col := range vindexFromCols {
		fmt.Fprintf(buf, "%s%s", prefix, sqlescape.EscapeID(col))
		prefix = ", "
	}
	fmt.Fprintf(buf, ")")
	modified = append(modified, buf.String())
	modified = append(modified, ")")
	createDDL = strings.Join(modified, "\n")

	// Generate vreplication query.
	buf = sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select ")
	for i := range vindexFromCols {
		buf.Myprintf("%s as %s, ", sqlparser.String(sqlparser.NewIdentifierCI(sourceVindexColumns[i])), sqlparser.String(sqlparser.NewIdentifierCI(vindexFromCols[i])))
	}
	if strings.EqualFold(vindexToCol, "keyspace_id") || strings.EqualFold(vindex.Type, "consistent_lookup_unique") || strings.EqualFold(vindex.Type, "consistent_lookup") {
		buf.Myprintf("keyspace_id() as %s ", sqlparser.String(sqlparser.NewIdentifierCI(vindexToCol)))
	} else {
		buf.Myprintf("%s as %s ", sqlparser.String(sqlparser.NewIdentifierCI(vindexToCol)), sqlparser.String(sqlparser.NewIdentifierCI(vindexToCol)))
	}
	buf.Myprintf("from %s", sqlparser.String(sqlparser.NewIdentifierCS(sourceTableName)))
	if vindexIgnoreNulls {
		buf.Myprintf(" where ")
		lastValIdx := len(vindexFromCols) - 1
		for i := range vindexFromCols {
			buf.Myprintf("%s is not null", sqlparser.String(sqlparser.NewIdentifierCI(vindexFromCols[i])))
			if i != lastValIdx {
				buf.Myprintf(" and ")
			}
		}
	}
	if vindex.Owner != "" {
		// Only backfill.
		buf.Myprintf(" group by ")
		for i := range vindexFromCols {
			buf.Myprintf("%s, ", sqlparser.String(sqlparser.NewIdentifierCI(vindexFromCols[i])))
		}
		buf.Myprintf("%s", sqlparser.String(sqlparser.NewIdentifierCI(vindexToCol)))
	}
	materializeQuery = buf.String()

	// Update targetVSchema.
	targetTable := specs.Tables[targetTableName]
	if targetVSchema.Sharded {
		// Choose a primary vindex type for the lookup table based on the source
		// definition if one was not explicitly specified.
		var targetVindexType string
		var targetVindex *vschemapb.Vindex
		for _, field := range tableSchema.TableDefinitions[0].Fields {
			if sourceVindexColumns[0] == field.Name {
				if targetTable != nil && len(targetTable.ColumnVindexes) > 0 {
					targetVindexType = targetTable.ColumnVindexes[0].Name
				}
				if targetVindexType == "" {
					targetVindexType, err = vindexes.ChooseVindexForType(field.Type)
					if err != nil {
						return nil, nil, nil, err
					}
				}
				targetVindex = &vschemapb.Vindex{
					Type: targetVindexType,
				}
				break
			}
		}
		if targetVindex == nil {
			// Unreachable. We validated column names when generating the DDL.
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %s not found in target schema %s", sourceVindexColumns[0], tableSchema.TableDefinitions[0].Schema)
		}
		if existing, ok := targetVSchema.Vindexes[targetVindexType]; ok {
			if !proto.Equal(existing, targetVindex) {
				return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "a conflicting vindex named %v already exists in the %s keyspace", targetVindexType, targetKeyspace)
			}
		} else {
			targetVSchema.Vindexes[targetVindexType] = targetVindex
		}

		targetTable = &vschemapb.Table{
			ColumnVindexes: []*vschemapb.ColumnVindex{{
				Column: vindexFromCols[0],
				Name:   targetVindexType,
			}},
		}
	} else {
		targetTable = &vschemapb.Table{}
	}
	if existing, ok := targetVSchema.Tables[targetTableName]; ok {
		if !proto.Equal(existing, targetTable) {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "a conflicting table named %s already exists in the %s vschema", targetTableName, targetKeyspace)
		}
	} else {
		targetVSchema.Tables[targetTableName] = targetTable
	}

	ms = &vtctldatapb.MaterializeSettings{
		Workflow:              workflow,
		MaterializationIntent: vtctldatapb.MaterializationIntent_CREATELOOKUPINDEX,
		SourceKeyspace:        keyspace,
		TargetKeyspace:        targetKeyspace,
		StopAfterCopy:         vindex.Owner != "" && !continueAfterCopyWithOwner,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      targetTableName,
			SourceExpression: materializeQuery,
			CreateDdl:        createDDL,
		}},
	}

	// Update sourceVSchema
	sourceVSchema.Vindexes[vindexName] = vindex
	sourceVSchemaTable.ColumnVindexes = append(sourceVSchemaTable.ColumnVindexes, sourceTable.ColumnVindexes[0])

	return ms, sourceVSchema, targetVSchema, nil
}

func generateColDef(lines []string, sourceVindexCol, vindexFromCol string) (string, error) {
	source := sqlescape.EscapeID(sourceVindexCol)
	target := sqlescape.EscapeID(vindexFromCol)

	for _, line := range lines[1:] {
		if strings.Contains(line, source) {
			line = strings.Replace(line, source, target, 1)
			line = strings.Replace(line, " AUTO_INCREMENT", "", 1)
			line = strings.Replace(line, " DEFAULT NULL", "", 1)
			return line, nil
		}
	}
	return "", fmt.Errorf("column %s not found in schema %v", sourceVindexCol, lines)
}

func (s *Server) MigrateCreate(ctx context.Context, req *vtctldatapb.MigrateCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	moveTablesCreateRequest := &vtctldatapb.MoveTablesCreateRequest{
		Workflow:                  req.Workflow,
		SourceKeyspace:            req.SourceKeyspace,
		TargetKeyspace:            req.TargetKeyspace,
		ExternalClusterName:       req.MountName,
		Cells:                     req.Cells,
		TabletTypes:               req.TabletTypes,
		TabletSelectionPreference: req.TabletSelectionPreference,
		AllTables:                 req.AllTables,
		IncludeTables:             req.IncludeTables,
		ExcludeTables:             req.ExcludeTables,
		SourceTimeZone:            req.SourceTimeZone,
		OnDdl:                     req.OnDdl,
		StopAfterCopy:             req.StopAfterCopy,
		DeferSecondaryKeys:        req.DeferSecondaryKeys,
		DropForeignKeys:           req.DropForeignKeys,
		AutoStart:                 req.AutoStart,
		NoRoutingRules:            req.NoRoutingRules,
	}
	return s.moveTablesCreate(ctx, moveTablesCreateRequest, binlogdatapb.VReplicationWorkflowType_Migrate)
}
