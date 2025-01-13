/*
Copyright 2024 The Vitess Authors.

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

/*
This file provides functions for fetching and retrieving information about VReplication workflows

At the moment it is used by the `GetWorkflows` function in `server.go and includes functionality to
get the following:
- Fetch workflows by shard
- Fetch copy states by shard stream
- Build workflows with metadata
- Fetch stream logs
*/

package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/workflow/common"
	"vitess.io/vitess/go/vt/vtctl/workflow/vexec"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

// workflowFetcher is responsible for fetching and retrieving information
// about VReplication workflows.
type workflowFetcher struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient

	logger logutil.Logger
	parser *sqlparser.Parser
}

type workflowMetadata struct {
	sourceKeyspace                string
	sourceShards                  sets.Set[string]
	targetKeyspace                string
	targetShards                  sets.Set[string]
	maxVReplicationLag            float64
	maxVReplicationTransactionLag float64
}

var vrepLogQuery = strings.TrimSpace(`
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

func (wf *workflowFetcher) fetchWorkflowsByShard(
	ctx context.Context,
	req *vtctldatapb.GetWorkflowsRequest,
) (map[*topo.TabletInfo]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
	readReq := &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{}
	if req.Workflow != "" {
		readReq.IncludeWorkflows = []string{req.Workflow}
	}
	if req.ActiveOnly {
		readReq.ExcludeStates = []binlogdatapb.VReplicationWorkflowState{binlogdatapb.VReplicationWorkflowState_Stopped}
	}

	m := sync.Mutex{}

	shards, err := common.GetShards(ctx, wf.ts, req.Keyspace, req.Shards)
	if err != nil {
		return nil, err
	}

	results := make(map[*topo.TabletInfo]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, len(shards))

	err = wf.forAllShards(ctx, req.Keyspace, shards, func(ctx context.Context, si *topo.ShardInfo) error {
		primary, err := wf.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return err
		}
		if primary == nil {
			return fmt.Errorf("%w %s/%s: tablet %v not found", vexec.ErrNoShardPrimary, req.Keyspace, si.ShardName(), topoproto.TabletAliasString(si.PrimaryAlias))
		}
		// Clone the request so that we can set the correct DB name for tablet.
		req := readReq.CloneVT()
		wres, err := wf.tmc.ReadVReplicationWorkflows(ctx, primary.Tablet, req)
		if err != nil {
			return err
		}
		m.Lock()
		defer m.Unlock()
		results[primary] = wres
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (wf *workflowFetcher) fetchCopyStatesByShardStream(
	ctx context.Context,
	workflowsByShard map[*topo.TabletInfo]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse,
) (map[string][]*vtctldatapb.Workflow_Stream_CopyState, error) {
	m := sync.Mutex{}

	copyStatesByShardStreamId := make(map[string][]*vtctldatapb.Workflow_Stream_CopyState, len(workflowsByShard))

	fetchCopyStates := func(ctx context.Context, tablet *topo.TabletInfo, streamIds []int32) error {
		span, ctx := trace.NewSpan(ctx, "workflowFetcher.workflow.fetchCopyStates")
		defer span.Finish()

		span.Annotate("shard", tablet.Shard)
		span.Annotate("tablet_alias", tablet.AliasString())

		copyStates, err := wf.getWorkflowCopyStates(ctx, tablet, streamIds)
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
	for tablet, result := range workflowsByShard {
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

	return copyStatesByShardStreamId, nil
}

func (wf *workflowFetcher) getWorkflowCopyStates(ctx context.Context, tablet *topo.TabletInfo, streamIds []int32) ([]*vtctldatapb.Workflow_Stream_CopyState, error) {
	span, ctx := trace.NewSpan(ctx, "workflowFetcher.workflow.getWorkflowCopyStates")
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
	qr, err := wf.tmc.VReplicationExec(ctx, tablet.Tablet, query)
	if err != nil {
		return nil, err
	}

	result := sqltypes.Proto3ToResult(qr)
	if result == nil {
		return nil, nil
	}

	copyStates := make([]*vtctldatapb.Workflow_Stream_CopyState, len(result.Rows))
	for i, row := range result.Named().Rows {
		streamId, err := row["vrepl_id"].ToInt64()
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to cast vrepl_id to int64: %v", err)
		}
		// These string fields are technically varbinary, but this is close enough.
		copyStates[i] = &vtctldatapb.Workflow_Stream_CopyState{
			StreamId: streamId,
			Table:    row["table_name"].ToString(),
			LastPk:   row["lastpk"].ToString(),
		}
	}

	return copyStates, nil
}

func (wf *workflowFetcher) buildWorkflows(
	ctx context.Context,
	results map[*topo.TabletInfo]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse,
	copyStatesByShardStreamId map[string][]*vtctldatapb.Workflow_Stream_CopyState,
	req *vtctldatapb.GetWorkflowsRequest,
) ([]*vtctldatapb.Workflow, error) {
	workflowsMap := make(map[string]*vtctldatapb.Workflow, len(results))
	workflowMetadataMap := make(map[string]*workflowMetadata, len(results))

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
				workflowMetadataMap[workflowName] = &workflowMetadata{
					sourceShards: sets.New[string](),
					targetShards: sets.New[string](),
				}
			}

			metadata := workflowMetadataMap[workflowName]
			err := wf.scanWorkflow(ctx, workflow, wfres, tablet, metadata, copyStatesByShardStreamId, req.Keyspace)
			if err != nil {
				return nil, err
			}
		}
	}

	for name, workflow := range workflowsMap {
		meta := workflowMetadataMap[name]
		updateWorkflowWithMetadata(workflow, meta)

		// Sort shard streams by stream_id ASC, to support an optimization
		// in fetchStreamLogs below.
		for _, shardStreams := range workflow.ShardStreams {
			sort.Slice(shardStreams.Streams, func(i, j int) bool {
				return shardStreams.Streams[i].Id < shardStreams.Streams[j].Id
			})
		}
	}

	if req.IncludeLogs {
		var fetchLogsWG sync.WaitGroup

		for _, workflow := range workflowsMap {
			// Fetch logs for all streams associated with this workflow in the background.
			fetchLogsWG.Add(1)
			go func(ctx context.Context, workflow *vtctldatapb.Workflow) {
				defer fetchLogsWG.Done()
				wf.fetchStreamLogs(ctx, req.Keyspace, workflow)
			}(ctx, workflow)
		}

		// Wait for all the log fetchers to finish.
		fetchLogsWG.Wait()
	}

	return maps.Values(workflowsMap), nil
}

func (wf *workflowFetcher) scanWorkflow(
	ctx context.Context,
	workflow *vtctldatapb.Workflow,
	res *tabletmanagerdatapb.ReadVReplicationWorkflowResponse,
	tablet *topo.TabletInfo,
	meta *workflowMetadata,
	copyStatesByShardStreamId map[string][]*vtctldatapb.Workflow_Stream_CopyState,
	keyspace string,
) error {
	shardStreamKey := fmt.Sprintf("%s/%s", tablet.Shard, tablet.AliasString())
	shardStream, ok := workflow.ShardStreams[shardStreamKey]
	if !ok {
		ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer cancel()

		si, err := wf.ts.GetShard(ctx, keyspace, tablet.Shard)
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
		options := res.Options
		if options != "" {
			if err := json.Unmarshal([]byte(options), &workflow.Options); err != nil {
				return err
			}
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
		if copyStates, ok := copyStatesByShardStreamId[shardStreamId]; ok {
			stream.CopyStates = copyStates
		}

		if rstream.TimeUpdated == nil {
			rstream.TimeUpdated = &vttimepb.Time{}
		}

		stream.State = getStreamState(stream, rstream)

		shardStream.Streams = append(shardStream.Streams, stream)

		meta.sourceShards.Insert(stream.BinlogSource.Shard)
		meta.targetShards.Insert(tablet.Shard)

		if meta.sourceKeyspace != "" && meta.sourceKeyspace != stream.BinlogSource.Keyspace {
			return vterrors.Wrapf(ErrMultipleSourceKeyspaces, "workflow = %v, ks1 = %v, ks2 = %v", workflow.Name, meta.sourceKeyspace, stream.BinlogSource.Keyspace)
		}

		meta.sourceKeyspace = stream.BinlogSource.Keyspace

		if meta.targetKeyspace != "" && meta.targetKeyspace != tablet.Keyspace {
			return vterrors.Wrapf(ErrMultipleTargetKeyspaces, "workflow = %v, ks1 = %v, ks2 = %v", workflow.Name, meta.targetKeyspace, tablet.Keyspace)
		}

		meta.targetKeyspace = tablet.Keyspace

		if stream.TimeUpdated == nil {
			stream.TimeUpdated = &vttimepb.Time{}
		}
		timeUpdated := time.Unix(stream.TimeUpdated.Seconds, 0)
		vreplicationLag := time.Since(timeUpdated)

		// MaxVReplicationLag represents the time since we last processed any event
		// in the workflow.
		if vreplicationLag.Seconds() > meta.maxVReplicationLag {
			meta.maxVReplicationLag = vreplicationLag.Seconds()
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
		if rstream.TransactionTimestamp == nil {
			rstream.TransactionTimestamp = &vttimepb.Time{}
		}
		lastTransactionTime := rstream.TransactionTimestamp.Seconds
		if rstream.TimeHeartbeat == nil {
			rstream.TimeHeartbeat = &vttimepb.Time{}
		}
		lastHeartbeatTime := rstream.TimeHeartbeat.Seconds
		if stream.State == binlogdatapb.VReplicationWorkflowState_Copying.String() {
			meta.maxVReplicationTransactionLag = math.MaxInt64
		} else {
			if lastTransactionTime == 0 /* no new events after copy */ ||
				lastHeartbeatTime > lastTransactionTime /* no recent transactions, so all caught up */ {

				lastTransactionTime = lastHeartbeatTime
			}
			now := time.Now().Unix() /* seconds since epoch */
			transactionReplicationLag := float64(now - lastTransactionTime)
			if transactionReplicationLag > meta.maxVReplicationTransactionLag {
				meta.maxVReplicationTransactionLag = transactionReplicationLag
			}
		}
	}

	return nil
}

func updateWorkflowWithMetadata(workflow *vtctldatapb.Workflow, meta *workflowMetadata) {
	workflow.Source = &vtctldatapb.Workflow_ReplicationLocation{
		Keyspace: meta.sourceKeyspace,
		Shards:   sets.List(meta.sourceShards),
	}

	workflow.Target = &vtctldatapb.Workflow_ReplicationLocation{
		Keyspace: meta.targetKeyspace,
		Shards:   sets.List(meta.targetShards),
	}

	workflow.MaxVReplicationLag = int64(meta.maxVReplicationLag)
	workflow.MaxVReplicationTransactionLag = int64(meta.maxVReplicationTransactionLag)
}

func (wf *workflowFetcher) fetchStreamLogs(ctx context.Context, keyspace string, workflow *vtctldatapb.Workflow) {
	span, ctx := trace.NewSpan(ctx, "workflowFetcher.workflow.fetchStreamLogs")
	defer span.Finish()

	span.Annotate("keyspace", keyspace)
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

	vx := vexec.NewVExec(keyspace, workflow.Name, wf.ts, wf.tmc, wf.parser)
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

		for _, row := range qr.Named().Rows {
			id, err := row["id"].ToCastInt64()
			if err != nil {
				markErrors(err)
				continue
			}

			streamID, err := row["vrepl_id"].ToCastInt64()
			if err != nil {
				markErrors(err)
				continue
			}

			typ := row["type"].ToString()
			state := row["state"].ToString()
			message := row["message"].ToString()

			createdAt, err := time.Parse("2006-01-02 15:04:05", row["created_at"].ToString())
			if err != nil {
				markErrors(err)
				continue
			}

			updatedAt, err := time.Parse("2006-01-02 15:04:05", row["updated_at"].ToString())
			if err != nil {
				markErrors(err)
				continue
			}

			count, err := row["count"].ToCastInt64()
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

			// Earlier, in buildWorkflows, we sorted each ShardStreams
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
					wf.logger.Warningf("Found stream log for nonexistent stream: %+v", streamLog)
					// This can happen on manual/failed workflow cleanup so move to the next log.
					break
				}

				// stream.Id == streamLog.StreamId
				stream.Logs = append(stream.Logs, streamLog)
				break
			}
		}
	}
}

func (wf *workflowFetcher) forAllShards(
	ctx context.Context,
	keyspace string,
	shards []string,
	f func(ctx context.Context, shard *topo.ShardInfo) error,
) error {
	eg, egCtx := errgroup.WithContext(ctx)
	for _, shard := range shards {
		eg.Go(func() error {
			si, err := wf.ts.GetShard(ctx, keyspace, shard)
			if err != nil {
				return err
			}
			if si.PrimaryAlias == nil {
				return fmt.Errorf("%w %s/%s", vexec.ErrNoShardPrimary, keyspace, shard)
			}

			if err := f(egCtx, si); err != nil {
				return err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func getStreamState(stream *vtctldatapb.Workflow_Stream, rstream *tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream) string {
	switch {
	case strings.Contains(strings.ToLower(stream.Message), "error"):
		return binlogdatapb.VReplicationWorkflowState_Error.String()
	case stream.State == binlogdatapb.VReplicationWorkflowState_Running.String() && len(stream.CopyStates) > 0:
		return binlogdatapb.VReplicationWorkflowState_Copying.String()
	case stream.State == binlogdatapb.VReplicationWorkflowState_Running.String() && int64(time.Now().Second())-rstream.TimeUpdated.Seconds > 10:
		return binlogdatapb.VReplicationWorkflowState_Lagging.String()
	}
	return rstream.State.String()
}
