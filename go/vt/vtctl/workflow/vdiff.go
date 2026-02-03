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

package workflow

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

// TableSummary aggregates the current state of the table diff from all shards.
type TableSummary struct {
	TableName       string
	State           vdiff.VDiffState
	RowsCompared    int64
	MatchingRows    int64
	MismatchedRows  int64
	ExtraRowsSource int64
	ExtraRowsTarget int64
	LastUpdated     string `json:"LastUpdated,omitempty"`
}

// Summary aggregates the current state of the vdiff from all shards.
type Summary struct {
	Workflow, Keyspace string
	State              vdiff.VDiffState
	UUID               string
	RowsCompared       int64
	HasMismatch        bool
	Shards             string
	StartedAt          string                  `json:"StartedAt,omitempty"`
	CompletedAt        string                  `json:"CompletedAt,omitempty"`
	TableSummaryMap    map[string]TableSummary `json:"TableSummary,omitempty"`
	// This is keyed by table name and then by shard name.
	Reports map[string]map[string]vdiff.DiffReport `json:"Reports,omitempty"`
	// This is keyed by shard name.
	Errors   map[string]string     `json:"Errors,omitempty"`
	Progress *vdiff.ProgressReport `json:"Progress,omitempty"`
}

// SortedTableSummaries returns the table summaries sorted alphabetically by table name.
// This is used by text templates to display tables in a consistent order.
func (s *Summary) SortedTableSummaries() []TableSummary {
	names := make([]string, 0, len(s.TableSummaryMap))
	for name := range s.TableSummaryMap {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]TableSummary, 0, len(names))
	for _, name := range names {
		result = append(result, s.TableSummaryMap[name])
	}
	return result
}

// BuildSummary generates a summary from a vdiff show response.
func BuildSummary(keyspace, workflow, uuid string, resp *vtctldatapb.VDiffShowResponse, verbose bool) (*Summary, error) {
	summary := &Summary{
		Workflow:     workflow,
		Keyspace:     keyspace,
		UUID:         uuid,
		State:        vdiff.UnknownState,
		RowsCompared: 0,
		StartedAt:    "",
		CompletedAt:  "",
		HasMismatch:  false,
		Shards:       "",
		Reports:      make(map[string]map[string]vdiff.DiffReport),
		Errors:       make(map[string]string),
		Progress:     nil,
	}

	var tableSummaryMap map[string]TableSummary
	var reports map[string]map[string]vdiff.DiffReport
	// Keep a tally of the states across all tables in all shards.
	tableStateCounts := map[vdiff.VDiffState]int{
		vdiff.UnknownState:   0,
		vdiff.PendingState:   0,
		vdiff.StartedState:   0,
		vdiff.StoppedState:   0,
		vdiff.ErrorState:     0,
		vdiff.CompletedState: 0,
	}
	// Keep a tally of the summary states across all shards.
	shardStateCounts := map[vdiff.VDiffState]int{
		vdiff.UnknownState:   0,
		vdiff.PendingState:   0,
		vdiff.StartedState:   0,
		vdiff.StoppedState:   0,
		vdiff.ErrorState:     0,
		vdiff.CompletedState: 0,
	}
	// Keep a tally of the approximate total rows to process as we'll use this for our progress
	// report.
	totalRowsToCompare := int64(0)
	var shards []string
	for shard, resp := range resp.TabletResponses {
		first := true
		if resp != nil && resp.Output != nil {
			shards = append(shards, shard)
			qr := sqltypes.Proto3ToResult(resp.Output)
			if tableSummaryMap == nil {
				tableSummaryMap = make(map[string]TableSummary, 0)
				reports = make(map[string]map[string]vdiff.DiffReport, 0)
			}
			for _, row := range qr.Named().Rows {
				// Update the global VDiff summary based on the per shard level summary.
				// Since these values will be the same for all subsequent rows we only use
				// the first row.
				if first {
					first = false
					// Our timestamps are strings in `2022-06-26 20:43:25` format so we sort
					// them lexicographically.
					// We should use the earliest started_at across all shards.
					if sa := row.AsString("started_at", ""); summary.StartedAt == "" || sa < summary.StartedAt {
						summary.StartedAt = sa
					}
					// And we should use the latest completed_at across all shards.
					if ca := row.AsString("completed_at", ""); summary.CompletedAt == "" || ca > summary.CompletedAt {
						summary.CompletedAt = ca
					}
					// If we had an error on the shard, then let's add that to the summary.
					if le := row.AsString("last_error", ""); le != "" {
						summary.Errors[shard] = le
					}
					// Keep track of how many shards are marked as a specific state. We check
					// this combined with the shard.table states to determine the VDiff summary
					// state.
					shardStateCounts[vdiff.VDiffState(strings.ToLower(row.AsString("vdiff_state", "")))]++
				}

				// Global VDiff summary updates that take into account the per table details
				// per shard.
				{
					summary.RowsCompared += row.AsInt64("rows_compared", 0)
					totalRowsToCompare += row.AsInt64("table_rows", 0)

					// If we had a mismatch on any table on any shard then the global VDiff
					// summary does too.
					if mm, _ := row.ToBool("has_mismatch"); mm {
						summary.HasMismatch = true
					}
				}

				// Table summary information that must be accounted for across all shards.
				{
					table := row.AsString("table_name", "")
					if table == "" { // This occurs when the table diff has not started on 1 or more shards
						continue
					}
					// Create the global VDiff table summary object if it doesn't exist.
					if _, ok := tableSummaryMap[table]; !ok {
						tableSummaryMap[table] = TableSummary{
							TableName: table,
							State:     vdiff.UnknownState,
						}
					}
					ts := tableSummaryMap[table]
					// This is the shard level VDiff table state.
					sts := vdiff.VDiffState(strings.ToLower(row.AsString("table_state", "")))
					tableStateCounts[sts]++

					// The error state must be sticky, and we should not override any other
					// known state with completed.
					switch sts {
					case vdiff.CompletedState:
						if ts.State == vdiff.UnknownState {
							ts.State = sts
						}
					case vdiff.ErrorState:
						ts.State = sts
					default:
						if ts.State != vdiff.ErrorState {
							ts.State = sts
						}
					}

					diffReport := row.AsString("report", "")
					dr := vdiff.DiffReport{}
					if diffReport != "" {
						err := json.Unmarshal([]byte(diffReport), &dr)
						if err != nil {
							return nil, err
						}
						ts.RowsCompared += dr.ProcessedRows
						ts.MismatchedRows += dr.MismatchedRows
						ts.MatchingRows += dr.MatchingRows
						ts.ExtraRowsTarget += dr.ExtraRowsTarget
						ts.ExtraRowsSource += dr.ExtraRowsSource
					}
					if _, ok := reports[table]; !ok {
						reports[table] = make(map[string]vdiff.DiffReport)
					}

					reports[table][shard] = dr
					tableSummaryMap[table] = ts
				}
			}
		}
	}

	// The global VDiff summary should progress from pending->started->completed with
	// stopped for any shard and error for any table being sticky for the global summary.
	// We should only consider the VDiff to be complete if it's completed for every table
	// on every shard.
	if shardStateCounts[vdiff.StoppedState] > 0 {
		summary.State = vdiff.StoppedState
	} else if shardStateCounts[vdiff.ErrorState] > 0 || tableStateCounts[vdiff.ErrorState] > 0 {
		summary.State = vdiff.ErrorState
	} else if tableStateCounts[vdiff.StartedState] > 0 {
		summary.State = vdiff.StartedState
	} else if tableStateCounts[vdiff.PendingState] > 0 {
		summary.State = vdiff.PendingState
	} else if tableStateCounts[vdiff.CompletedState] == (len(tableSummaryMap) * len(shards)) {
		// When doing shard consolidations/merges, we cannot rely solely on the
		// vdiff_table state as there are N sources that we process rows from sequentially
		// with each one writing to the shared _vt.vdiff_table record for the target shard.
		// So we only mark the vdiff for the shard as completed when we've finished
		// processing rows from all of the sources -- which is recorded by marking the
		// vdiff done for the shard by setting _vt.vdiff.state = completed.
		if shardStateCounts[vdiff.CompletedState] == len(shards) {
			summary.State = vdiff.CompletedState
		} else {
			summary.State = vdiff.StartedState
		}
	} else {
		summary.State = vdiff.UnknownState
	}

	// If the vdiff has been started then we can calculate the progress.
	if summary.State == vdiff.StartedState {
		summary.Progress = BuildProgressReport(summary.RowsCompared, totalRowsToCompare, summary.StartedAt)
	}

	sort.Strings(shards) // Sort for predictable output
	summary.Shards = strings.Join(shards, ",")
	summary.TableSummaryMap = tableSummaryMap
	summary.Reports = reports
	if !summary.HasMismatch && !verbose {
		summary.Reports = nil
		summary.TableSummaryMap = nil
	}
	// If we haven't completed the global VDiff then be sure to reflect that with no
	// CompletedAt value.
	if summary.State != vdiff.CompletedState {
		summary.CompletedAt = ""
	}
	return summary, nil
}

func BuildProgressReport(rowsCompared int64, rowsToCompare int64, startedAt string) *vdiff.ProgressReport {
	report := &vdiff.ProgressReport{}
	if rowsCompared >= 1 {
		// Round to 2 decimal points.
		report.Percentage = math.Round(math.Min((float64(rowsCompared)/float64(rowsToCompare))*100, 100.00)*100) / 100
	}
	if math.IsNaN(report.Percentage) {
		report.Percentage = 0
	}
	pctToGo := math.Abs(report.Percentage - 100.00)
	startTime, _ := time.Parse(vdiff.TimestampFormat, startedAt)
	curTime := time.Now().UTC()
	runTime := curTime.Unix() - startTime.Unix()
	if report.Percentage >= 1 {
		// Calculate how long 1% took, on avg, and multiply that by the % left.
		eta := time.Unix(((int64(runTime)/int64(report.Percentage))*int64(pctToGo))+curTime.Unix(), 1).UTC()
		// Cap the ETA at 1 year out to prevent providing nonsensical ETAs.
		if eta.Before(time.Now().UTC().AddDate(1, 0, 0)) {
			report.ETA = eta.Format(vdiff.TimestampFormat)
		}
	}
	return report
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
	if req.AutoStart != nil {
		span.Annotate("auto_start", req.GetAutoStart())
	}

	var err error
	req.Uuid = strings.TrimSpace(req.Uuid)
	if req.Uuid == "" { // Generate a UUID
		req.Uuid = uuid.New().String()
	} else { // Validate UUID if provided
		if err = uuid.Validate(req.Uuid); err != nil {
			return nil, vterrors.Wrapf(err, "invalid UUID provided: %s", req.Uuid)
		}
	}

	tabletTypesStr := discovery.BuildTabletTypesString(req.TabletTypes, req.TabletSelectionPreference)

	if req.Limit == 0 { // This would produce no useful results
		req.Limit = math.MaxInt64
	}
	// This is a pointer so there's no ZeroValue in the message
	// and an older v18 client will not provide it.
	if req.MaxDiffDuration == nil {
		req.MaxDiffDuration = &vttimepb.Duration{}
	}
	// The other vttime.Duration vars should not be nil as the
	// client should always provide them, but we check anyway to
	// be safe.
	if req.FilteredReplicationWaitTime == nil {
		// A value of 0 is not valid as the vdiff will never succeed.
		req.FilteredReplicationWaitTime = &vttimepb.Duration{
			Seconds: int64(DefaultTimeout.Seconds()),
		}
	}
	if req.WaitUpdateInterval == nil {
		req.WaitUpdateInterval = &vttimepb.Duration{}
	}

	autoStart := true
	if req.AutoStart != nil {
		autoStart = req.GetAutoStart()
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
			AutoStart:             &autoStart,
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			OnlyPks:                 req.OnlyPKs,
			DebugQuery:              req.DebugQuery,
			MaxSampleRows:           req.MaxReportSampleRows,
			RowDiffColumnTruncateAt: req.RowDiffColumnTruncateAt,
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

	workflowStatus, err := s.getWorkflowStatus(ctx, req.TargetKeyspace, req.Workflow)
	if err != nil {
		return nil, err
	}
	if workflowStatus != binlogdatapb.VReplicationWorkflowState_Running {
		s.Logger().Infof("Workflow %s.%s is not running, cannot start VDiff in state %s", req.TargetKeyspace, req.Workflow, workflowStatus)
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"not all streams are running in workflow %s.%s", req.TargetKeyspace, req.Workflow)
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		s.Logger().Errorf("Error executing vdiff create action: %v", err)
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
		s.Logger().Errorf("Error executing vdiff delete action: %v", err)
		return nil, err
	}

	return &vtctldatapb.VDiffDeleteResponse{}, nil
}

// VDiffResume is part of the vtctlservicepb.VtctldServer interface.
func (s *Server) VDiffResume(ctx context.Context, req *vtctldatapb.VDiffResumeRequest) (*vtctldatapb.VDiffResumeResponse, error) {
	span, ctx := trace.NewSpan(ctx, "workflow.Server.VDiffResume")
	defer span.Finish()

	targetShards := req.GetTargetShards()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("uuid", req.Uuid)
	span.Annotate("target_shards", targetShards)

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

	if len(targetShards) > 0 {
		if err := applyTargetShards(ts, targetShards); err != nil {
			return nil, err
		}
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		s.Logger().Errorf("Error executing vdiff resume action: %v", err)
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
		s.Logger().Errorf("Error executing vdiff show action: %v", output.err)
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

	targetShards := req.GetTargetShards()

	span.Annotate("keyspace", req.TargetKeyspace)
	span.Annotate("workflow", req.Workflow)
	span.Annotate("uuid", req.Uuid)
	span.Annotate("target_shards", targetShards)

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

	if len(targetShards) > 0 {
		if err := applyTargetShards(ts, targetShards); err != nil {
			return nil, err
		}
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		_, err := s.tmc.VDiff(ctx, target.GetPrimary().Tablet, tabletreq)
		return err
	})
	if err != nil {
		s.Logger().Errorf("Error executing vdiff stop action: %v", err)
		return nil, err
	}

	return &vtctldatapb.VDiffStopResponse{}, nil
}
