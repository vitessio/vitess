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
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
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
