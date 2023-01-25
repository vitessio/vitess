/*
Copyright 2022 The Vitess Authors.

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

// code related to the handling of the vtctl vdiff2 command

package vtctl

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"text/template"
	"time"
	"unsafe"

	"github.com/bndr/gotabulate"
	"github.com/google/uuid"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func commandVDiff2(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	_ = subFlags.Bool("v1", false, "Use legacy VDiff v1")

	timeout := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on primary migrations. The migration will be cancelled on a timeout.")
	maxRows := subFlags.Int64("limit", math.MaxInt64, "Max rows to stop comparing after")
	tables := subFlags.String("tables", "", "Only run vdiff for these tables in the workflow")

	sourceCell := subFlags.String("source_cell", "", "The source cell to compare from; default is any available cell")
	targetCell := subFlags.String("target_cell", "", "The target cell to compare with; default is any available cell")
	tabletTypes := subFlags.String("tablet_types", "in_order:RDONLY,REPLICA,PRIMARY", "Tablet types for source (PRIMARY is always used on target)")

	debugQuery := subFlags.Bool("debug_query", false, "Adds a mysql query to the report that can be used for further debugging")
	onlyPks := subFlags.Bool("only_pks", false, "When reporting missing rows, only show primary keys in the report.")
	var format string
	subFlags.StringVar(&format, "format", "text", "Format of report") // "json" or "text"
	maxExtraRowsToCompare := subFlags.Int64("max_extra_rows_to_compare", 1000, "If there are collation differences between the source and target, you can have rows that are identical but simply returned in a different order from MySQL. We will do a second pass to compare the rows for any actual differences in this case and this flag allows you to control the resources used for this operation.")

	autoRetry := subFlags.Bool("auto-retry", true, "Should this vdiff automatically retry and continue in case of recoverable errors")
	checksum := subFlags.Bool("checksum", false, "Use row-level checksums to compare, not yet implemented")
	samplePct := subFlags.Int64("sample_pct", 100, "How many rows to sample, not yet implemented")
	verbose := subFlags.Bool("verbose", false, "Show verbose vdiff output in summaries")
	wait := subFlags.Bool("wait", false, "When creating or resuming a vdiff, wait for it to finish before exiting")
	waitUpdateInterval := subFlags.Duration("wait-update-interval", time.Duration(1*time.Minute), "When waiting on a vdiff to finish, check and display the current status this often")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	format = strings.ToLower(format)

	var action vdiff.VDiffAction
	var actionArg string

	usage := fmt.Errorf("usage: VDiff -- <keyspace>.<workflow> %s [%s|<UUID>]", strings.Join(*(*[]string)(unsafe.Pointer(&vdiff.Actions)), "|"), strings.Join(vdiff.ActionArgs, "|"))
	switch subFlags.NArg() {
	case 1: // for backward compatibility with vdiff1
		action = vdiff.CreateAction
	case 2:
		action = vdiff.VDiffAction(strings.ToLower(subFlags.Arg(1)))
		if action != vdiff.CreateAction {
			return usage
		}
	case 3:
		action = vdiff.VDiffAction(strings.ToLower(subFlags.Arg(1)))
		actionArg = strings.ToLower(subFlags.Arg(2))
	default:
		return usage
	}
	if action == "" {
		return fmt.Errorf("invalid action '%s'; %s", subFlags.Arg(1), usage)
	}
	keyspace, workflowName, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}

	if *maxRows <= 0 {
		return fmt.Errorf("invalid --limit value (%d), maximum number of rows to compare needs to be greater than 0", *maxRows)
	}

	options := &tabletmanagerdatapb.VDiffOptions{
		PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{
			TabletTypes: *tabletTypes,
			SourceCell:  *sourceCell,
			TargetCell:  *targetCell,
		},
		CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
			Tables:                *tables,
			AutoRetry:             *autoRetry,
			MaxRows:               *maxRows,
			Checksum:              *checksum,
			SamplePct:             *samplePct,
			TimeoutSeconds:        int64(timeout.Seconds()),
			MaxExtraRowsToCompare: *maxExtraRowsToCompare,
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			OnlyPks:    *onlyPks,
			DebugQuery: *debugQuery,
			Format:     format,
		},
	}

	var vdiffUUID uuid.UUID
	switch action {
	case vdiff.CreateAction:
		if actionArg != "" {
			vdiffUUID, err = uuid.Parse(actionArg)
		} else {
			vdiffUUID, err = uuid.NewUUID()
		}
		if err != nil {
			return fmt.Errorf("%v, please provide a valid UUID", err)
		}
	case vdiff.ShowAction:
		switch actionArg {
		case vdiff.AllActionArg, vdiff.LastActionArg:
		default:
			vdiffUUID, err = uuid.Parse(actionArg)
			if err != nil {
				return fmt.Errorf("can only show a specific vdiff, please provide a valid UUID; view all with: VDiff -- %s.%s show all", keyspace, workflowName)
			}
		}
	case vdiff.StopAction, vdiff.ResumeAction:
		vdiffUUID, err = uuid.Parse(actionArg)
		if err != nil {
			return fmt.Errorf("can only %s a specific vdiff, please provide a valid UUID; view all with: VDiff -- %s.%s show all", action, keyspace, workflowName)
		}
	case vdiff.DeleteAction:
		switch actionArg {
		case vdiff.AllActionArg:
		default:
			vdiffUUID, err = uuid.Parse(actionArg)
			if err != nil {
				return fmt.Errorf("can only delete a specific vdiff, please provide a valid UUID; view all with: VDiff -- %s.%s show all", keyspace, workflowName)
			}
		}
	default:
		return fmt.Errorf("invalid action '%s'; %s", action, usage)
	}

	type ErrorResponse struct {
		Error string
	}
	output, err := wr.VDiff2(ctx, keyspace, workflowName, action, actionArg, vdiffUUID.String(), options)
	if err != nil {
		log.Errorf("vdiff2 returning with error: %v", err)
		return err
	}

	switch action {
	case vdiff.CreateAction, vdiff.ResumeAction:
		if *wait {
			tkr := time.NewTicker(*waitUpdateInterval)
			defer tkr.Stop()
			var err error
			var state vdiff.VDiffState
			for {
				select {
				case <-ctx.Done():
					return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
				case <-tkr.C:
					if output, err = wr.VDiff2(ctx, keyspace, workflowName, vdiff.ShowAction, vdiffUUID.String(), vdiffUUID.String(), options); err != nil {
						return err
					}
					if state, err = displayVDiff2ShowSingleSummary(wr, format, keyspace, workflowName, vdiffUUID.String(), output, *verbose); err != nil {
						return err
					}
					if state == vdiff.CompletedState {
						return nil
					}
				}
			}
		} else {
			displayVDiff2ScheduledResponse(wr, format, vdiffUUID.String(), action)
		}
	case vdiff.ShowAction:
		if output == nil {
			// should not happen
			return fmt.Errorf("invalid (empty) response from show command")
		}
		if err := displayVDiff2ShowResponse(wr, format, keyspace, workflowName, actionArg, output, *verbose); err != nil {
			return err
		}
	case vdiff.StopAction, vdiff.DeleteAction:
		uuidToDisplay := ""
		if actionArg != vdiff.AllActionArg {
			uuidToDisplay = vdiffUUID.String()
		}
		displayVDiff2ActionStatusResponse(wr, format, uuidToDisplay, action, vdiff.CompletedState)
	default:
		return fmt.Errorf("invalid action %s; %s", action, usage)
	}

	return nil
}

//region ****show response

// summary aggregates/selects the current state of the vdiff from all shards

type vdiffTableSummary struct {
	TableName       string
	State           vdiff.VDiffState
	RowsCompared    int64
	MatchingRows    int64
	MismatchedRows  int64
	ExtraRowsSource int64
	ExtraRowsTarget int64
	LastUpdated     string `json:"LastUpdated,omitempty"`
}
type vdiffSummary struct {
	Workflow, Keyspace string
	State              vdiff.VDiffState
	UUID               string
	RowsCompared       int64
	HasMismatch        bool
	Shards             string
	StartedAt          string                                 `json:"StartedAt,omitempty"`
	CompletedAt        string                                 `json:"CompletedAt,omitempty"`
	TableSummaryMap    map[string]vdiffTableSummary           `json:"TableSummary,omitempty"`
	Reports            map[string]map[string]vdiff.DiffReport `json:"Reports,omitempty"`
	Errors             map[string]string                      `json:"Errors,omitempty"`
	Progress           *vdiff.ProgressReport                  `json:"Progress,omitempty"`
}

const (
	summaryTextTemplate = `
VDiff Summary for {{.Keyspace}}.{{.Workflow}} ({{.UUID}})
State:        {{.State}}
{{if .Errors}}
{{- range $shard, $error := .Errors}}
              Error: (shard {{$shard}}) {{$error}}
{{- end}}
{{end}}
RowsCompared: {{.RowsCompared}}
HasMismatch:  {{.HasMismatch}}
StartedAt:    {{.StartedAt}}
{{if (eq .State "started")}}Progress:     {{printf "%.2f" .Progress.Percentage}}%%{{if .Progress.ETA}}, ETA: {{.Progress.ETA}}{{end}}{{end}}
{{if .CompletedAt}}CompletedAt:  {{.CompletedAt}}{{end}}
{{range $table := .TableSummaryMap}} 
Table {{$table.TableName}}:
	State:            {{$table.State}}
	ProcessedRows:    {{$table.RowsCompared}}
	MatchingRows:     {{$table.MatchingRows}}
{{if $table.MismatchedRows}}	MismatchedRows:   {{$table.MismatchedRows}}{{end}}
{{if $table.ExtraRowsSource}}	ExtraRowsSource:  {{$table.ExtraRowsSource}}{{end}}
{{if $table.ExtraRowsTarget}}	ExtraRowsTarget:  {{$table.ExtraRowsTarget}}{{end}}
{{end}}
 
Use "--format=json" for more detailed output.
`
)

type VDiffListing struct {
	UUID, Workflow, Keyspace, Shard, State string
}

func (vdl *VDiffListing) String() string {
	str := fmt.Sprintf("UUID: %s, Workflow: %s, Keyspace: %s, Shard: %s, State: %s",
		vdl.UUID, vdl.Workflow, vdl.Keyspace, vdl.Shard, vdl.State)
	return str
}

func getStructFieldNames(s any) []string {
	t := reflect.TypeOf(s)

	names := make([]string, t.NumField())
	for i := range names {
		names[i] = t.Field(i).Name
	}

	return names
}

func displayListings(listings []*VDiffListing) string {
	var strArray2 [][]string
	var strArray []string
	str := ""

	if len(listings) == 0 {
		return ""
	}
	fields := getStructFieldNames(VDiffListing{})
	strArray = append(strArray, fields...)
	strArray2 = append(strArray2, strArray)
	for _, listing := range listings {
		strArray = nil
		v := reflect.ValueOf(*listing)
		for _, field := range fields {
			strArray = append(strArray, v.FieldByName(field).String())
		}
		strArray2 = append(strArray2, strArray)
	}
	t := gotabulate.Create(strArray2)
	str = t.Render("grid")
	return str
}

func displayVDiff2ShowResponse(wr *wrangler.Wrangler, format, keyspace, workflowName, actionArg string, output *wrangler.VDiffOutput, verbose bool) error {
	var vdiffUUID uuid.UUID
	var err error
	switch actionArg {
	case vdiff.AllActionArg:
		return displayVDiff2ShowRecent(wr, format, keyspace, workflowName, actionArg, output)
	case vdiff.LastActionArg:
		for _, resp := range output.Responses {
			vdiffUUID, err = uuid.Parse(resp.VdiffUuid)
			if err != nil {
				if format == "json" {
					wr.Logger().Printf("{}\n")
				} else {
					wr.Logger().Printf("No previous vdiff found for %s.%s\n", keyspace, workflowName)
				}
				return nil
			}
			break
		}
		fallthrough
	default:
		if vdiffUUID == uuid.Nil { // then it must be passed as the action arg
			vdiffUUID, err = uuid.Parse(actionArg)
			if err != nil {
				return err
			}
		}
		if len(output.Responses) == 0 {
			return fmt.Errorf("no response received for vdiff show of %s.%s(%s)", keyspace, workflowName, vdiffUUID.String())
		}
		_, err := displayVDiff2ShowSingleSummary(wr, format, keyspace, workflowName, vdiffUUID.String(), output, verbose)
		return err
	}
}

func displayVDiff2ShowRecent(wr *wrangler.Wrangler, format, keyspace, workflowName, subCommand string, output *wrangler.VDiffOutput) error {
	str := ""
	recent, err := buildVDiff2Recent(output)
	if err != nil {
		return err
	}
	if format == "json" {
		jsonText, err := json.MarshalIndent(recent, "", "\t")
		if err != nil {
			return err
		}
		str = string(jsonText)
		if str == "null" {
			str = "[]"
		}
	} else {
		str = displayListings(recent)
		if str == "" {
			str = fmt.Sprintf("No vdiffs found for %s.%s", keyspace, workflowName)
		}
	}
	wr.Logger().Printf(str + "\n")
	return nil
}

func buildVDiff2Recent(output *wrangler.VDiffOutput) ([]*VDiffListing, error) {
	var listings []*VDiffListing
	for _, resp := range output.Responses {
		if resp != nil && resp.Output != nil {
			qr := sqltypes.Proto3ToResult(resp.Output)
			for _, row := range qr.Named().Rows {
				listings = append(listings, &VDiffListing{
					UUID:     row["vdiff_uuid"].ToString(),
					Workflow: row["workflow"].ToString(),
					Keyspace: row["keyspace"].ToString(),
					Shard:    row["shard"].ToString(),
					State:    row["state"].ToString(),
				})
			}
		}
	}
	return listings, nil
}

func displayVDiff2ShowSingleSummary(wr *wrangler.Wrangler, format, keyspace, workflowName, uuid string, output *wrangler.VDiffOutput, verbose bool) (vdiff.VDiffState, error) {
	state := vdiff.UnknownState
	str := ""
	summary, err := buildVDiff2SingleSummary(wr, keyspace, workflowName, uuid, output, verbose)
	if err != nil {
		return state, err
	}
	state = summary.State
	if format == "json" {
		jsonText, err := json.MarshalIndent(summary, "", "\t")
		if err != nil {
			return state, err
		}
		str = string(jsonText)
	} else {
		tmpl, err := template.New("test").Parse(summaryTextTemplate)
		if err != nil {
			return state, err
		}
		sb := new(strings.Builder)
		err = tmpl.Execute(sb, summary)
		if err != nil {
			return state, err
		}
		str = sb.String()
		for {
			str2 := strings.Replace(str, "\n\n", "\n", -1)
			if str == str2 {
				break
			}
			str = str2
		}
	}
	wr.Logger().Printf(str + "\n")
	return state, nil
}
func buildVDiff2SingleSummary(wr *wrangler.Wrangler, keyspace, workflow, uuid string, output *wrangler.VDiffOutput, verbose bool) (*vdiffSummary, error) {
	summary := &vdiffSummary{
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

	var tableSummaryMap map[string]vdiffTableSummary
	var reports map[string]map[string]vdiff.DiffReport
	// Keep a tally of the states across all tables in all shards
	tableStateCounts := map[vdiff.VDiffState]int{
		vdiff.UnknownState:   0,
		vdiff.PendingState:   0,
		vdiff.StartedState:   0,
		vdiff.StoppedState:   0,
		vdiff.ErrorState:     0,
		vdiff.CompletedState: 0,
	}
	// Keep a tally of the summary states across all shards
	shardStateCounts := map[vdiff.VDiffState]int{
		vdiff.UnknownState:   0,
		vdiff.PendingState:   0,
		vdiff.StartedState:   0,
		vdiff.StoppedState:   0,
		vdiff.ErrorState:     0,
		vdiff.CompletedState: 0,
	}
	// Keep a tally of the approximate total rows to process as we'll use this for our progress report
	totalRowsToCompare := int64(0)
	var shards []string
	for shard, resp := range output.Responses {
		first := true
		if resp != nil && resp.Output != nil {
			shards = append(shards, shard)
			qr := sqltypes.Proto3ToResult(resp.Output)
			if tableSummaryMap == nil {
				tableSummaryMap = make(map[string]vdiffTableSummary, 0)
				reports = make(map[string]map[string]vdiff.DiffReport, 0)
			}
			for _, row := range qr.Named().Rows {
				// Update the global VDiff summary based on the per shard level summary.
				// Since these values will be the same for all subsequent rows we only use the first row.
				if first {
					first = false
					// Our timestamps are strings in `2022-06-26 20:43:25` format so we sort them lexicographically.
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
					// Keep track of how many shards are marked as a specific state. We check this combined
					// with the shard.table states to determine the VDiff summary state.
					shardStateCounts[vdiff.VDiffState(strings.ToLower(row.AsString("vdiff_state", "")))]++
				}

				{ // Global VDiff summary updates that take into account the per table details per shard
					summary.RowsCompared += row.AsInt64("rows_compared", 0)
					totalRowsToCompare += row.AsInt64("table_rows", 0)

					// If we had a mismatch on any table on any shard then the global VDiff summary does too
					if mm, _ := row.ToBool("has_mismatch"); mm {
						summary.HasMismatch = true
					}
				}

				{ // Table summary information that must be accounted for across all shards
					table := row.AsString("table_name", "")
					// Create the global VDiff table summary object if it doesn't exist
					if _, ok := tableSummaryMap[table]; !ok {
						tableSummaryMap[table] = vdiffTableSummary{
							TableName: table,
							State:     vdiff.UnknownState,
						}

					}
					ts := tableSummaryMap[table]
					// This is the shard level VDiff table state
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
		// So we only mark the vdiff for the shard as completed when we've finished processing
		// rows from all of the sources -- which is recorded by marking the vdiff done for the
		// shard by setting _vt.vdiff.state = completed.
		if shardStateCounts[vdiff.CompletedState] == len(shards) {
			summary.State = vdiff.CompletedState
		} else {
			summary.State = vdiff.StartedState
		}
	} else {
		summary.State = vdiff.UnknownState
	}

	// If the vdiff has been started then we can calculate the progress
	if summary.State == vdiff.StartedState {
		buildProgressReport(summary, totalRowsToCompare)
	}

	sort.Strings(shards) // sort for predictable output
	summary.Shards = strings.Join(shards, ",")
	summary.TableSummaryMap = tableSummaryMap
	summary.Reports = reports
	if !summary.HasMismatch && !verbose {
		summary.Reports = nil
		summary.TableSummaryMap = nil
	}
	// If we haven't completed the global VDiff then be sure to reflect that with no CompletedAt value
	if summary.State != vdiff.CompletedState {
		summary.CompletedAt = ""
	}
	return summary, nil
}

//endregion

func displayVDiff2ScheduledResponse(wr *wrangler.Wrangler, format, uuid string, typ vdiff.VDiffAction) {
	if format == "json" {
		type ScheduledResponse struct {
			UUID string
		}
		resp := &ScheduledResponse{UUID: uuid}
		jsonText, _ := json.MarshalIndent(resp, "", "\t")
		wr.Logger().Printf(string(jsonText) + "\n")
	} else {
		addtlMsg := ""
		if typ == vdiff.ResumeAction {
			addtlMsg = "to resume "
		}
		msg := fmt.Sprintf("VDiff %s scheduled %son target shards, use show to view progress\n", uuid, addtlMsg)
		wr.Logger().Printf(msg)
	}
}

func displayVDiff2ActionStatusResponse(wr *wrangler.Wrangler, format, uuid string, action vdiff.VDiffAction, status vdiff.VDiffState) {
	if format == "json" {
		type ActionStatusResponse struct {
			UUID   string `json:"UUID,omitempty"`
			Action vdiff.VDiffAction
			Status vdiff.VDiffState
		}
		resp := &ActionStatusResponse{UUID: uuid, Action: action, Status: status}
		jsonText, _ := json.MarshalIndent(resp, "", "\t")
		wr.Logger().Printf(string(jsonText) + "\n")
	} else {
		msg := fmt.Sprintf("The %s action for vdiff %s is %s on target shards\n", action, uuid, status)
		wr.Logger().Printf(msg)
	}
}

func buildProgressReport(summary *vdiffSummary, rowsToCompare int64) {
	report := &vdiff.ProgressReport{}
	if summary.RowsCompared >= 1 {
		// Round to 2 decimal points
		report.Percentage = math.Round(math.Min((float64(summary.RowsCompared)/float64(rowsToCompare))*100, 100.00)*100) / 100
	}
	if math.IsNaN(report.Percentage) {
		report.Percentage = 0
	}
	pctToGo := math.Abs(report.Percentage - 100.00)
	startTime, _ := time.Parse(vdiff.TimestampFormat, summary.StartedAt)
	curTime := time.Now().UTC()
	runTime := curTime.Unix() - startTime.Unix()
	if report.Percentage >= 1 {
		// calculate how long 1% took, on avg, and multiply that by the % left
		eta := time.Unix(((int64(runTime)/int64(report.Percentage))*int64(pctToGo))+curTime.Unix(), 1).UTC()
		// cap the ETA at 1 year out to prevent providing nonsensical ETAs
		if eta.Before(time.Now().UTC().AddDate(1, 0, 0)) {
			report.ETA = eta.Format(vdiff.TimestampFormat)
		}
	}
	summary.Progress = report
}
