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
	"flag"
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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/wrangler"
)

func commandVDiff2(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	_ = subFlags.Bool("v2", false, "Use VDiff2")

	timeout := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on primary migrations. The migration will be cancelled on a timeout.")
	maxRows := subFlags.Int64("limit", math.MaxInt64, "Max rows to stop comparing after")
	tables := subFlags.String("tables", "", "Only run vdiff for these tables in the workflow")

	sourceCell := subFlags.String("source_cell", "", "The source cell to compare from; default is any available cell")
	targetCell := subFlags.String("target_cell", "", "The target cell to compare with; default is any available cell")
	tabletTypes := subFlags.String("tablet_types", "in_order:RDONLY,REPLICA,PRIMARY", "Tablet types for source (PRIMARY is always used on target)")

	debugQuery := subFlags.Bool("debug_query", false, "Adds a mysql query to the report that can be used for further debugging")
	onlyPks := subFlags.Bool("only_pks", false, "When reporting missing rows, only show primary keys in the report.")
	format := subFlags.String("format", "", "Format of report") // "json" or ""
	maxExtraRowsToCompare := subFlags.Int64("max_extra_rows_to_compare", 1000, "If there are collation differences between the source and target, you can have rows that are identical but simply returned in a different order from MySQL. We will do a second pass to compare the rows for any actual differences in this case and this flag allows you to control the resources used for this operation.")

	resumable := subFlags.Bool("resumable", false, "Should this vdiff retry in case of recoverable errors, not yet implemented")
	checksum := subFlags.Bool("checksum", false, "Use row-level checksums to compare, not yet implemented")
	samplePct := subFlags.Int64("sample_pct", 100, "How many rows to sample, not yet implemented")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	var action vdiff.VDiffAction
	var actionArg string

	usage := fmt.Errorf("usage: VDiff -- --v2 <keyspace>.<workflow> %s [%s|<UUID>]", strings.Join(*(*[]string)(unsafe.Pointer(&vdiff.Actions)), "|"), strings.Join(vdiff.ActionArgs, "|"))
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
		return fmt.Errorf("invalid action %s; %s", subFlags.Arg(1), usage)
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
			Resumable:             *resumable,
			MaxRows:               *maxRows,
			Checksum:              *checksum,
			SamplePct:             *samplePct,
			TimeoutSeconds:        int64(timeout.Seconds()),
			MaxExtraRowsToCompare: *maxExtraRowsToCompare,
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			OnlyPKS:    *onlyPks,
			DebugQuery: *debugQuery,
			Format:     *format,
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
				return fmt.Errorf("can only show a specific vdiff, please provide a valid UUID; view all with: VDiff -- --v2 %s.%s show all", keyspace, workflowName)
			}
		}
	case vdiff.ResumeAction:
		vdiffUUID, err = uuid.Parse(actionArg)
		if err != nil {
			return fmt.Errorf("can only resume a specific vdiff, please provide a valid UUID; view all with: VDiff -- --v2 %s.%s show all", keyspace, workflowName)
		}
	default:
		return fmt.Errorf("invalid action %s; %s", action, usage)
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
		displayVDiff2ScheduledResponse(wr, *format, vdiffUUID.String(), action)
	case vdiff.ShowAction:
		if output == nil {
			// should not happen
			return fmt.Errorf("invalid (empty) response from show command")
		}
		if err := displayVDiff2ShowResponse(wr, *format, keyspace, workflowName, actionArg, output); err != nil {
			return err
		}
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
}

const (
	summaryTextTemplate = `
VDiff Summary for {{.Keyspace}}.{{.Workflow}} ({{.UUID}})
State:        {{.State}}
RowsCompared: {{.RowsCompared}}
HasMismatch:  {{.HasMismatch}}
StartedAt:    {{.StartedAt}}
CompletedAt:  {{.CompletedAt}}
{{ range $table := .TableSummaryMap}} 
Table {{$table.TableName}}:
	State:            {{$table.State}}
	ProcessedRows:    {{$table.RowsCompared}}
	MatchingRows:     {{$table.MatchingRows}}
{{ if $table.MismatchedRows}}	MismatchedRows:   {{$table.MismatchedRows}}{{ end }}
{{ if $table.ExtraRowsSource}}	ExtraRowsSource:  {{$table.ExtraRowsSource}}{{ end }}
{{ if $table.ExtraRowsTarget}}	ExtraRowsTarget:  {{$table.ExtraRowsTarget}}{{ end }}
{{ end }}
 
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

func displayVDiff2ShowResponse(wr *wrangler.Wrangler, format, keyspace, workflowName, actionArg string, output *wrangler.VDiffOutput) error {
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
		return displayVDiff2ShowSingleSummary(wr, format, keyspace, workflowName, vdiffUUID.String(), output)
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

func displayVDiff2ShowSingleSummary(wr *wrangler.Wrangler, format, keyspace, workflowName, uuid string, output *wrangler.VDiffOutput) error {
	str := ""
	summary, err := buildVDiff2SingleSummary(wr, keyspace, workflowName, uuid, output)
	if err != nil {
		return err
	}
	if format == "json" {
		jsonText, err := json.MarshalIndent(summary, "", "\t")
		if err != nil {
			return err
		}
		str = string(jsonText)
	} else {
		tmpl, err := template.New("test").Parse(summaryTextTemplate)
		if err != nil {
			return err
		}
		sb := new(strings.Builder)
		err = tmpl.Execute(sb, summary)
		if err != nil {
			return err
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
	return nil
}
func buildVDiff2SingleSummary(wr *wrangler.Wrangler, keyspace, workflow, uuid string, output *wrangler.VDiffOutput) (*vdiffSummary, error) {
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
	}

	var tableSummaryMap map[string]vdiffTableSummary
	var reports map[string]map[string]vdiff.DiffReport
	first := true
	var shards []string
	for shard, resp := range output.Responses {
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
				}

				{ // Global VDiff summary updates that take into account the per table details per shard
					summary.RowsCompared += row.AsInt64("rows_compared", 0)

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

					// The global VDiff summary needs to align with the table states across all
					// shards. It should progress from pending->started->completed with error at any point
					// being sticky. This largely mirrors the global table summary, with the exception
					// being the started state, which should also be sticky; i.e. we shouldn't go from
					// started->pending->started in the global VDiff summary state.
					if summary.State != ts.State &&
						(summary.State != vdiff.StartedState && ts.State != vdiff.PendingState) {
						summary.State = ts.State
					}
				}
			}
		}
	}
	sort.Strings(shards) // sort for predictable output, for test purposes
	summary.Shards = strings.Join(shards, ",")
	summary.TableSummaryMap = tableSummaryMap
	summary.Reports = reports
	if !summary.HasMismatch {
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

func displayVDiff2ScheduledResponse(wr *wrangler.Wrangler, format string, uuid string, typ vdiff.VDiffAction) {
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
