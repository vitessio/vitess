/*
Copyright 2023 The Vitess Authors.

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

package vdiff

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/bndr/gotabulate"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	tabletTypesDefault = []topodatapb.TabletType{
		topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_PRIMARY,
	}

	createOptions = struct {
		UUID                        uuid.UUID
		SourceCells                 []string
		TargetCells                 []string
		TabletTypes                 []topodatapb.TabletType
		Tables                      []string
		Limit                       int64
		FilteredReplicationWaitTime time.Duration
		DebugQuery                  bool
		MaxReportSampleRows         int64
		OnlyPKs                     bool
		UpdateTableStats            bool
		MaxExtraRowsToCompare       int64
		Wait                        bool
		WaitUpdateInterval          time.Duration
		AutoRetry                   bool
		MaxDiffDuration             time.Duration
	}{}

	deleteOptions = struct {
		Arg string
	}{}

	resumeOptions = struct {
		UUID uuid.UUID
	}{}

	showOptions = struct {
		Arg     string
		Verbose bool
	}{}

	stopOptions = struct {
		UUID uuid.UUID
	}{}

	parseAndValidateCreate = func(cmd *cobra.Command, args []string) error {
		var err error
		if len(args) == 1 { // Validate UUID if provided
			if createOptions.UUID, err = uuid.Parse(args[0]); err != nil {
				return fmt.Errorf("invalid UUID provided: %v", err)
			}
		} else { // Generate a UUID
			createOptions.UUID = uuid.New()
		}
		if !cmd.Flags().Lookup("tablet-types").Changed {
			createOptions.TabletTypes = tabletTypesDefault
		}
		if cmd.Flags().Lookup("source-cells").Changed {
			for i, cell := range createOptions.SourceCells {
				createOptions.SourceCells[i] = strings.TrimSpace(cell)
			}
		}
		if cmd.Flags().Lookup("target-cells").Changed {
			for i, cell := range createOptions.TargetCells {
				createOptions.TargetCells[i] = strings.TrimSpace(cell)
			}
		}
		if cmd.Flags().Lookup("tables").Changed {
			for i, table := range createOptions.Tables {
				createOptions.Tables[i] = strings.TrimSpace(table)
			}
		}
		// Enforce non-negative values for limits and max options.
		if createOptions.Limit < 1 {
			return fmt.Errorf("--limit must be a positive value")
		}
		if createOptions.MaxReportSampleRows < 0 {
			return fmt.Errorf("--max-report-sample-rows must not be a negative value")
		}
		if createOptions.MaxExtraRowsToCompare < 0 {
			return fmt.Errorf("--max-extra-rows-to-compare must not be a negative value")
		}
		return nil
	}

	// base is the base command for all actions related to VDiff.
	base = &cobra.Command{
		Use:                   "VDiff --workflow <workflow> --target-keyspace <keyspace> [command] [command-flags]",
		Short:                 "Perform commands related to diffing tables involved in a VReplication workflow between the source and target.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"vdiff"},
		Args:                  cobra.NoArgs,
	}

	// create makes a VDiffCreate gRPC call to a vtctld.
	create = &cobra.Command{
		Use:   "create",
		Short: "Create and run a VDiff to compare the tables involved in a VReplication workflow between the source and target.",
		Example: `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer create
vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer create b3f59678-5241-11ee-be56-0242ac120002`,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.MaximumNArgs(1),
		PreRunE:               parseAndValidateCreate,
		RunE:                  commandCreate,
	}

	// delete makes a VDiffDelete gRPC call to a vtctld.
	delete = &cobra.Command{
		Use:   "delete",
		Short: "Delete VDiffs.",
		Example: `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer delete a037a9e2-5628-11ee-8c99-0242ac120002
vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace delete all`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Delete"},
		Args:                  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			larg := strings.ToLower(args[0])
			switch larg {
			case "all":
			default:
				if _, err := uuid.Parse(args[0]); err != nil {
					return fmt.Errorf("invalid argument provided (%s), valid arguments are 'all' or a valid UUID",
						args[0])
				}
			}
			deleteOptions.Arg = larg
			return nil
		},
		RunE: commandDelete,
	}

	// resume makes a VDiffResume gRPC call to a vtctld.
	resume = &cobra.Command{
		Use:                   "resume",
		Short:                 "Resume a VDiff.",
		Example:               `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer resume a037a9e2-5628-11ee-8c99-0242ac120002`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Resume"},
		Args:                  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			uuid, err := uuid.Parse(args[0])
			if err != nil {
				return fmt.Errorf("invalid UUID provided: %v", err)
			}
			resumeOptions.UUID = uuid
			return nil
		},
		RunE: commandResume,
	}

	// show makes a VDiffShow gRPC call to a vtctld.
	show = &cobra.Command{
		Use:   "show",
		Short: "Show the status of a VDiff.",
		Example: `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer show last
vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer show a037a9e2-5628-11ee-8c99-0242ac120002
vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer show all`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			larg := strings.ToLower(args[0])
			switch larg {
			case "last", "all":
			default:
				if _, err := uuid.Parse(args[0]); err != nil {
					return fmt.Errorf("invalid argument provided (%s), valid arguments are 'all', 'last', or a valid UUID",
						args[0])
				}
			}
			showOptions.Arg = larg
			return nil
		},
		RunE: commandShow,
	}

	// stop makes a VDiffStop gRPC call to a vtctld.
	stop = &cobra.Command{
		Use:                   "stop",
		Short:                 "Stop a running VDiff.",
		Example:               `vtctldclient --server localhost:15999 vdiff --workflow commerce2customer --target-keyspace customer stop a037a9e2-5628-11ee-8c99-0242ac120002`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Stop"},
		Args:                  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			uuid, err := uuid.Parse(args[0])
			if err != nil {
				return fmt.Errorf("invalid UUID provided: %v", err)
			}
			stopOptions.UUID = uuid
			return nil
		},
		RunE: commandStop,
	}
)

type simpleResponse struct {
	Action vdiff.VDiffAction
	Status string
}

// displaySimpleResponse displays a simple standard response for the
// resume, stop, and delete commands after the client command completes
// without an error.
func displaySimpleResponse(out io.Writer, format string, action vdiff.VDiffAction) {
	status := "completed"
	if action == vdiff.ResumeAction {
		status = "scheduled"
	}
	if format == "json" {
		resp := &simpleResponse{
			Action: action,
			Status: status,
		}
		jsonText, _ := cli.MarshalJSONPretty(resp)
		fmt.Fprintln(out, string(jsonText))
	} else {
		fmt.Fprintf(out, "VDiff %s %s\n", action, status)
	}
}

func commandCreate(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	resp, err := common.GetClient().VDiffCreate(common.GetCommandCtx(), &vtctldatapb.VDiffCreateRequest{
		Workflow:                    common.BaseOptions.Workflow,
		TargetKeyspace:              common.BaseOptions.TargetKeyspace,
		Uuid:                        createOptions.UUID.String(),
		SourceCells:                 createOptions.SourceCells,
		TargetCells:                 createOptions.TargetCells,
		TabletTypes:                 createOptions.TabletTypes,
		TabletSelectionPreference:   tsp,
		Tables:                      createOptions.Tables,
		Limit:                       createOptions.Limit,
		FilteredReplicationWaitTime: protoutil.DurationToProto(createOptions.FilteredReplicationWaitTime),
		DebugQuery:                  createOptions.DebugQuery,
		OnlyPKs:                     createOptions.OnlyPKs,
		UpdateTableStats:            createOptions.UpdateTableStats,
		MaxExtraRowsToCompare:       createOptions.MaxExtraRowsToCompare,
		Wait:                        createOptions.Wait,
		WaitUpdateInterval:          protoutil.DurationToProto(createOptions.WaitUpdateInterval),
		AutoRetry:                   createOptions.AutoRetry,
		MaxReportSampleRows:         createOptions.MaxReportSampleRows,
		MaxDiffDuration:             protoutil.DurationToProto(createOptions.MaxDiffDuration),
	})

	if err != nil {
		return err
	}

	if createOptions.Wait {
		tkr := time.NewTicker(createOptions.WaitUpdateInterval)
		defer tkr.Stop()
		var state vdiff.VDiffState
		ctx := common.GetCommandCtx()
		vtctldClient := common.GetClient()
		uuidStr := createOptions.UUID.String()
		for {
			select {
			case <-ctx.Done():
				return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
			case <-tkr.C:
				resp, err := vtctldClient.VDiffShow(ctx, &vtctldatapb.VDiffShowRequest{
					Workflow:       common.BaseOptions.Workflow,
					TargetKeyspace: common.BaseOptions.TargetKeyspace,
					Arg:            uuidStr,
				})
				if err != nil {
					return err
				}
				if state, err = displayShowSingleSummary(cmd.OutOrStdout(), format, common.BaseOptions.TargetKeyspace, common.BaseOptions.Workflow, uuidStr, resp, false); err != nil {
					return err
				}
				if state == vdiff.CompletedState {
					return nil
				}
			}
		}
	} else {
		var data []byte
		if format == "json" {
			data, err = cli.MarshalJSONPretty(resp)
			if err != nil {
				return err
			}
		} else {
			data = []byte(fmt.Sprintf("VDiff %s scheduled on target shards, use show to view progress", resp.UUID))
		}
		fmt.Println(string(data))
	}

	return nil
}

func commandDelete(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	_, err = common.GetClient().VDiffDelete(common.GetCommandCtx(), &vtctldatapb.VDiffDeleteRequest{
		Workflow:       common.BaseOptions.Workflow,
		TargetKeyspace: common.BaseOptions.TargetKeyspace,
		Arg:            deleteOptions.Arg,
	})

	if err != nil {
		return err
	}

	displaySimpleResponse(cmd.OutOrStdout(), format, vdiff.DeleteAction)

	return nil
}

func commandResume(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	_, err = common.GetClient().VDiffResume(common.GetCommandCtx(), &vtctldatapb.VDiffResumeRequest{
		Workflow:       common.BaseOptions.Workflow,
		TargetKeyspace: common.BaseOptions.TargetKeyspace,
		Uuid:           resumeOptions.UUID.String(),
	})

	if err != nil {
		return err
	}

	displaySimpleResponse(cmd.OutOrStdout(), format, vdiff.ResumeAction)

	return nil
}

// tableSummary aggregates the current state of the table diff from all shards.
type tableSummary struct {
	TableName       string
	State           vdiff.VDiffState
	RowsCompared    int64
	MatchingRows    int64
	MismatchedRows  int64
	ExtraRowsSource int64
	ExtraRowsTarget int64
	LastUpdated     string `json:"LastUpdated,omitempty"`
}

// summary aggregates the current state of the vdiff from all shards.
type summary struct {
	Workflow, Keyspace string
	State              vdiff.VDiffState
	UUID               string
	RowsCompared       int64
	HasMismatch        bool
	Shards             string
	StartedAt          string                                 `json:"StartedAt,omitempty"`
	CompletedAt        string                                 `json:"CompletedAt,omitempty"`
	TableSummaryMap    map[string]tableSummary                `json:"TableSummary,omitempty"`
	Reports            map[string]map[string]vdiff.DiffReport `json:"Reports,omitempty"`
	Errors             map[string]string                      `json:"Errors,omitempty"`
	Progress           *vdiff.ProgressReport                  `json:"Progress,omitempty"`
}

const summaryTextTemplate = `
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

type listing struct {
	UUID, Workflow, Keyspace, Shard, State string
}

func (vdl *listing) String() string {
	return fmt.Sprintf("UUID: %s, Workflow: %s, Keyspace: %s, Shard: %s, State: %s",
		vdl.UUID, vdl.Workflow, vdl.Keyspace, vdl.Shard, vdl.State)
}

func getStructFieldNames(s any) []string {
	t := reflect.TypeOf(s)

	names := make([]string, t.NumField())
	for i := range names {
		names[i] = t.Field(i).Name
	}

	return names
}

func buildListings(listings []*listing) string {
	var values []string
	var lines [][]string
	var result string

	if len(listings) == 0 {
		return ""
	}
	// Get the column headers.
	fields := getStructFieldNames(listing{})
	// The header is the first row.
	lines = append(lines, fields)
	for _, listing := range listings {
		v := reflect.ValueOf(*listing)
		for _, field := range fields {
			values = append(values, v.FieldByName(field).String())
		}
		lines = append(lines, values)
	}
	t := gotabulate.Create(lines)
	result = t.Render("grid")
	return result
}

func displayShowResponse(out io.Writer, format, keyspace, workflowName, actionArg string, resp *vtctldatapb.VDiffShowResponse, verbose bool) error {
	var vdiffUUID uuid.UUID
	var err error
	switch actionArg {
	case vdiff.AllActionArg:
		return displayShowRecent(out, format, keyspace, workflowName, actionArg, resp)
	case vdiff.LastActionArg:
		for _, resp := range resp.TabletResponses {
			vdiffUUID, err = uuid.Parse(resp.VdiffUuid)
			if err != nil {
				if format == "json" {
					fmt.Fprintln(out, "{}")
				} else {
					fmt.Fprintf(out, "No previous vdiff found for %s.%s\n", keyspace, workflowName)
				}
				return nil
			}
			break
		}
		fallthrough
	default:
		if vdiffUUID == uuid.Nil { // Then it must be passed as the action arg
			vdiffUUID, err = uuid.Parse(actionArg)
			if err != nil {
				return err
			}
		}
		if len(resp.TabletResponses) == 0 {
			return fmt.Errorf("no response received for vdiff show of %s.%s (%s)", keyspace, workflowName, vdiffUUID.String())
		}
		_, err = displayShowSingleSummary(out, format, keyspace, workflowName, vdiffUUID.String(), resp, verbose)
		return err
	}
}

func displayShowRecent(out io.Writer, format, keyspace, workflowName, subCommand string, resp *vtctldatapb.VDiffShowResponse) error {
	output := ""
	recentListings, err := buildRecentListings(resp)
	if err != nil {
		return err
	}
	if format == "json" {
		jsonText, err := cli.MarshalJSONPretty(recentListings)
		if err != nil {
			return err
		}
		output = string(jsonText)
		if output == "null" {
			output = "[]"
		}
	} else {
		output = buildListings(recentListings)
		if output == "" {
			output = fmt.Sprintf("No vdiffs found for %s.%s", keyspace, workflowName)
		}
	}
	fmt.Fprintln(out, output)
	return nil
}

func buildRecentListings(resp *vtctldatapb.VDiffShowResponse) ([]*listing, error) {
	var listings []*listing
	for _, resp := range resp.TabletResponses {
		if resp != nil && resp.Output != nil {
			qr := sqltypes.Proto3ToResult(resp.Output)
			for _, row := range qr.Named().Rows {
				listings = append(listings, &listing{
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

func displayShowSingleSummary(out io.Writer, format, keyspace, workflowName, uuid string, resp *vtctldatapb.VDiffShowResponse, verbose bool) (vdiff.VDiffState, error) {
	state := vdiff.UnknownState
	var output string
	summary, err := buildSingleSummary(keyspace, workflowName, uuid, resp, verbose)
	if err != nil {
		return state, err
	}
	if summary == nil { // Should never happen
		return state, fmt.Errorf("no report to show for vdiff %s.%s (%s)", keyspace, workflowName, uuid)
	}
	state = summary.State
	if format == "json" {
		jsonText, err := cli.MarshalJSONPretty(summary)
		if err != nil {
			return state, err
		}
		output = string(jsonText)
	} else {
		tmpl, err := template.New("summary").Parse(summaryTextTemplate)
		if err != nil {
			return state, err
		}
		sb := new(strings.Builder)
		err = tmpl.Execute(sb, summary)
		if err != nil {
			return state, err
		}
		output = sb.String()
		for {
			str := strings.Replace(output, "\n\n", "\n", -1)
			if output == str {
				break
			}
			output = str
		}
	}
	fmt.Fprintln(out, output)
	return state, nil
}

func buildSingleSummary(keyspace, workflow, uuid string, resp *vtctldatapb.VDiffShowResponse, verbose bool) (*summary, error) {
	summary := &summary{
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

	var tableSummaryMap map[string]tableSummary
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
				tableSummaryMap = make(map[string]tableSummary, 0)
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
					// Create the global VDiff table summary object if it doesn't exist.
					if _, ok := tableSummaryMap[table]; !ok {
						tableSummaryMap[table] = tableSummary{
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
		buildProgressReport(summary, totalRowsToCompare)
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

func buildProgressReport(summary *summary, rowsToCompare int64) {
	report := &vdiff.ProgressReport{}
	if summary.RowsCompared >= 1 {
		// Round to 2 decimal points.
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
		// Calculate how long 1% took, on avg, and multiply that by the % left.
		eta := time.Unix(((int64(runTime)/int64(report.Percentage))*int64(pctToGo))+curTime.Unix(), 1).UTC()
		// Cap the ETA at 1 year out to prevent providing nonsensical ETAs.
		if eta.Before(time.Now().UTC().AddDate(1, 0, 0)) {
			report.ETA = eta.Format(vdiff.TimestampFormat)
		}
	}
	summary.Progress = report
}

func commandShow(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	resp, err := common.GetClient().VDiffShow(common.GetCommandCtx(), &vtctldatapb.VDiffShowRequest{
		Workflow:       common.BaseOptions.Workflow,
		TargetKeyspace: common.BaseOptions.TargetKeyspace,
		Arg:            showOptions.Arg,
	})

	if err != nil {
		return err
	}

	if err = displayShowResponse(cmd.OutOrStdout(), format, common.BaseOptions.TargetKeyspace, common.BaseOptions.Workflow, showOptions.Arg, resp, showOptions.Verbose); err != nil {
		return err
	}

	return nil
}

func commandStop(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	_, err = common.GetClient().VDiffStop(common.GetCommandCtx(), &vtctldatapb.VDiffStopRequest{
		Workflow:       common.BaseOptions.Workflow,
		TargetKeyspace: common.BaseOptions.TargetKeyspace,
		Uuid:           stopOptions.UUID.String(),
	})

	if err != nil {
		return err
	}

	displaySimpleResponse(cmd.OutOrStdout(), format, vdiff.StopAction)

	return nil
}

func registerCommands(root *cobra.Command) {
	common.AddCommonFlags(base)
	root.AddCommand(base)

	create.Flags().StringSliceVar(&createOptions.SourceCells, "source-cells", nil, "The source cell(s) to compare from; default is any available cell.")
	create.Flags().StringSliceVar(&createOptions.TargetCells, "target-cells", nil, "The target cell(s) to compare with; default is any available cell.")
	create.Flags().Var((*topoprotopb.TabletTypeListFlag)(&createOptions.TabletTypes), "tablet-types", "Tablet types to use on the source and target.")
	create.Flags().BoolVar(&common.CreateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	create.Flags().DurationVar(&createOptions.FilteredReplicationWaitTime, "filtered-replication-wait-time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for replication to catch up when syncing tablet streams.")
	create.Flags().Int64Var(&createOptions.Limit, "limit", math.MaxInt64, "Max rows to stop comparing after.")
	create.Flags().BoolVar(&createOptions.DebugQuery, "debug-query", false, "Adds a mysql query to the report that can be used for further debugging.")
	create.Flags().Int64Var(&createOptions.MaxReportSampleRows, "max-report-sample-rows", 10, "Maximum number of row differences to report (0 for all differences). NOTE: when increasing this value it is highly recommended to also specify --only-pks")
	create.Flags().BoolVar(&createOptions.OnlyPKs, "only-pks", false, "When reporting missing rows, only show primary keys in the report.")
	create.Flags().StringSliceVar(&createOptions.Tables, "tables", nil, "Only run vdiff for these tables in the workflow.")
	create.Flags().Int64Var(&createOptions.MaxExtraRowsToCompare, "max-extra-rows-to-compare", 1000, "If there are collation differences between the source and target, you can have rows that are identical but simply returned in a different order from MySQL. We will do a second pass to compare the rows for any actual differences in this case and this flag allows you to control the resources used for this operation.")
	create.Flags().BoolVar(&createOptions.Wait, "wait", false, "When creating or resuming a vdiff, wait for it to finish before exiting.")
	create.Flags().DurationVar(&createOptions.WaitUpdateInterval, "wait-update-interval", time.Duration(1*time.Minute), "When waiting on a vdiff to finish, check and display the current status this often.")
	create.Flags().BoolVar(&createOptions.AutoRetry, "auto-retry", true, "Should this vdiff automatically retry and continue in case of recoverable errors.")
	create.Flags().BoolVar(&createOptions.UpdateTableStats, "update-table-stats", false, "Update the table statistics, using ANALYZE TABLE, on each table involved in the VDiff during initialization. This will ensure that progress estimates are as accurate as possible -- but it does involve locks and can potentially impact query processing on the target keyspace.")
	create.Flags().DurationVar(&createOptions.MaxDiffDuration, "max-diff-duration", 0, "How long should an individual table diff run before being stopped and restarted in order to lessen the impact on tablets due to holding open database snapshots for long periods of time (0 is the default and means no time limit).")
	base.AddCommand(create)

	base.AddCommand(delete)

	base.AddCommand(resume)

	show.Flags().BoolVar(&showOptions.Verbose, "verbose", false, "Show verbose output in summaries")
	base.AddCommand(show)

	base.AddCommand(stop)
}

func init() {
	common.RegisterCommandHandler("VDiff", registerCommands)
}
