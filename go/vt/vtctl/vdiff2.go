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
	"strings"
	"text/template"
	"time"

	"github.com/bndr/gotabulate"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/wrangler"
)

func commandVDiff2(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	_ = subFlags.Bool("v2", true, "")

	timeout := subFlags.Duration("filtered_replication_wait_time", 30*time.Second, "Specifies the maximum time to wait, in seconds, for filtered replication to catch up on primary migrations. The migration will be cancelled on a timeout.")
	maxRows := subFlags.Int64("limit", math.MaxInt64, "Max rows to stop comparing after")
	tables := subFlags.String("tables", "", "Only run vdiff for these tables in the workflow")

	sourceCell := subFlags.String("source_cell", "", "The source cell to compare from; default is any available cell")
	targetCell := subFlags.String("target_cell", "", "The target cell to compare with; default is any available cell")
	tabletTypes := subFlags.String("tablet_types", "in_order:replica,primary", "Tablet types for source and target")

	debugQuery := subFlags.Bool("debug_query", false, "Adds a mysql query to the report that can be used for further debugging")
	onlyPks := subFlags.Bool("only_pks", false, "When reporting missing rows, only show primary keys in the report.")
	format := subFlags.String("format", "", "Format of report") //"json" or ""
	maxExtraRowsToCompare := subFlags.Int64("max_extra_rows_to_compare", 1000, "If there are collation differences between the source and target, you can have rows that are identical but simply returned in a different order from MySQL. We will do a second pass to compare the rows for any actual differences in this case and this flag allows you to control the resources used for this operation.")

	resumable := subFlags.Bool("resumable", false, "Should this vdiff retry in case of recoverable errors, not yet implemented")
	checksum := subFlags.Bool("checksum", false, "Use row-level checksums to compare, not yet implemented")
	samplePct := subFlags.Int64("sample_pct", 100, "How many rows to sample, not yet implemented")

	if err := subFlags.Parse(args); err != nil {
		return err
	}
	var cmd vdiff.VDiffAction
	var subCommand string

	usage := fmt.Errorf("usage: vdiff --v2 <keyspace>.<workflow> command [sub_command|vdiff_uuid]")
	switch subFlags.NArg() {
	case 1: // for backward compatibility with vdiff1
		cmd = vdiff.CreateAction
	case 2:
		cmd = vdiff.VDiffAction(strings.ToLower(subFlags.Arg(1)))
		if cmd != vdiff.CreateAction {
			return usage
		}
	case 3:
		cmd = vdiff.VDiffAction(strings.ToLower(subFlags.Arg(1)))
		subCommand = strings.ToLower(subFlags.Arg(2))
	default:
		return usage
	}
	if cmd == "" {
		return fmt.Errorf("invalid command %s", subFlags.Arg(1))
	}
	keyspace, workflowName, err := splitKeyspaceWorkflow(subFlags.Arg(0))
	if err != nil {
		return err
	}
	log.Infof("VDiff2 Action is %s, args %s", cmd, strings.Join(args, " "))

	if *maxRows <= 0 {
		return fmt.Errorf("maximum number of rows to compare needs to be greater than 0")
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

	var uuid string
	switch cmd {
	case vdiff.CreateAction:
		uuid, err = schema.CreateOnlineDDLUUID()
		if err != nil {
			return err
		}
	case vdiff.ShowAction:
		switch subCommand {
		case "all":
		default:
			if !schema.IsOnlineDDLUUID(subCommand) {
				return fmt.Errorf("can only show specific migration, provide valid uuid")
			}
			uuid = subCommand
		}

	default:
		return fmt.Errorf("invalid command %s", cmd)
	}
	type ErrorResponse struct {
		Error string
	}
	output, err := wr.VDiff2(ctx, keyspace, workflowName, cmd, subCommand, uuid, options)
	if err != nil {
		log.Errorf("vdiff2 returning with error: %v", err)
		return err
	}

	switch cmd {
	case vdiff.CreateAction:
		showVDiff2CreateResponse(wr, *format, uuid)
	case vdiff.ShowAction:
		if output == nil {
			return fmt.Errorf("invalid response from show command")
		}
		log.Infof("show action: %+v", output)
		if err := showVDiff2ShowResponse(wr, *format, keyspace, workflowName, subCommand, output); err != nil {
			return err
		}
	default:
		return fmt.Errorf("action %s not valid", cmd)
	}
	return nil
}

//region ****show response

// summary aggregates/selects the current state of the vdiff from all shards

type vdiffTableSummary struct {
	TableName, State string
	RowsCompared     int64
	LastUpdated      string `json:"LastUpdated,omitempty"`
}
type vdiffSummary struct {
	Workflow, Keyspace, State string
	UUID                      string
	HasMismatch               bool
	Shards                    string
	LastUpdated               string                                 `json:"LastUpdated,omitempty"`
	TableSummaryMap           map[string]vdiffTableSummary           `json:"TableSummary,omitempty"`
	Reports                   map[string]map[string]vdiff.DiffReport `json:"Reports,omitempty"`
}

const (
	summaryTextTemplate = `
VDiff Summary for {{.Keyspace}}.{{.Workflow}} ({{.UUID}})
State: {{.State}}
HasMismatch: {{.HasMismatch}}
`
)

type VDiffListing struct {
	UUID, Workflow, Keyspace, Shard, State string
}

func (vdl *VDiffListing) String() string {
	str := ""
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

func showVDiff2ShowResponse(wr *wrangler.Wrangler, format, keyspace, workflowName, subCommand string, output *wrangler.VDiffOutput) error {
	if schema.IsOnlineDDLUUID(subCommand) {
		return showVDiff2ShowSingleSummary(wr, format, keyspace, workflowName, subCommand, output)
	}
	switch subCommand {
	case "all":
		return showVDiff2ShowRecent(wr, format, keyspace, workflowName, subCommand, output)
	}
	return nil
}

func showVDiff2ShowRecent(wr *wrangler.Wrangler, format, keyspace, workflowName, subCommand string, output *wrangler.VDiffOutput) error {
	str := ""
	recent, err := getVDiff2Recent(output)
	if err != nil {
		return err
	}
	log.Infof("recent: %+v", recent)
	log.Flush()
	if format != "json" {
		wr.Logger().Printf(displayListings(recent))
	} else {
		jsonText, err := json.MarshalIndent(recent, "", "\t")
		if err != nil {
			return err
		}
		str = string(jsonText)
	}
	wr.Logger().Printf(str + "\n")
	return nil
}

func getVDiff2Recent(output *wrangler.VDiffOutput) ([]*VDiffListing, error) {
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

func showVDiff2ShowSingleSummary(wr *wrangler.Wrangler, format, keyspace, workflowName, subCommand string, output *wrangler.VDiffOutput) error {
	str := ""
	summary, err := getVDiff2SingleSummary(wr, keyspace, workflowName, subCommand, output)
	if err != nil {
		return err
	}
	if format != "json" {
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
	} else {
		jsonText, err := json.MarshalIndent(summary, "", "\t")
		if err != nil {
			return err
		}
		str = string(jsonText)
	}
	wr.Logger().Printf(str + "\n")
	return nil
}
func getVDiff2SingleSummary(wr *wrangler.Wrangler, keyspace, workflow, uuid string, output *wrangler.VDiffOutput) (*vdiffSummary, error) {
	summary := &vdiffSummary{
		Workflow:    workflow,
		Keyspace:    keyspace,
		UUID:        uuid,
		State:       "",
		HasMismatch: false,
		Shards:      "",
		Reports:     make(map[string]map[string]vdiff.DiffReport),
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
			log.Infof("qr is %+v", qr.Named().Rows)
			for _, row := range qr.Named().Rows {
				log.Infof("table row is %+v", row)
				if first {
					first = false
					summary.State, _ = row.ToString("VDiff Status")
					summary.State = strings.ToLower(summary.State)
					summary.HasMismatch, _ = row.ToBool("Has Mismatch")
				}
				table := row.AsString("Table", "")
				if _, ok := tableSummaryMap[table]; !ok {
					tableSummaryMap[table] = vdiffTableSummary{
						TableName: table,
					}

				}
				ts := tableSummaryMap[table]
				state := strings.ToLower(row.AsString("Table State", ""))
				rowsCompared := row.AsInt64("Compared Rows", 0)
				ts.RowsCompared += rowsCompared

				switch state {
				case "completed":
					if ts.State == "" {
						ts.State = state
					}
				case "error":
					ts.State = state
				default:
					if ts.State != "error" {
						ts.State = state
					}
				}
				diffReport := row.AsString("report", "")
				dr := vdiff.DiffReport{}
				err := json.Unmarshal([]byte(diffReport), &dr)
				if err != nil {
					return nil, err
				}
				if _, ok := reports[table]; !ok {
					reports[table] = make(map[string]vdiff.DiffReport)
				}
				reports[table][shard] = dr
				tableSummaryMap[table] = ts
			}
		}

	}
	summary.Shards = strings.Join(shards, ",")
	summary.TableSummaryMap = tableSummaryMap
	summary.Reports = reports
	if !summary.HasMismatch { // nolint // lines below commented during testing
		//summary.Reports = nil
		//summary.TableSummaryMap = nil
	}
	return summary, nil
}

//endregion

func showVDiff2CreateResponse(wr *wrangler.Wrangler, format string, uuid string) {
	if format != "json" {
		wr.Logger().Printf("VDiff %s scheduled on target shards, use Show to view progress", uuid)
	} else {
		type CreateResponse struct {
			UUID string
		}
		resp := &CreateResponse{UUID: uuid}
		log.Infof("VDiff scheduled on target shards, use Show to view progress")
		jsonText, _ := json.MarshalIndent(resp, "", "\t")
		log.Infof("response is %+v, %s", resp, jsonText)
		wr.Logger().Printf(string(jsonText))
	}
}
