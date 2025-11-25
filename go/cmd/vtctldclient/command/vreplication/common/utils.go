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

package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vtctl/workflow"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

var (
	client     vtctldclient.VtctldClient
	commandCtx context.Context
	// The generic default for most commands.
	tabletTypesDefault = []topodatapb.TabletType{
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_PRIMARY,
	}
	onDDLDefault             = binlogdatapb.OnDDLAction_IGNORE.String()
	MaxReplicationLagDefault = 30 * time.Second

	BaseOptions = struct {
		Workflow       string
		TargetKeyspace string
		Format         string
	}{}

	CreateOptions = struct {
		Cells                        []string
		AllCells                     bool
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		OnDDL                        string
		DeferSecondaryKeys           bool
		AutoStart                    bool
		StopAfterCopy                bool
		MySQLServerVersion           string
		TruncateUILen                int
		TruncateErrLen               int
		ReferenceTables              []string
		ConfigOverrides              []string
	}{}
)

var commandHandlers = make(map[string]func(cmd *cobra.Command))

func RegisterCommandHandler(command string, handler func(cmd *cobra.Command)) {
	commandHandlers[command] = handler
}

func RegisterCommands(root *cobra.Command) {
	for _, handler := range commandHandlers {
		handler(root)
	}
}

type SubCommandsOpts struct {
	SubCommand string
	Workflow   string // Used to specify an example workflow name for the Examples section of the help output.
}

func SetClient(c vtctldclient.VtctldClient) {
	client = c
}

func GetClient() vtctldclient.VtctldClient {
	return client
}

func SetCommandCtx(ctx context.Context) {
	commandCtx = ctx
}

func GetCommandCtx() context.Context {
	return commandCtx
}

func ParseCells(cmd *cobra.Command) error {
	cf := cmd.Flags().Lookup("cells")
	af := cmd.Flags().Lookup("all-cells")
	if cf != nil && cf.Changed && af != nil && af.Changed {
		return errors.New("cannot specify both --cells and --all-cells")
	}
	if cf.Changed { // Validate the provided value(s)
		for i, cell := range CreateOptions.Cells { // Which only means trimming whitespace
			CreateOptions.Cells[i] = strings.TrimSpace(cell)
		}
	}
	if CreateOptions.AllCells { // Use all current cells
		ctx, cancel := context.WithTimeout(commandCtx, topo.RemoteOperationTimeout)
		defer cancel()
		resp, err := client.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
		if err != nil {
			return fmt.Errorf("failed to get current cells: %v", err)
		}
		CreateOptions.Cells = make([]string, len(resp.Names))
		copy(CreateOptions.Cells, resp.Names)
	}
	return nil
}

func ParseTabletTypes(cmd *cobra.Command) error {
	ttf := cmd.Flags().Lookup("tablet-types")
	if ttf == nil {
		return errors.New("no tablet-types flag found")
	}
	if !ttf.Changed {
		CreateOptions.TabletTypes = tabletTypesDefault
	} else if strings.TrimSpace(ttf.Value.String()) == "" {
		return errors.New("invalid tablet-types value, at least one valid tablet type must be specified")
	}
	return nil
}

func ParseTableMaterializeSettings(tableSettings string, parser *sqlparser.Parser) ([]*vtctldatapb.TableMaterializeSettings, error) {
	tableMaterializeSettings := make([]*vtctldatapb.TableMaterializeSettings, 0)
	err := json.Unmarshal([]byte(tableSettings), &tableMaterializeSettings)
	if err != nil {
		return tableMaterializeSettings, errors.New("table-settings is not valid JSON")
	}
	if len(tableMaterializeSettings) == 0 {
		return tableMaterializeSettings, errors.New("empty table-settings")
	}

	// Validate the provided queries.
	seenSourceTables := make(map[string]bool)
	for _, tms := range tableMaterializeSettings {
		if tms.TargetTable == "" || tms.SourceExpression == "" {
			return tableMaterializeSettings, errors.New("missing target_table or source_expression")
		}
		// Validate that the query is valid.
		stmt, err := parser.Parse(tms.SourceExpression)
		if err != nil {
			return tableMaterializeSettings, fmt.Errorf("invalid source_expression: %q", tms.SourceExpression)
		}
		// Validate that each source-expression uses a different table.
		// If any of them query the same table the materialize workflow
		// will fail.
		err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case sqlparser.TableName:
				if node.Name.NotEmpty() {
					if seenSourceTables[node.Name.String()] {
						return false, fmt.Errorf("multiple source_expression queries use the same table: %q", node.Name.String())
					}
					seenSourceTables[node.Name.String()] = true
				}
			}
			return true, nil
		}, stmt)
		if err != nil {
			return tableMaterializeSettings, err
		}
	}

	return tableMaterializeSettings, nil
}

func validateOnDDL(cmd *cobra.Command) error {
	if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(CreateOptions.OnDDL)]; !ok {
		return fmt.Errorf("invalid on-ddl value: %s", CreateOptions.OnDDL)
	}
	return nil
}

// ParseConfigOverrides converts a slice of key=value strings into a map of config overrides. The slice is passed
// as a flag to the command, and the key=value pairs are used to override the default vreplication config values.
func ParseConfigOverrides(overrides []string) (map[string]string, error) {
	configOverrides := make(map[string]string, len(overrides))
	defaultConfig, err := vttablet.NewVReplicationConfig(nil)
	if err != nil {
		return nil, err
	}
	for _, kv := range overrides {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("invalid config override format (var=value expected): %s", kv)
		}
		if _, ok := defaultConfig.Map()[key]; !ok {
			return nil, fmt.Errorf("unknown vreplication config flag: %s", key)
		}
		configOverrides[key] = value
	}
	return configOverrides, nil
}

// ValidateShards checks if the provided shard names are valid key ranges.
func ValidateShards(shards []string) error {
	for _, shard := range shards {
		if !key.IsValidKeyRange(shard) {
			return fmt.Errorf("invalid shard: %q", shard)
		}
	}
	return nil
}

func ParseAndValidateCreateOptions(cmd *cobra.Command) error {
	if err := validateOnDDL(cmd); err != nil {
		return err
	}
	if err := ParseCells(cmd); err != nil {
		return err
	}
	if err := ParseTabletTypes(cmd); err != nil {
		return err
	}
	return nil
}

func GetOutputFormat(cmd *cobra.Command) (string, error) {
	format := strings.ToLower(strings.TrimSpace(BaseOptions.Format))
	switch format {
	case "text", "json":
		return format, nil
	default:
		return "", fmt.Errorf("invalid output format, got %s", BaseOptions.Format)
	}
}

func GetTabletSelectionPreference(cmd *cobra.Command) tabletmanagerdatapb.TabletSelectionPreference {
	tsp := tabletmanagerdatapb.TabletSelectionPreference_ANY
	if CreateOptions.TabletTypesInPreferenceOrder {
		tsp = tabletmanagerdatapb.TabletSelectionPreference_INORDER
	}
	return tsp
}

func OutputStatusResponse(resp *vtctldatapb.WorkflowStatusResponse, format string) error {
	if format == "json" {
		output, err := cli.MarshalJSONPretty(resp)
		if err != nil {
			return err
		}
		fmt.Println(string(output))
		return nil
	}

	// Plain text formatted output.
	tout := strings.Builder{}
	tout.WriteString(fmt.Sprintf("The following vreplication streams exist for workflow %s.%s:\n\n",
		BaseOptions.TargetKeyspace, BaseOptions.Workflow))
	for _, shardstreams := range resp.ShardStreams {
		for _, shardstream := range shardstreams.Streams {
			tablet := fmt.Sprintf("%s-%d", shardstream.Tablet.Cell, shardstream.Tablet.Uid)
			tout.WriteString(fmt.Sprintf("id=%d on %s/%s: Status: %s. %s.\n",
				shardstream.Id, BaseOptions.TargetKeyspace, tablet, shardstream.Status, shardstream.Info))
		}
	}
	if len(resp.TableCopyState) > 0 {
		tables := maps.Keys(resp.TableCopyState)
		sort.Strings(tables) // Ensure that the output is intuitive and consistent
		tout.WriteString("\nTable Copy Status:")
		for _, table := range tables {
			// Unfortunately we cannot use the prototext marshaler here as it has no option
			// to emit unpopulated fields.
			tcs := resp.TableCopyState[table]
			tout.WriteString("\n\t")
			tout.WriteString(table)
			tout.WriteString(": ")
			tout.WriteString(fmt.Sprintf("RowsCopied:%d, ", tcs.RowsCopied))
			tout.WriteString(fmt.Sprintf("RowsTotal:%d, ", tcs.RowsTotal))
			tout.WriteString(fmt.Sprintf("RowsPercentage:%.2f, ", tcs.RowsPercentage))
			tout.WriteString(fmt.Sprintf("BytesCopied:%d, ", tcs.BytesCopied))
			tout.WriteString(fmt.Sprintf("BytesTotal:%d, ", tcs.BytesTotal))
			tout.WriteString(fmt.Sprintf("BytesPercentage:%.2f", tcs.BytesPercentage))
			// If we're talking to an older server it won't provide this field. We should
			// not show a wrong or confusing value in this case so elide it from the output.
			if tcs.Phase != vtctldatapb.TableCopyPhase_UNKNOWN {
				tout.WriteString(fmt.Sprintf(", Phase:%s", tcs.Phase))
			}
		}
		tout.WriteString("\n")
	}
	tout.WriteString("\nTraffic State: ")
	tout.WriteString(resp.TrafficState)
	fmt.Println(tout.String())
	return nil
}

func AddCommonFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&BaseOptions.TargetKeyspace, "target-keyspace", "", "Target keyspace for this workflow.")
	cmd.MarkPersistentFlagRequired("target-keyspace")
	cmd.PersistentFlags().StringVarP(&BaseOptions.Workflow, "workflow", "w", "", "The workflow you want to perform the command on.")
	cmd.MarkPersistentFlagRequired("workflow")
	cmd.PersistentFlags().StringVar(&BaseOptions.Format, "format", "text", "The format of the output; supported formats are: text,json.")
}

func AddCommonCreateFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVarP(&CreateOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to copy table data from.")
	cmd.Flags().BoolVarP(&CreateOptions.AllCells, "all-cells", "a", false, "Copy table data from any existing cell.")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&CreateOptions.TabletTypes), "tablet-types", "Source tablet types to replicate table data from (e.g. PRIMARY,REPLICA,RDONLY).")
	cmd.Flags().BoolVar(&CreateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	cmd.Flags().StringVar(&CreateOptions.OnDDL, "on-ddl", onDDLDefault, "What to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE.")
	cmd.Flags().BoolVar(&CreateOptions.DeferSecondaryKeys, "defer-secondary-keys", true, "Defer secondary index creation for a table until after it has been copied.")
	cmd.Flags().BoolVar(&CreateOptions.AutoStart, "auto-start", true, "Start the workflow after creating it.")
	cmd.Flags().BoolVar(&CreateOptions.StopAfterCopy, "stop-after-copy", false, "Stop the workflow after it's finished copying the existing rows and before it starts replicating changes.")
	cmd.Flags().StringSliceVar(&CreateOptions.ConfigOverrides, "config-overrides", []string{}, "Specify one or more VReplication config flags to override as a comma-separated list of key=value pairs.")
}

var MirrorTrafficOptions = struct {
	DryRun      bool
	Percent     float32
	TabletTypes []topodatapb.TabletType
}{}

var SwitchTrafficOptions = struct {
	Cells                     []string
	TabletTypes               []topodatapb.TabletType
	Timeout                   time.Duration
	MaxReplicationLagAllowed  time.Duration
	EnableReverseReplication  bool
	DryRun                    bool
	Direction                 workflow.TrafficSwitchDirection
	InitializeTargetSequences bool
	Shards                    []string
	Force                     bool
}{}

func AddCommonSwitchTrafficFlags(cmd *cobra.Command, initializeTargetSequences bool) {
	cmd.Flags().StringSliceVarP(&SwitchTrafficOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to switch traffic in.")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&SwitchTrafficOptions.TabletTypes), "tablet-types", "Tablet types to switch traffic for.")
	cmd.Flags().DurationVar(&SwitchTrafficOptions.Timeout, "timeout", workflow.DefaultTimeout, "Specifies the maximum time to wait, in seconds, for VReplication to catch up on primary tablets. The traffic switch will be cancelled on timeout.")
	cmd.Flags().DurationVar(&SwitchTrafficOptions.MaxReplicationLagAllowed, "max-replication-lag-allowed", MaxReplicationLagDefault, "Allow traffic to be switched only if VReplication lag is below this.")
	cmd.Flags().BoolVar(&SwitchTrafficOptions.EnableReverseReplication, "enable-reverse-replication", true, "Setup replication going back to the original source keyspace to support rolling back the traffic cutover.")
	cmd.Flags().BoolVar(&SwitchTrafficOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred.")
	cmd.Flags().BoolVar(&SwitchTrafficOptions.Force, "force", false, "Force the traffic switch even if some potentially non-critical actions cannot be performed; for example the tablet refresh fails on some tablets in the keyspace. WARNING: this should be used with extreme caution and only in emergency situations!")
	if initializeTargetSequences {
		cmd.Flags().BoolVar(&SwitchTrafficOptions.InitializeTargetSequences, "initialize-target-sequences", false, "When moving tables from an unsharded keyspace to a sharded keyspace, initialize any sequences that are being used on the target when switching writes. If the sequence table is not found, and the sequence table reference was fully qualified OR a value was specified for --global-keyspace, then we will attempt to create the sequence table in that keyspace.")
	}
}

func AddShardSubsetFlag(cmd *cobra.Command, shardsOption *[]string) {
	cmd.Flags().StringSliceVar(shardsOption, "shards", nil, "(Optional) Specifies a comma-separated list of shards to operate on.")
}
