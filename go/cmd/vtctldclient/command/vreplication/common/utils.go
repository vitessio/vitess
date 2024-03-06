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
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vtctl/workflow"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
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
	TimeoutDefault           = 30 * time.Second

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
		return fmt.Errorf("cannot specify both --cells and --all-cells")
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
		return fmt.Errorf("no tablet-types flag found")
	}
	if !ttf.Changed {
		CreateOptions.TabletTypes = tabletTypesDefault
	} else if strings.TrimSpace(ttf.Value.String()) == "" {
		return fmt.Errorf("invalid tablet-types value, at least one valid tablet type must be specified")
	}
	return nil
}

func validateOnDDL(cmd *cobra.Command) error {
	if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(CreateOptions.OnDDL)]; !ok {
		return fmt.Errorf("invalid on-ddl value: %s", CreateOptions.OnDDL)
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
	var output []byte
	var err error
	if format == "json" {
		output, err = cli.MarshalJSONPretty(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(fmt.Sprintf("The following vreplication streams exist for workflow %s.%s:\n\n",
			BaseOptions.TargetKeyspace, BaseOptions.Workflow))
		for _, shardstreams := range resp.ShardStreams {
			for _, shardstream := range shardstreams.Streams {
				tablet := fmt.Sprintf("%s-%d", shardstream.Tablet.Cell, shardstream.Tablet.Uid)
				tout.WriteString(fmt.Sprintf("id=%d on %s/%s: Status: %s. %s.\n",
					shardstream.Id, BaseOptions.TargetKeyspace, tablet, shardstream.Status, shardstream.Info))
			}
		}
		tout.WriteString("\nTraffic State: ")
		tout.WriteString(resp.TrafficState)
		output = tout.Bytes()
	}
	fmt.Println(string(output))
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
	cmd.Flags().BoolVar(&CreateOptions.DeferSecondaryKeys, "defer-secondary-keys", false, "Defer secondary index creation for a table until after it has been copied.")
	cmd.Flags().BoolVar(&CreateOptions.AutoStart, "auto-start", true, "Start the workflow after creating it.")
	cmd.Flags().BoolVar(&CreateOptions.StopAfterCopy, "stop-after-copy", false, "Stop the workflow after it's finished copying the existing rows and before it starts replicating changes.")
}

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
}{}

func AddCommonSwitchTrafficFlags(cmd *cobra.Command, initializeTargetSequences bool) {
	cmd.Flags().StringSliceVarP(&SwitchTrafficOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to switch traffic in.")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&SwitchTrafficOptions.TabletTypes), "tablet-types", "Tablet types to switch traffic for.")
	cmd.Flags().DurationVar(&SwitchTrafficOptions.Timeout, "timeout", TimeoutDefault, "Specifies the maximum time to wait, in seconds, for VReplication to catch up on primary tablets. The traffic switch will be cancelled on timeout.")
	cmd.Flags().DurationVar(&SwitchTrafficOptions.MaxReplicationLagAllowed, "max-replication-lag-allowed", MaxReplicationLagDefault, "Allow traffic to be switched only if VReplication lag is below this.")
	cmd.Flags().BoolVar(&SwitchTrafficOptions.EnableReverseReplication, "enable-reverse-replication", true, "Setup replication going back to the original source keyspace to support rolling back the traffic cutover.")
	cmd.Flags().BoolVar(&SwitchTrafficOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred.")
	if initializeTargetSequences {
		cmd.Flags().BoolVar(&SwitchTrafficOptions.InitializeTargetSequences, "initialize-target-sequences", false, "When moving tables from an unsharded keyspace to a sharded keyspace, initialize any sequences that are being used on the target when switching writes.")
	}
}

func AddShardSubsetFlag(cmd *cobra.Command, shardsOption *[]string) {
	cmd.Flags().StringSliceVar(shardsOption, "shards", nil, "(Optional) Specifies a comma-separated list of shards to operate on.")
}
