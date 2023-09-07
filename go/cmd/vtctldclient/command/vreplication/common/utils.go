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

	"vitess.io/vitess/go/vt/vtctl/workflow"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"

	"github.com/spf13/cobra"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

	// CommonVROptions are common options for all vreplication commands.
	CommonVROptions = struct {
		VrCommonOptions
	}{}

	CommonVRCreateOptions = struct {
		VrCreateCommonOptions
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

type VrCommonOptions = struct {
	Workflow       string
	TargetKeyspace string
	Format         string
}

type VrCreateCommonOptions = struct {
	Cells                        []string
	TabletTypes                  []topodatapb.TabletType
	TabletTypesInPreferenceOrder bool
	OnDDL                        string
	DeferSecondaryKeys           bool
	AutoStart                    bool
	StopAfterCopy                bool
}

type SubCommandsOpts struct {
	SubCommand string
	Workflow   string
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

func ParseCells(cmd *cobra.Command, opts *VrCreateCommonOptions) {
	if cmd.Flags().Lookup("cells").Changed { // Validate the provided value(s)
		for i, cell := range opts.Cells { // Which only means trimming whitespace
			opts.Cells[i] = strings.TrimSpace(cell)
		}
	}
}

func ParseTabletTypes(cmd *cobra.Command, opts *VrCreateCommonOptions) {
	if !cmd.Flags().Lookup("tablet-types").Changed {
		opts.TabletTypes = tabletTypesDefault
	}
}

func validateOnDDL(cmd *cobra.Command, opts *VrCreateCommonOptions) error {
	if _, ok := binlogdatapb.OnDDLAction_value[strings.ToUpper(opts.OnDDL)]; !ok {
		return fmt.Errorf("invalid on-ddl value: %s", opts.OnDDL)
	}
	return nil
}

func ParseAndValidateCreateOptions(cmd *cobra.Command, opts *VrCreateCommonOptions) error {
	if err := validateOnDDL(cmd, opts); err != nil {
		return err
	}
	ParseCells(cmd, opts)
	ParseTabletTypes(cmd, opts)
	return nil
}

func ParseAndValidateFormat(cmd *cobra.Command, opts *VrCommonOptions) (string, error) {
	format := strings.ToLower(strings.TrimSpace(opts.Format))
	switch format {
	case "text", "json":
		return format, nil
	default:
		return "", fmt.Errorf("invalid output format, got %s", opts.Format)
	}
}

func GetCommonOptions(cmd *cobra.Command, opts *VrCommonOptions) (string, error) {
	format, err := ParseAndValidateFormat(cmd, opts)
	if err != nil {
		return "", err
	}
	return format, nil
}

func GetCreateOptions(cmd *cobra.Command, opts *VrCreateCommonOptions) tabletmanagerdatapb.TabletSelectionPreference {
	tsp := tabletmanagerdatapb.TabletSelectionPreference_ANY
	if opts.TabletTypesInPreferenceOrder {
		tsp = tabletmanagerdatapb.TabletSelectionPreference_INORDER
	}
	return tsp
}

func OutputStatusResponse(resp *vtctldatapb.WorkflowStatusResponse, format string) error {
	var output []byte
	var err error
	if format == "json" {
		output, err = cli.MarshalJSON(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(fmt.Sprintf("The following vreplication streams exist for workflow %s.%s:\n\n",
			CommonVROptions.TargetKeyspace, CommonVROptions.Workflow))
		for _, shardstreams := range resp.ShardStreams {
			for _, shardstream := range shardstreams.Streams {
				tablet := fmt.Sprintf("%s-%d", shardstream.Tablet.Cell, shardstream.Tablet.Uid)
				tout.WriteString(fmt.Sprintf("id=%d on %s/%s: Status: %s. %s.\n",
					shardstream.Id, CommonVROptions.TargetKeyspace, tablet, shardstream.Status, shardstream.Info))
			}
		}
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)
	return nil
}

func AddCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&CommonVROptions.TargetKeyspace, "target-keyspace", "", "Target keyspace for this workflow exists (required)")
	cmd.MarkFlagRequired("target-keyspace")
	cmd.Flags().StringVarP(&CommonVROptions.Workflow, "workflow", "w", "", "The workflow you want to perform the command on (required)")
	cmd.MarkFlagRequired("workflow")
	cmd.Flags().StringVar(&CommonVROptions.Format, "format", "text", "The format of the output; supported formats are: text,json")
}

func AddCommonCreateFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVarP(&CommonVRCreateOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to copy table data from")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&CommonVRCreateOptions.TabletTypes), "tablet-types", "Source tablet types to replicate table data from (e.g. PRIMARY,REPLICA,RDONLY)")
	cmd.Flags().BoolVar(&CommonVRCreateOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag")
	cmd.Flags().StringVar(&CommonVRCreateOptions.OnDDL, "on-ddl", onDDLDefault, "What to do when DDL is encountered in the VReplication stream. Possible values are IGNORE, STOP, EXEC, and EXEC_IGNORE")
	cmd.Flags().BoolVar(&CommonVRCreateOptions.DeferSecondaryKeys, "defer-secondary-keys", false, "Defer secondary index creation for a table until after it has been copied")
	cmd.Flags().BoolVar(&CommonVRCreateOptions.AutoStart, "auto-start", true, "Start the MoveTables workflow after creating it")
	cmd.Flags().BoolVar(&CommonVRCreateOptions.StopAfterCopy, "stop-after-copy", false, "Stop the MoveTables workflow after it's finished copying the existing rows and before it starts replicating changes")
}

var CommonSwitchTrafficOptions = struct {
	Cells                     []string
	TabletTypes               []topodatapb.TabletType
	Timeout                   time.Duration
	MaxReplicationLagAllowed  time.Duration
	EnableReverseReplication  bool
	DryRun                    bool
	Direction                 workflow.TrafficSwitchDirection
	InitializeTargetSequences bool
}{}

func AddCommonSwitchTrafficFlags(cmd *cobra.Command, initializeTargetSequences bool) {
	cmd.Flags().StringSliceVarP(&CommonSwitchTrafficOptions.Cells, "cells", "c", nil, "Cells and/or CellAliases to switch traffic in")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&CommonSwitchTrafficOptions.TabletTypes), "tablet-types", "Tablet types to switch traffic for")
	cmd.Flags().DurationVar(&CommonSwitchTrafficOptions.Timeout, "timeout", TimeoutDefault, "Specifies the maximum time to wait, in seconds, for VReplication to catch up on primary tablets. The traffic switch will be cancelled on timeout.")
	cmd.Flags().DurationVar(&CommonSwitchTrafficOptions.MaxReplicationLagAllowed, "max-replication-lag-allowed", MaxReplicationLagDefault, "Allow traffic to be switched only if VReplication lag is below this")
	cmd.Flags().BoolVar(&CommonSwitchTrafficOptions.EnableReverseReplication, "enable-reverse-replication", true, "Setup replication going back to the original source keyspace to support rolling back the traffic cutover")
	cmd.Flags().BoolVar(&CommonSwitchTrafficOptions.DryRun, "dry-run", false, "Print the actions that would be taken and report any known errors that would have occurred")
	if initializeTargetSequences {
		cmd.Flags().BoolVar(&CommonSwitchTrafficOptions.InitializeTargetSequences, "initialize-target-sequences", false, "When moving tables from an unsharded keyspace to a sharded keyspace, initialize any sequences that are being used on the target when switching writes.")
	}
}
