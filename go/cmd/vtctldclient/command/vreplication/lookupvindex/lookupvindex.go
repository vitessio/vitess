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

package lookupvindex

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/vt/sqlparser"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	tabletTypesDefault = []topodatapb.TabletType{
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_PRIMARY,
	}

	baseOptions = struct {
		// This is where the vindex will be created.
		Keyspace string
		// This will come from the name of the vindex target table
		// in the provided spec with a static suffix of `_vdx` added.
		Workflow string
		Vindex   *vschemapb.Keyspace
		// This is where the vindex target table and VReplicaiton
		// workflow will be created.
		TargetKeyspace string
	}{}

	// base is the base command for all actions related to Lookup Vindexes.
	base = &cobra.Command{
		Use:   "LookupVindex --keyspace <keyspace> [command] [<vindex spec>]",
		Short: "Perform commands related to creating, backfilling, and externalizing Lookup Vindexes using VReplication workflows.",
		Long: `LookupVindex commands: create, show, externalize, and cancel.
	See the --help output for each command for more details.`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"lookupvindex"},
		Args:                  cobra.NoArgs,
	}

	createOptions = struct {
		Cells                        []string
		TabletTypes                  []topodatapb.TabletType
		TabletTypesInPreferenceOrder bool
		ContinueAfterCopyWithOwner   bool
	}{}

	parseAndValidateFlags = func(cmd *cobra.Command, args []string) error {
		// Validate provided JSON spec.
		jsonSpec := []byte(args[0])
		baseOptions.Vindex = &vschemapb.Keyspace{}
		if err := protojson.Unmarshal(jsonSpec, baseOptions.Vindex); err != nil {
			return fmt.Errorf("invalid vindex specs: %v", err)
		}
		if baseOptions.Vindex == nil || len(baseOptions.Vindex.Vindexes) != 1 {
			return fmt.Errorf("vindex spec must contain exactly one vindex")
		}
		vindexName := maps.Keys(baseOptions.Vindex.Vindexes)[0]
		vindex := maps.Values(baseOptions.Vindex.Vindexes)[0]
		var tableName string
		var err error
		baseOptions.TargetKeyspace, tableName, err = sqlparser.ParseTable(vindex.Params["table"])
		if err != nil || (baseOptions.TargetKeyspace == "" || tableName == "") {
			return fmt.Errorf("invalid vindex table name: %s, it must be in the form <keyspace>.<table>", vindex.Params["table"])
		}
		// Workflow name is the name of the vindex target table with a
		// static suffix of `_vdx` added.
		baseOptions.Workflow = fmt.Sprintf("%s_vdx", vindexName)
		if cmd.Flags() == nil { // No specific command flags to verify.
			return nil
		}
		// Only the create command has these additional flags so we
		// only validate them if they exist for the command and they
		// were changed from their default.
		ttFlag := cmd.Flags().Lookup("tablet-types")
		if ttFlag != nil && ttFlag.Changed {
			createOptions.TabletTypes = tabletTypesDefault
		}
		cFlag := cmd.Flags().Lookup("cells")
		if cFlag != nil && cFlag.Changed {
			for i, cell := range createOptions.Cells {
				createOptions.Cells[i] = strings.TrimSpace(cell)
			}
		}
		return nil
	}

	// cancel makes a WorkflowDelete call to a vtctld.
	cancel = &cobra.Command{
		Use:                   "cancel",
		Short:                 "Canel the VReplication workflow that backfills the lookup vindex.",
		Example:               `vtctldclient --server localhost:15999 LookupVindex --keyspace customer cancel '{"sharded":true,"vindexes":{"corder_lookup":{"type":"consistent_lookup_unique","params":{"table":"customer.corder_lookup","from":"sku","to":"keyspace_id"},"owner":"corder"}},"tables":{"corder":{"column_vindexes":[{"column":"sku","name":"corder_lookup"}]}}}'`,
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Cancel"},
		Args:                  cobra.ExactArgs(1),
		PreRunE:               parseAndValidateFlags,
		RunE:                  commandCancel,
	}

	// create makes a LookupVindexCreate call to a vtctld.
	create = &cobra.Command{
		Use:                   "create",
		Short:                 "Create the Lookup Vindex in the specified keyspace and backfill it with a VReplication workflow using the provided Vindex specification.",
		Example:               `vtctldclient --server localhost:15999 LookupVindex --keyspace customer create '{"sharded":true,"vindexes":{"corder_lookup":{"type":"consistent_lookup_unique","params":{"table":"customer.corder_lookup","from":"sku","to":"keyspace_id"},"owner":"corder"}},"tables":{"corder":{"column_vindexes":[{"column":"sku","name":"corder_lookup"}]}}}'`,
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.ExactArgs(1),
		PreRunE:               parseAndValidateFlags,
		RunE:                  commandCreate,
	}

	// externalize makes a LookupVindexExternalize call to a vtctld.
	externalize = &cobra.Command{
		Use:                   "externalize",
		Short:                 "Externalize the Lookup Vindex. If the Vindex has an owner the VReplication workflow will also be deleted.",
		Example:               `vtctldclient --server localhost:15999 LookupVindex --keyspace customer externalize '{"sharded":true,"vindexes":{"corder_lookup":{"type":"consistent_lookup_unique","params":{"table":"customer.corder_lookup","from":"sku","to":"keyspace_id"},"owner":"corder"}},"tables":{"corder":{"column_vindexes":[{"column":"sku","name":"corder_lookup"}]}}}'`,
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Externalize"},
		Args:                  cobra.ExactArgs(1),
		PreRunE:               parseAndValidateFlags,
		RunE:                  commandExternalize,
	}

	// show makes a GetWorkflows call to a vtctld.
	show = &cobra.Command{
		Use:                   "show",
		Short:                 "Show the status of the VReplication workflow that backfills the lookup vindex.",
		Example:               `vtctldclient --server localhost:15999 LookupVindex --keyspace customer show '{"sharded":true,"vindexes":{"corder_lookup":{"type":"consistent_lookup_unique","params":{"table":"customer.corder_lookup","from":"sku","to":"keyspace_id"},"owner":"corder"}},"tables":{"corder":{"column_vindexes":[{"column":"sku","name":"corder_lookup"}]}}}'`,
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Show"},
		Args:                  cobra.ExactArgs(1),
		PreRunE:               parseAndValidateFlags,
		RunE:                  commandShow,
	}
)

func commandCancel(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowDeleteRequest{
		Keyspace: baseOptions.Keyspace,
		Workflow: baseOptions.Workflow,
	}
	_, err := common.GetClient().WorkflowDelete(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	vindexName := maps.Keys(baseOptions.Vindex.Vindexes)[0]
	output := fmt.Sprintf("LookupVindex %s left in the %s keyspace and the %s VReplication wokflow has been deleted",
		vindexName, baseOptions.Keyspace, baseOptions.Workflow)
	fmt.Println(output)

	return nil
}

func commandCreate(cmd *cobra.Command, args []string) error {
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	_, err := common.GetClient().LookupVindexCreate(common.GetCommandCtx(), &vtctldatapb.LookupVindexCreateRequest{
		Workflow:                   baseOptions.Workflow,
		Keyspace:                   baseOptions.Keyspace,
		Vindex:                     baseOptions.Vindex,
		Cells:                      createOptions.Cells,
		TabletTypes:                createOptions.TabletTypes,
		TabletSelectionPreference:  tsp,
		ContinueAfterCopyWithOwner: createOptions.ContinueAfterCopyWithOwner,
	})

	if err != nil {
		return err
	}

	vindexName := maps.Keys(baseOptions.Vindex.Vindexes)[0]
	output := fmt.Sprintf("LookupVindex %s created in the %s keyspace and the %s VReplication wokflow scheduled on the %s shards, use show to view progress",
		vindexName, baseOptions.Keyspace, baseOptions.Workflow, baseOptions.TargetKeyspace)
	fmt.Println(output)

	return nil
}

func commandExternalize(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := common.GetClient().LookupVindexExternalize(common.GetCommandCtx(), &vtctldatapb.LookupVindexExternalizeRequest{
		Workflow: baseOptions.Workflow,
		Keyspace: baseOptions.Keyspace,
		Vindex:   baseOptions.Vindex,
	})

	if err != nil {
		return err
	}

	output := fmt.Sprintf("LookupVindex %s has been externalized", maps.Keys(baseOptions.Vindex.Vindexes)[0])
	if resp.Deleted {
		output = output + fmt.Sprintf(" and the %s VReplication workflow has been deleted", baseOptions.Workflow)
	}
	fmt.Println(output)

	return nil
}

func commandShow(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetWorkflowsRequest{
		Keyspace: baseOptions.Keyspace,
		Workflow: baseOptions.Workflow,
	}
	resp, err := common.GetClient().GetWorkflows(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSONPretty(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func registerCommands(root *cobra.Command) {
	base.PersistentFlags().StringVar(&baseOptions.Keyspace, "keyspace", "", "The keyspace to create the Lookup Vindex in.")
	base.MarkPersistentFlagRequired("keyspace")
	root.AddCommand(base)

	// This will create the lookup vindex in the specified keyspace
	// and setup a VReplication workflow to backfill it.
	create.Flags().StringSliceVar(&createOptions.Cells, "cells", nil, "Cells to look in for source tablets to replicate from.")
	create.Flags().Var((*topoprotopb.TabletTypeListFlag)(&createOptions.TabletTypes), "tablet-types", "Source tablet types to replicate from.")
	create.Flags().BoolVar(&createOptions.ContinueAfterCopyWithOwner, "continue-after-copy-with-owner", false, "Vindex will continue materialization after copy when an owner is provided")
	create.Flags().BoolVar(&createOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	base.AddCommand(create)

	// This will show the output of GetWorkflows client call
	// for the VReplication workflow used.
	base.AddCommand(show)

	// This will also delete the VReplication workflow if the
	// vindex has an owner.
	base.AddCommand(externalize)

	// The cancel command deletes the VReplication workflow used
	// to backfill the lookup vindex. It ends up making a
	// WorkflowDelete VtctldServer call.
	base.AddCommand(cancel)
}

func init() {
	common.RegisterCommandHandler("LookupVindex", registerCommands)
}
