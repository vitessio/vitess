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
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
)

// simpleResponse is used for JSON responses.
type simpleResponse struct {
	Action string
	Status string
}

var (
	tabletTypesDefault = []topodatapb.TabletType{
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_PRIMARY,
	}

	// base is the base command for all actions related to Lookup Vindexes.
	base = &cobra.Command{
		Use:   "LookupVindex --workflow <workflow> --target-keyspace <keyspace> [command] [<vschema spec>]",
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
		ContinueAfterCopy            bool
		Vindex                       *vschemapb.Keyspace
	}{}

	parseAndValidateCreate = func(cmd *cobra.Command, args []string) error {
		// Validate provided JSON spec.
		jsonSpec := []byte(args[0])
		createOptions.Vindex = &vschemapb.Keyspace{}
		if err := protojson.Unmarshal(jsonSpec, createOptions.Vindex); err != nil {
			return fmt.Errorf("invalid vindex spec: %v", err)
		}
		if !cmd.Flags().Lookup("tablet-types").Changed {
			createOptions.TabletTypes = tabletTypesDefault
		}
		if cmd.Flags().Lookup("cells").Changed {
			for i, cell := range createOptions.Cells {
				createOptions.Cells[i] = strings.TrimSpace(cell)
			}
		}
		return nil
	}

	// create makes a LookupVindexCreate call to a vtctld.
	create = &cobra.Command{
		Use:                   "create",
		Short:                 "Create and backfill a Lookup Vindex in the specified keyspace using the provided Vindex specification.",
		Example:               `vtctldclient --server localhost:15999 LookupVindex --workflow mylookupvdx --target-keyspace customer create '{"sharded":true,"vindexes":{"corder_lookup":{"type":"consistent_lookup_unique","params":{"table":"customer.corder_lookup","from":"sku","to":"keyspace_id"},"owner":"corder"}},"tables":{"corder":{"column_vindexes":[{"column":"sku","name":"corder_lookup"}]}}}'`,
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.ExactArgs(1),
		PreRunE:               parseAndValidateCreate,
		RunE:                  commandCreate,
	}

	// externalize makes a LookupVindexExternalize call to a vtctld.
	externalize = &cobra.Command{
		Use:                   "externalize",
		Short:                 "Externalize the Lookup Vindex. If the Vindex has an owner the workflow will also be deleted.",
		Example:               `vtctldclient --server localhost:15999 LookupVindex --workflow mylookupvdx --target-keyspace customer externalize`,
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Externalize"},
		Args:                  cobra.NoArgs,
		RunE:                  commandExternalize,
	}
)

func commandCreate(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	_, err = common.GetClient().LookupVindexCreate(common.GetCommandCtx(), &vtctldatapb.LookupVindexCreateRequest{
		Workflow:                  common.BaseOptions.Workflow,
		TargetKeyspace:            common.BaseOptions.TargetKeyspace,
		Vindex:                    createOptions.Vindex,
		Cells:                     createOptions.Cells,
		TabletTypes:               createOptions.TabletTypes,
		TabletSelectionPreference: tsp,
		ContinueAfterCopy:         createOptions.ContinueAfterCopy,
	})

	if err != nil {
		return err
	}

	var data []byte
	if format == "json" {
		resp := &simpleResponse{
			Action: "create",
			Status: "completed",
		}
		data, err = cli.MarshalJSONPretty(resp)
		if err != nil {
			return err
		}
	} else {
		data = []byte("LookupVindex wokflow scheduled on target shards, use show to view progress")
	}
	fmt.Println(string(data))

	return nil
}

func commandExternalize(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	resp, err := common.GetClient().LookupVindexExternalize(common.GetCommandCtx(), &vtctldatapb.LookupVindexExternalizeRequest{
		TargetKeyspace: common.BaseOptions.TargetKeyspace,
		Workflow:       common.BaseOptions.Workflow,
	})

	if err != nil {
		return err
	}

	var data []byte
	if format == "json" {
		sr := []simpleResponse{
			{
				Action: "externalize",
				Status: "completed",
			},
		}
		if resp.Deleted {
			sr = append(sr, simpleResponse{
				Action: "workflow_delete",
				Status: "completed",
			})
		}
		data, err = cli.MarshalJSONPretty(sr)
		if err != nil {
			return err
		}
	} else {
		data = []byte("LookupVindex has been externalized")
		if resp.Deleted {
			data = append(data, []byte(" and the workflow has been deleted")...)
		}
	}
	fmt.Println(string(data))

	return nil
}

func registerCommands(root *cobra.Command) {
	common.AddCommonFlags(base)
	root.AddCommand(base)

	create.Flags().StringSliceVar(&createOptions.Cells, "cells", nil, "Cells to look in for source tablets to replicate from.")
	create.Flags().Var((*topoprotopb.TabletTypeListFlag)(&createOptions.TabletTypes), "tablet-types", "Source tablet types to replicate from.")
	create.Flags().BoolVar(&createOptions.ContinueAfterCopy, "continue-after-copy-with-owner", false, "Vindex will continue materialization after copy when an owner is provided")
	create.Flags().BoolVar(&createOptions.TabletTypesInPreferenceOrder, "tablet-types-in-preference-order", true, "When performing source tablet selection, look for candidates in the type order as they are listed in the tablet-types flag.")
	base.AddCommand(create)

	exampleOpts := &common.SubCommandsOpts{
		SubCommand: "LookupVindex",
		Workflow:   "corder_lookup_vdx",
	}
	showCommand := common.GetShowCommand(exampleOpts)
	cancelCommand := common.GetCancelCommand(exampleOpts)
	base.AddCommand(showCommand)
	base.AddCommand(cancelCommand)

	// This will also delete the workflow if the vindex has an owner.
	base.AddCommand(externalize)
}

func init() {
	common.RegisterCommandHandler("LookupVindex", registerCommands)
}
