/*
Copyright 2025 The Vitess Authors.

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

package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	"vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	VSchema = &cobra.Command{
		Use:                   "VSchema --name <vschema_name> [command] [command-flags]",
		Short:                 "Performs CRUD operations on VSchema",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(2),
		Aliases:               []string{"vschema"},
	}
	SetReference = &cobra.Command{
		Use:                   "set-reference --table <table_name> --source <vschema.table>",
		Short:                 "Set up a reference table, which points to a source table in another vschema.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer set-reference --table "corder" --source "commerce.corder"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Set-Reference"},
		Args:                  cobra.NoArgs,
		RunE:                  commandSetReference,
	}
	setReferenceOptions = struct {
		Table  string
		Source string
	}{}
	commonOptions = struct {
		Name string
	}{}
)

func commandSetReference(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaSetReference(commandCtx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: commonOptions.Name,
		TableName:   setReferenceOptions.Table,
		Source:      setReferenceOptions.Source,
	})
	if err != nil {
		return err
	}

	fmt.Printf("reference table '%s' successfully added to vschema '%s'\n", setReferenceOptions.Table, commonOptions.Name)

	return nil
}

func init() {
	// TODO(beingnoble03): Flag usage.
	SetReference.Flags().StringVar(&setReferenceOptions.Table, "table", "", "reference table name")
	SetReference.Flags().StringVar(&setReferenceOptions.Source, "source", "", "source of the reference table")
	VSchema.AddCommand(SetReference)

	VSchema.Flags().StringVar(&commonOptions.Name, "name", "", "vschema name")
	Root.AddCommand(VSchema)
}
