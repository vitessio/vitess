/*
Copyright 2021 The Vitess Authors.

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
	"os"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/json2"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetVSchema makes a GetVSchema gRPC call to a vtctld.
	GetVSchema = &cobra.Command{
		Use:                   "GetVSchema <keyspace>",
		Short:                 "Prints a JSON representation of a keyspace's topo record.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetVSchema,
	}
	// ApplyVSchema makes an ApplyVSchema gRPC call to a vtctld.
	ApplyVSchema = &cobra.Command{
		Use:                   "ApplyVSchema {--vschema=<vschema> || --vschema-file=<vschema file> || --sql=<sql> || --sql-file=<sql file>} [--cells=c1,c2,...] [--skip-rebuild] [--dry-run] <keyspace>",
		Short:                 "Applies the VTGate routing schema to the provided keyspace. Shows the result after application.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandApplyVSchema,
	}
)

var applyVSchemaOptions = struct {
	VSchema     string
	VSchemaFile string
	SQL         string
	SQLFile     string
	DryRun      bool
	SkipRebuild bool
	Cells       []string
}{}

func commandApplyVSchema(cmd *cobra.Command, args []string) error {
	sqlMode := (applyVSchemaOptions.SQL != "") != (applyVSchemaOptions.SQLFile != "")
	jsonMode := (applyVSchemaOptions.VSchema != "") != (applyVSchemaOptions.VSchemaFile != "")

	if sqlMode && jsonMode {
		return fmt.Errorf("only one of the sql, sql-file, vschema, or vschema-file flags may be specified when calling the ApplyVSchema command")
	}

	if !sqlMode && !jsonMode {
		return fmt.Errorf("one of the sql, sql-file, vschema, or vschema-file flags must be specified when calling the ApplyVSchema command")
	}

	req := &vtctldatapb.ApplyVSchemaRequest{
		Keyspace:    cmd.Flags().Arg(0),
		SkipRebuild: applyVSchemaOptions.SkipRebuild,
		Cells:       applyVSchemaOptions.Cells,
		DryRun:      applyVSchemaOptions.DryRun,
	}

	var err error
	if sqlMode {
		if applyVSchemaOptions.SQLFile != "" {
			sqlBytes, err := os.ReadFile(applyVSchemaOptions.SQLFile)
			if err != nil {
				return err
			}
			req.Sql = string(sqlBytes)
		} else {
			req.Sql = applyVSchemaOptions.SQL
		}
	} else { // jsonMode
		var schema []byte
		if applyVSchemaOptions.VSchemaFile != "" {
			schema, err = os.ReadFile(applyVSchemaOptions.VSchemaFile)
			if err != nil {
				return err
			}
		} else {
			schema = []byte(applyVSchemaOptions.VSchema)
		}

		var vs vschemapb.Keyspace
		err = json2.Unmarshal(schema, &vs)
		if err != nil {
			return err
		}
		req.VSchema = &vs
	}

	cli.FinishedParsing(cmd)

	res, err := client.ApplyVSchema(commandCtx, req)
	if err != nil {
		return err
	}
	data, err := cli.MarshalJSON(res.VSchema)
	if err != nil {
		return err
	}
	fmt.Printf("New VSchema object:\n%s\nIf this is not what you expected, check the input data (as JSON parsing will skip unexpected fields).\n", data)
	return nil
}

func commandGetVSchema(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)

	resp, err := client.GetVSchema(commandCtx, &vtctldatapb.GetVSchemaRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.VSchema)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.VSchema, "vschema", "", "VSchema to apply, in JSON form.")
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.VSchemaFile, "vschema-file", "", "Path to a file containing the vschema to apply, in JSON form.")
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.SQL, "sql", "", "A VSchema DDL SQL statement, e.g. `alter table t add vindex hash(id)`.")
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.SQLFile, "sql-file", "", "Path to a file containing a VSchema DDL SQL.")
	ApplyVSchema.Flags().BoolVar(&applyVSchemaOptions.DryRun, "dry-run", false, "If set, do not save the altered vschema, simply echo to console.")
	ApplyVSchema.Flags().BoolVar(&applyVSchemaOptions.SkipRebuild, "skip-rebuild", false, "Skip rebuilding the SrvSchema objects.")
	ApplyVSchema.Flags().StringSliceVar(&applyVSchemaOptions.Cells, "cells", nil, "Limits the rebuild to the specified cells, after application. Ignored if --skip-rebuild is set.")
	Root.AddCommand(ApplyVSchema)

	Root.AddCommand(GetVSchema)
}
