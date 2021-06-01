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
	"io/ioutil"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// GetVSchema makes a GetVSchema gRPC call to a vtctld.
	GetVSchema = &cobra.Command{
		Use:  "GetVSchema keyspace",
		Args: cobra.ExactArgs(1),
		RunE: commandGetVSchema,
	}
	// ApplyVSchema makes an ApplyVSchema gRPC call to a vtctld.
	ApplyVSchema = &cobra.Command{
		Use:  "ApplyVSchema {-vschema=<vschema> || -vschema_file=<vschema file> || -sql=<sql> || -sql_file=<sql file>} [-cells=c1,c2,...] [-skip_rebuild] [-dry-run] <keyspace>",
		Args: cobra.ExactArgs(1),
		RunE: commandApplyVSchema,
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
	keyspace := cmd.Flags().Arg(0) // validated on the server-side

	var vs *vschemapb.Keyspace
	var err error

	sqlMode := (applyVSchemaOptions.SQL != "") != (applyVSchemaOptions.SQLFile != "")
	jsonMode := (applyVSchemaOptions.VSchema != "") != (applyVSchemaOptions.VSchemaFile != "")

	if sqlMode && jsonMode {
		return fmt.Errorf("only one of the sql, sql_file, vschema, or vschema_file flags may be specified when calling the ApplyVSchema command")
	}

	if !sqlMode && !jsonMode {
		return fmt.Errorf("one of the sql, sql_file, vschema, or vschema_file flags must be specified when calling the ApplyVSchema command")
	}

	if sqlMode {
		if applyVSchemaOptions.SQLFile != "" {
			sqlBytes, err := ioutil.ReadFile(applyVSchemaOptions.SQLFile)
			if err != nil {
				return err
			}
			applyVSchemaOptions.SQL = string(sqlBytes)
		}

		stmt, err := sqlparser.Parse(applyVSchemaOptions.SQL)
		if err != nil {
			return fmt.Errorf("error parsing VSchema statement `%s`: %v", applyVSchemaOptions.SQL, err)
		}
		ddl, ok := stmt.(*sqlparser.AlterVschema)
		if !ok {
			return fmt.Errorf("error parsing VSchema statement `%s`: not a ddl statement", applyVSchemaOptions.SQL)
		}

		resp, err := client.GetVSchema(commandCtx, &vtctldatapb.GetVSchemaRequest{
			Keyspace: keyspace,
		})
		if err != nil && !topo.IsErrType(err, topo.NoNode) {
			return err
		} // otherwise, we keep the empty vschema object from above

		vs, err = topotools.ApplyVSchemaDDL(keyspace, resp.VSchema, ddl)
		if err != nil {
			return err
		}

	} else { // jsonMode
		var schema []byte
		if applyVSchemaOptions.VSchemaFile != "" {
			schema, err = ioutil.ReadFile(applyVSchemaOptions.VSchemaFile)
			if err != nil {
				return err
			}
		} else {
			schema = []byte(applyVSchemaOptions.VSchema)
		}

		vs = &vschemapb.Keyspace{}
		err = json2.Unmarshal(schema, vs)
		if err != nil {
			return err
		}
	}

	cli.FinishedParsing(cmd)

	if applyVSchemaOptions.DryRun {
		data, err := cli.MarshalJSON(vs)
		if err != nil {
			return err
		}
		fmt.Printf("Dry run: Skipping update of VSchema. New VSchema would be: %s\n", data)
		return nil
	}

	res, err := client.ApplyVSchema(commandCtx, &vtctldatapb.ApplyVSchemaRequest{
		Keyspace:    keyspace,
		VSchema:     vs,
		SkipRebuild: applyVSchemaOptions.SkipRebuild,
		Cells:       applyVSchemaOptions.Cells,
	})
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
	ApplyVSchema.Flags().StringVarP(&applyVSchemaOptions.VSchema, "vschema", "vschema", "", "VSchema")
	ApplyVSchema.Flags().StringVarP(&applyVSchemaOptions.VSchemaFile, "vschema_file", "vschema_file", "", "VSchema File")
	ApplyVSchema.Flags().StringVarP(&applyVSchemaOptions.SQL, "sql", "sql", "", "A VSchema DDL SQL statement, e.g. `alter table t add vindex hash(id)`")
	ApplyVSchema.Flags().StringVarP(&applyVSchemaOptions.SQLFile, "sql_file", "sql_file", "", "A file containing VSchema DDL SQL")
	ApplyVSchema.Flags().BoolVarP(&applyVSchemaOptions.DryRun, "dry-run", "dry-run", false, "If set, do not save the altered vschema, simply echo to console.")
	ApplyVSchema.Flags().BoolVarP(&applyVSchemaOptions.SkipRebuild, "skip_rebuild", "skip_rebuild", false, "If set, do no rebuild the SrvSchema objects.")
	ApplyVSchema.Flags().StringSliceVarP(&applyVSchemaOptions.Cells, "cells", "cells", nil, "If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set.")
	Root.AddCommand(ApplyVSchema)

	Root.AddCommand(GetVSchema)
}
