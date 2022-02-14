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
		Use:  "GetVSchema keyspace",
		Args: cobra.ExactArgs(1),
		RunE: commandGetVSchema,
	}
	// ApplyVSchema makes an ApplyVSchema gRPC call to a vtctld.
	ApplyVSchema = &cobra.Command{
		Use:                   "ApplyVSchema {-vschema=<vschema> || -vschema-file=<vschema file> || -sql=<sql> || -sql-file=<sql file>} [-cells=c1,c2,...] [-skip-rebuild] [-dry-run] <keyspace>",
		Args:                  cobra.ExactArgs(1),
		DisableFlagsInUseLine: true,
		RunE:                  commandApplyVSchema,
		Short:                 "Applies the VTGate routing schema to the provided keyspace. Shows the result after application.",
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

		var vs *vschemapb.Keyspace
		err = json2.Unmarshal(schema, vs)
		if err != nil {
			return err
		}
		req.VSchema = vs
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
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.VSchema, "vschema", "", "VSchema")
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.VSchemaFile, "vschema-file", "", "VSchema File")
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.SQL, "sql", "", "A VSchema DDL SQL statement, e.g. `alter table t add vindex hash(id)`")
	ApplyVSchema.Flags().StringVar(&applyVSchemaOptions.SQLFile, "sql-file", "", "A file containing VSchema DDL SQL")
	ApplyVSchema.Flags().BoolVar(&applyVSchemaOptions.DryRun, "dry-run", false, "If set, do not save the altered vschema, simply echo to console.")
	ApplyVSchema.Flags().BoolVar(&applyVSchemaOptions.SkipRebuild, "skip-rebuild", false, "If set, do no rebuild the SrvSchema objects.")
	ApplyVSchema.Flags().StringSliceVar(&applyVSchemaOptions.Cells, "cells", nil, "If specified, limits the rebuild to the cells, after upload. Ignored if skipRebuild is set.")
	Root.AddCommand(ApplyVSchema)

	Root.AddCommand(GetVSchema)
}
