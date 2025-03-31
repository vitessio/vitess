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
	"os"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	VSchema = &cobra.Command{
		Use:                   "VSchema --name <vschema_name> [command] [command-flags]",
		Short:                 "Performs CRUD operations on VSchema.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(2),
		Aliases:               []string{"vschema"},
	}
	Create = &cobra.Command{
		Use:                   "create",
		Short:                 "Create a new keyspace. Either an initial vschema json is specified in which case it starts off with that spec or it creates an empty vschema which can be built iteratively.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer create --vschema-file "vschema.json" --sharded --draft`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.NoArgs,
		RunE:                  commandCreate,
	}
	Get = &cobra.Command{
		Use:                   "get",
		Short:                 "Retrieve the VSchema for a keyspace. By default, only published (draft: false) VSchemas are returned.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer get --include-drafts`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Get"},
		Args:                  cobra.NoArgs,
		RunE:                  commandGet,
	}
	Update = &cobra.Command{
		Use:                   "update",
		Short:                 "Update the VSchema metadata.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer update --sharded --foreign-key-mode "managed"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Update"},
		Args:                  cobra.NoArgs,
		RunE:                  commandUpdate,
	}
	Publish = &cobra.Command{
		Use:                   "publish",
		Short:                 "Publish a VSchema marking it as non-draft.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer publish`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Set-Reference"},
		Args:                  cobra.NoArgs,
		RunE:                  commandPublish,
	}
	AddVindex = &cobra.Command{
		Use:                   "add-vindex --name <vindex_name> --type <vindex_type> [--params key1=val1,key2=val2]",
		Short:                 "Add a new vindex to the vschema.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer add-vindex --name "hash_vdx" --type "hash"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"AddVindex"},
		Args:                  cobra.NoArgs,
		RunE:                  commandAddVindex,
	}
	RemoveVindex = &cobra.Command{
		Use:                   "remove-vindex --vindex <vindex_name>",
		Short:                 "Remove an existing vindex from the vschema.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer remove-vindex --vindex "hash_vdx"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"RemoveVindex"},
		Args:                  cobra.NoArgs,
		RunE:                  commandRemoveVindex,
	}
	AddLookupVindex = &cobra.Command{
		Use:                   "add-lookup-vindex --name <vindex_name> --type <lookup_vindex_type> --table <vschema.table> --from <column_name> [--owner <owner_table>]",
		Short:                 "Add a lookup vindex to the vschema.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer add-lookup-vindex --name "name_keyspace_idx" --type "lookup" --table "name_keyspace_idx" --from "name" --owner "user"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"AddLookupVindex"},
		Args:                  cobra.NoArgs,
		RunE:                  commandAddLookupVindex,
	}
	AddTables = &cobra.Command{
		Use:                   "add-tables --tables <table1,table2,...> [--all] --primary-vindex <vindex_name> --columns <col1,col2,...>",
		Short:                 "Add one or more tables to the vschema, along with a designated primary vindex and associated columns.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer add-tables --tables "corder,customer" --all --primary-vindex "hash_vdx" --columns "customer_id"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"AddTables"},
		Args:                  cobra.NoArgs,
		RunE:                  commandAddTables,
	}
	RemoveTables = &cobra.Command{
		Use:                   "remove-tables --tables <table1,table2,...>",
		Short:                 "Remove one or more tables from the vschema.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer remove-tables --tables "corder,customer"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"RemoveTables"},
		Args:                  cobra.NoArgs,
		RunE:                  commandRemoveTables,
	}
	SetPrimaryVindex = &cobra.Command{
		Use:                   "set-primary-vindex --tables <table1,table2,...> --primary-vindex <vindex_name> --columns <col1,col2,...>",
		Short:                 "Set or update the primary vindex for one or more tables, specifying the columns associated with the vindex.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer set-primary-vindex --tables "corder,customer" --primary-vindex "hash_vdx" --columns "customer_id"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"SetPrimaryVindex"},
		Args:                  cobra.NoArgs,
		RunE:                  commandSetPrimaryVindex,
	}
	SetSequence = &cobra.Command{
		Use:                   "set-sequence --table <table_name> --column <column_name> --sequence-source <vschema.table>",
		Short:                 "Specify that a table column uses a sequence from an unsharded source.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer set-sequence --table "user" --column "user_id" --sequence-source "unsharded_ks.user_seq"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"SetSequence"},
		Args:                  cobra.NoArgs,
		RunE:                  commandSetSequence,
	}
	SetReference = &cobra.Command{
		Use:                   "set-reference --table <table_name> --source <vschema.table>",
		Short:                 "Set up a reference table, which points to a source table in another vschema.",
		Example:               `vtctldclient --server localhost:15999 vschema --name customer set-reference --table "corder" --source "commerce.corder"`,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"SetReference"},
		Args:                  cobra.NoArgs,
		RunE:                  commandSetReference,
	}
	commonOptions = struct {
		Name string
	}{}
	createOptions = struct {
		Sharded     bool
		Draft       bool
		VSchema     string
		VSchemaFile string
	}{}
	getOptions = struct {
		IncludeDrafts bool
	}{}
	updateOptions = struct {
		Sharded            bool
		ForeignKeyMode     string
		MultiTenant        bool
		TenantIdColumn     string
		TenantIdColumnType string
		Draft              bool
	}{}
	addVindexOptions = struct {
		VindexName string
		VindexType string
		// A key-value pair slice
		Params []string
	}{}
	removeVindexOptions = struct {
		VindexName string
	}{}
	addLookupVindexOptions = struct {
		VindexName       string
		LookupVindexType string
		Table            string
		From             []string
		Owner            string
		IgnoreNulls      bool
	}{}
	addTablesOptions = struct {
		Tables            []string
		PrimaryVindexName string
		Columns           []string
		AddAll            bool
	}{}
	removeTablesOptions = struct {
		Tables []string
	}{}
	setPrimaryVindexOptions = struct {
		Tables            []string
		PrimaryVindexName string
		Columns           []string
	}{}
	setSequenceOptions = struct {
		Table  string
		Source string
		Column string
	}{}
	setReferenceOptions = struct {
		Table  string
		Source string
	}{}
)

func commandCreate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)
	if createOptions.VSchema != "" && createOptions.VSchemaFile != "" {
		return fmt.Errorf("cannot specify both --vschema and --vschema-file")
	}

	if createOptions.VSchemaFile != "" {
		vschema, err := os.ReadFile(createOptions.VSchemaFile)
		if err != nil {
			return err
		}
		createOptions.VSchema = string(vschema)
	}

	_, err := client.VSchemaCreate(commandCtx, &vtctldatapb.VSchemaCreateRequest{
		VSchemaName: commonOptions.Name,
		Sharded:     createOptions.Sharded,
		Draft:       createOptions.Draft,
		VSchemaJson: createOptions.VSchema,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Keyspace '%s' has been successfully created.\n", commonOptions.Name)
	return nil
}

func commandGet(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.VSchemaGet(commandCtx, &vtctldatapb.VSchemaGetRequest{
		VSchemaName:   commonOptions.Name,
		IncludeDrafts: getOptions.IncludeDrafts,
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

func commandUpdate(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	var (
		sharded, draft, multiTenant                        *bool
		foreignKeyMode, tenantIdColumn, tenantIdColumnType *string
	)
	if !cmd.Flags().Changed("sharded") {
		sharded = nil
	} else {
		sharded = &updateOptions.Sharded
	}
	if !cmd.Flags().Changed("draft") {
		draft = nil
	} else {
		draft = &updateOptions.Draft
	}
	if !cmd.Flags().Changed("foreign-key-mode") {
		foreignKeyMode = nil
	} else {
		foreignKeyMode = &updateOptions.ForeignKeyMode
	}
	if !cmd.Flags().Changed("multi-tenant") {
		multiTenant = nil
	} else {
		multiTenant = &updateOptions.MultiTenant
	}
	if !cmd.Flags().Changed("tenant-id-column") {
		tenantIdColumn = nil
	} else {
		tenantIdColumn = &updateOptions.TenantIdColumn
	}
	if !cmd.Flags().Changed("tenant-id-column-type") {
		tenantIdColumnType = nil
	} else {
		tenantIdColumnType = &updateOptions.TenantIdColumnType
	}

	_, err := client.VSchemaUpdate(commandCtx, &vtctldatapb.VSchemaUpdateRequest{
		VSchemaName:        commonOptions.Name,
		Sharded:            sharded,
		Draft:              draft,
		ForeignKeyMode:     foreignKeyMode,
		MultiTenant:        multiTenant,
		TenantIdColumnName: tenantIdColumn,
		TenantIdColumnType: tenantIdColumnType,
	})
	if err != nil {
		return err
	}

	fmt.Printf("VSchema '%s' successfully updated.\n", commonOptions.Name)
	return nil
}

func commandPublish(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaPublish(commandCtx, &vtctldatapb.VSchemaPublishRequest{
		VSchemaName: commonOptions.Name,
	})
	if err != nil {
		return err
	}

	fmt.Printf("VSchema '%s' successfully published.\n", commonOptions.Name)
	return nil
}

func commandAddVindex(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	params := map[string]string{}
	for _, param := range addVindexOptions.Params {
		kv := strings.Split(param, "=")
		if len(kv) != 2 {
			return fmt.Errorf("--params flag should contain params in key-value pair")
		}
		params[kv[0]] = kv[1]
	}

	_, err := client.VSchemaAddVindex(commandCtx, &vtctldatapb.VSchemaAddVindexRequest{
		VSchemaName: commonOptions.Name,
		VindexName:  addVindexOptions.VindexName,
		VindexType:  addVindexOptions.VindexType,
		Params:      params,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Vindex '%s' has been successfully added in VSchema '%s'.\n", addVindexOptions.VindexName, commonOptions.Name)
	return nil
}

func commandRemoveVindex(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaRemoveVindex(commandCtx, &vtctldatapb.VSchemaRemoveVindexRequest{
		VSchemaName: commonOptions.Name,
		VindexName:  removeVindexOptions.VindexName,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Vindex '%s' has been successfully removed from VSchema '%s'.\n", removeVindexOptions.VindexName, commonOptions.Name)
	return nil
}

func commandAddLookupVindex(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaAddLookupVindex(commandCtx, &vtctldatapb.VSchemaAddLookupVindexRequest{
		VSchemaName:      commonOptions.Name,
		VindexName:       addLookupVindexOptions.VindexName,
		LookupVindexType: addLookupVindexOptions.LookupVindexType,
		FromColumns:      addLookupVindexOptions.From,
		TableName:        addLookupVindexOptions.Table,
		Owner:            addLookupVindexOptions.Owner,
		IgnoreNulls:      addLookupVindexOptions.IgnoreNulls,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Lookup Vindex '%s' has been successfully added in VSchema '%s'.\n", addLookupVindexOptions.VindexName, commonOptions.Name)
	return nil
}

func commandAddTables(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaAddTables(commandCtx, &vtctldatapb.VSchemaAddTablesRequest{
		VSchemaName:       commonOptions.Name,
		Tables:            addTablesOptions.Tables,
		PrimaryVindexName: addTablesOptions.PrimaryVindexName,
		Columns:           addTablesOptions.Columns,
		AddAll:            addTablesOptions.AddAll,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Tables %s has been successfully added in VSchema '%s'.\n", strings.Join(addTablesOptions.Tables, ", "), commonOptions.Name)
	return nil
}

func commandRemoveTables(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaRemoveTables(commandCtx, &vtctldatapb.VSchemaRemoveTablesRequest{
		VSchemaName: commonOptions.Name,
		Tables:      removeTablesOptions.Tables,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Tables %s has been successfully removed from VSchema '%s'.\n", strings.Join(removeTablesOptions.Tables, ", "), commonOptions.Name)
	return nil
}

func commandSetPrimaryVindex(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaSetPrimaryVindex(commandCtx, &vtctldatapb.VSchemaSetPrimaryVindexRequest{
		VSchemaName: commonOptions.Name,
		VindexName:  setPrimaryVindexOptions.PrimaryVindexName,
		Tables:      setPrimaryVindexOptions.Tables,
		Columns:     setPrimaryVindexOptions.Columns,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Primary Vindex '%s' has been successfully set up for tables %s in VSchema '%s'.\n",
		setPrimaryVindexOptions.PrimaryVindexName, strings.Join(setPrimaryVindexOptions.Tables, ", "), commonOptions.Name)
	return nil
}

func commandSetSequence(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaSetSequence(commandCtx, &vtctldatapb.VSchemaSetSequenceRequest{
		VSchemaName:    commonOptions.Name,
		TableName:      setSequenceOptions.Table,
		Column:         setSequenceOptions.Column,
		SequenceSource: setSequenceOptions.Source,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Column '%s' in table '%s' has been configured to use sequence from source '%s'. Use get to view VSchema.\n",
		setSequenceOptions.Column, setSequenceOptions.Table, setSequenceOptions.Source)
	return nil
}

func commandSetReference(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.VSchemaSetReference(commandCtx, &vtctldatapb.VSchemaSetReferenceRequest{
		VSchemaName: commonOptions.Name,
		TableName:   setReferenceOptions.Table,
		Source:      setReferenceOptions.Source,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Reference table '%s' has been successfully set up in VSchema '%s'.\n", setReferenceOptions.Table, commonOptions.Name)
	return nil
}

func init() {
	Create.Flags().BoolVar(&createOptions.Sharded, "sharded", false, "Specifies whether the keyspace is sharded.")
	Create.Flags().BoolVar(&createOptions.Draft, "draft", false, "Specifies whether the vschema is a draft.")
	Create.Flags().StringVar(&createOptions.VSchemaFile, "vschema-file", "", "Path to the initial vschema JSON file.")
	Create.Flags().StringVar(&createOptions.VSchema, "vschema", "", "Initial vschema JSON string.")
	VSchema.AddCommand(Create)

	Get.Flags().BoolVar(&getOptions.IncludeDrafts, "include-drafts", false, "Include draft vschemas in the response.")
	VSchema.AddCommand(Get)

	Update.Flags().BoolVar(&updateOptions.Sharded, "sharded", false, "Specifies whether the keyspace is sharded. Use this flag only if updating the sharded status is required.")
	Update.Flags().BoolVar(&updateOptions.Draft, "draft", false, "Specifies whether the vschema is a draft. Use this flag only if updating the draft status is required.")
	Update.Flags().StringVar(&updateOptions.ForeignKeyMode, "foreign-key-mode", "", "Specifies the foreign key mode. Use this flag only if updating the foreign key mode is required.")
	Update.Flags().BoolVar(&updateOptions.MultiTenant, "multi-tenant", false, "Specifies whether the vschema is multi-tenant. Use this flag only if updating the multi-tenant metadata is required. Marking this as false removes the multi-tenant spec from vschema.")
	Update.Flags().StringVar(&updateOptions.TenantIdColumn, "tenant-id-column", "", "Specifies the tenant ID column. Use this flag only if updating the tenant ID column is required.")
	Update.Flags().StringVar(&updateOptions.TenantIdColumnType, "tenant-id-column-type", "", "Specifies the tenant ID column type. Use this flag only if updating the tenant ID column type is required.")
	VSchema.AddCommand(Update)

	VSchema.AddCommand(Publish)

	AddVindex.Flags().StringVar(&addVindexOptions.VindexName, "name", "", "The name of the vindex to add.")
	AddVindex.MarkFlagRequired("name")
	AddVindex.Flags().StringVar(&addVindexOptions.VindexType, "type", "", "The type of the vindex to add.")
	AddVindex.MarkFlagRequired("type")
	AddVindex.Flags().StringSliceVar(&addVindexOptions.Params, "params", nil, "Key-value pairs for vindex parameters.")
	VSchema.AddCommand(AddVindex)

	RemoveVindex.Flags().StringVar(&removeVindexOptions.VindexName, "name", "", "The name of the vindex to remove.")
	RemoveVindex.MarkFlagRequired("name")
	VSchema.AddCommand(RemoveVindex)

	AddLookupVindex.Flags().StringVar(&addLookupVindexOptions.VindexName, "name", "", "The name of the lookup vindex to add.")
	AddLookupVindex.MarkFlagRequired("name")
	AddLookupVindex.Flags().StringVar(&addLookupVindexOptions.LookupVindexType, "type", "", "The type of the lookup vindex to add.")
	AddLookupVindex.MarkFlagRequired("type")
	AddLookupVindex.Flags().StringVar(&addLookupVindexOptions.Table, "table", "", "The table name for the lookup vindex.")
	AddLookupVindex.MarkFlagRequired("table")
	AddLookupVindex.Flags().StringSliceVar(&addLookupVindexOptions.From, "from", nil, "The columns associated with the lookup vindex.")
	AddLookupVindex.MarkFlagRequired("from")
	AddLookupVindex.Flags().StringVar(&addLookupVindexOptions.Owner, "owner", "", "The owner table for the lookup vindex.")
	AddLookupVindex.Flags().BoolVar(&addLookupVindexOptions.IgnoreNulls, "ignore-nulls", false, "Specifies whether to ignore null values.")
	VSchema.AddCommand(AddLookupVindex)

	AddTables.Flags().StringSliceVar(&addTablesOptions.Tables, "tables", nil, "The tables to add to the vschema.")
	AddTables.MarkFlagRequired("tables")
	AddTables.Flags().StringVar(&addTablesOptions.PrimaryVindexName, "primary-vindex", "", "The primary vindex for the tables.")
	AddTables.Flags().StringSliceVar(&addTablesOptions.Columns, "columns", nil, "The columns associated with the primary vindex.")
	AddTables.Flags().BoolVar(&addTablesOptions.AddAll, "all", false, "Add all tables to the vschema.")
	VSchema.AddCommand(AddTables)

	RemoveTables.Flags().StringSliceVar(&removeTablesOptions.Tables, "tables", nil, "The tables to remove from the vschema.")
	RemoveTables.MarkFlagRequired("tables")
	VSchema.AddCommand(RemoveTables)

	SetPrimaryVindex.Flags().StringSliceVar(&setPrimaryVindexOptions.Tables, "tables", nil, "The tables to set the primary vindex for.")
	SetPrimaryVindex.MarkFlagRequired("tables")
	SetPrimaryVindex.Flags().StringVar(&setPrimaryVindexOptions.PrimaryVindexName, "primary-vindex", "", "The primary vindex to set.")
	SetPrimaryVindex.MarkFlagRequired("primary-vindex")
	SetPrimaryVindex.Flags().StringSliceVar(&setPrimaryVindexOptions.Columns, "columns", nil, "The columns associated with the primary vindex.")
	SetPrimaryVindex.MarkFlagRequired("columns")
	VSchema.AddCommand(SetPrimaryVindex)

	SetSequence.Flags().StringVar(&setSequenceOptions.Table, "table", "", "The table name for the sequence.")
	SetSequence.MarkFlagRequired("table")
	SetSequence.Flags().StringVar(&setSequenceOptions.Column, "column", "", "The column name for the sequence.")
	SetSequence.MarkFlagRequired("column")
	SetSequence.Flags().StringVar(&setSequenceOptions.Source, "sequence-source", "", "The source of the sequence in qualified form.")
	SetSequence.MarkFlagRequired("sequence-source")
	VSchema.AddCommand(SetSequence)

	SetReference.Flags().StringVar(&setReferenceOptions.Table, "table", "", "The name of the table that will be set as reference table.")
	SetReference.MarkFlagRequired("table")
	SetReference.Flags().StringVar(&setReferenceOptions.Source, "source", "", "Source of the reference table in qualified form i.e. <keyspace_name>.<table_name>.")
	VSchema.AddCommand(SetReference)

	VSchema.Flags().StringVar(&commonOptions.Name, "name", "", "The name of the vschema/keyspace.")
	Root.AddCommand(VSchema)
}
