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

package cli

import (
	"context"
	"fmt"
	"os"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtexplain"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"github.com/spf13/cobra"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	sqlFlag            string
	sqlFileFlag        string
	schemaFlag         string
	schemaFileFlag     string
	vschemaFlag        string
	vschemaFileFlag    string
	ksShardMapFlag     string
	ksShardMapFileFlag string
	normalize          bool
	dbName             string
	plannerVersionStr  string

	numShards       = 2
	replicationMode = "ROW"
	executionMode   = "multi"
	outputMode      = "text"

	Main = &cobra.Command{
		Use:   "vtexplain",
		Short: "vtexplain is a command line tool which provides information on how Vitess plans to execute a particular query.",
		Long: `vtexplain is a command line tool which provides information on how Vitess plans to execute a particular query.
		
It can be used to validate queries for compatibility with Vitess.

For a user guide that describes how to use the vtexplain tool to explain how Vitess executes a particular SQL statement, see Analyzing a SQL statement.

## Limitations

### The VSchema must use a keyspace name.

VTExplain requires a keyspace name for each keyspace in an input VSchema:
` +
			"```\n" +
			`"keyspace_name": {
    "_comment": "Keyspace definition goes here."
}
` + "```" + `

If no keyspace name is present, VTExplain will return the following error:
` +
			"```\n" +
			`ERROR: initVtgateExecutor: json: cannot unmarshal bool into Go value of type map[string]json.RawMessage
` + "```\n",
		Example: "Explain how Vitess will execute the query `SELECT * FROM users` using the VSchema contained in `vschemas.json` and the database schema `schema.sql`:\n\n" +
			"```\nvtexplain --vschema-file vschema.json --schema-file schema.sql --sql \"SELECT * FROM users\"\n```\n\n" +
			"Explain how the example will execute on 128 shards using Row-based replication:\n\n" +
			"```\nvtexplain -- -shards 128 --vschema-file vschema.json --schema-file schema.sql --replication-mode \"ROW\" --output-mode text --sql \"INSERT INTO users (user_id, name) VALUES(1, 'john')\"\n```\n",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		Version: servenv.AppVersion.String(),
		RunE:    run,
	}
)

func init() {
	servenv.MoveFlagsToCobraCommand(Main)
	Main.Flags().StringVar(&sqlFlag, "sql", sqlFlag, "A list of semicolon-delimited SQL commands to analyze")
	Main.Flags().StringVar(&sqlFileFlag, "sql-file", sqlFileFlag, "Identifies the file that contains the SQL commands to analyze")
	Main.Flags().StringVar(&schemaFlag, "schema", schemaFlag, "The SQL table schema")
	Main.Flags().StringVar(&schemaFileFlag, "schema-file", schemaFileFlag, "Identifies the file that contains the SQL table schema")
	Main.Flags().StringVar(&vschemaFlag, "vschema", vschemaFlag, "Identifies the VTGate routing schema")
	Main.Flags().StringVar(&vschemaFileFlag, "vschema-file", vschemaFileFlag, "Identifies the VTGate routing schema file")
	Main.Flags().StringVar(&ksShardMapFlag, "ks-shard-map", ksShardMapFlag, "JSON map of keyspace name -> shard name -> ShardReference object. The inner map is the same as the output of FindAllShardsInKeyspace")
	Main.Flags().StringVar(&ksShardMapFileFlag, "ks-shard-map-file", ksShardMapFileFlag, "File containing json blob of keyspace name -> shard name -> ShardReference object")
	Main.Flags().StringVar(&replicationMode, "replication-mode", replicationMode, "The replication mode to simulate -- must be set to either ROW or STATEMENT")
	Main.Flags().BoolVar(&normalize, "normalize", normalize, "Whether to enable vtgate normalization")
	Main.Flags().StringVar(&dbName, "dbname", dbName, "Optional database target to override normal routing")
	Main.Flags().StringVar(&plannerVersionStr, "planner-version", plannerVersionStr, "Sets the default planner to use. Valid values are: Gen4, Gen4Greedy, Gen4Left2Right")
	Main.Flags().IntVar(&numShards, "shards", numShards, "Number of shards per keyspace. Passing --ks-shard-map/--ks-shard-map-file causes this flag to be ignored.")
	Main.Flags().StringVar(&executionMode, "execution-mode", executionMode, "The execution mode to simulate -- must be set to multi, legacy-autocommit, or twopc")
	Main.Flags().StringVar(&outputMode, "output-mode", outputMode, "Output in human-friendly text or json")

	acl.RegisterFlags(Main.Flags())
}

// getFileParam returns a string containing either flag is not "",
// or the content of the file named flagFile
func getFileParam(flag, flagFile, name string, required bool) (string, error) {
	if flag != "" {
		if flagFile != "" {
			return "", fmt.Errorf("action requires only one of %v or %v-file", name, name)
		}
		return flag, nil
	}

	if flagFile == "" {
		if required {
			return "", fmt.Errorf("action requires one of %v or %v-file", name, name)
		}

		return "", nil
	}
	data, err := os.ReadFile(flagFile)
	if err != nil {
		return "", fmt.Errorf("cannot read file %v: %v", flagFile, err)
	}
	return string(data), nil
}

func run(cmd *cobra.Command, args []string) error {
	defer logutil.Flush()

	servenv.Init()
	return parseAndRun()
}

func parseAndRun() error {
	plannerVersion, _ := plancontext.PlannerNameToVersion(plannerVersionStr)
	if plannerVersionStr != "" && plannerVersion != querypb.ExecuteOptions_Gen4 {
		return fmt.Errorf("invalid value specified for planner-version of '%s' -- valid value is Gen4 or an empty value to use the default planner", plannerVersionStr)
	}

	sql, err := getFileParam(sqlFlag, sqlFileFlag, "sql", true)
	if err != nil {
		return err
	}

	schema, err := getFileParam(schemaFlag, schemaFileFlag, "schema", true)
	if err != nil {
		return err
	}

	vschema, err := getFileParam(vschemaFlag, vschemaFileFlag, "vschema", true)
	if err != nil {
		return err
	}

	ksShardMap, err := getFileParam(ksShardMapFlag, ksShardMapFileFlag, "ks-shard-map", false)
	if err != nil {
		return err
	}

	opts := &vtexplain.Options{
		ExecutionMode:   executionMode,
		PlannerVersion:  plannerVersion,
		ReplicationMode: replicationMode,
		NumShards:       numShards,
		Normalize:       normalize,
		Target:          dbName,
	}

	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		return err
	}
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, vtexplain.Cell)
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	vte, err := vtexplain.Init(ctx, env, ts, vschema, schema, ksShardMap, opts, srvTopoCounts)
	if err != nil {
		return err
	}
	defer vte.Stop()

	plans, err := vte.Run(sql)
	if err != nil {
		return err
	}

	if outputMode == "text" {
		fmt.Print(vte.ExplainsAsText(plans))
	} else {
		fmt.Print(vtexplain.ExplainsAsJSON(plans))
	}

	return nil
}
