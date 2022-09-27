/*
Copyright 2019 The Vitess Authors.

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

package main

import (
	"flag"
	"fmt"
	"os"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtexplain"

	querypb "vitess.io/vitess/go/vt/proto/query"

	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
	_flag "vitess.io/vitess/go/internal/flag"
)

var (
	sqlFlag            = flag.String("sql", "", "A list of semicolon-delimited SQL commands to analyze")
	sqlFileFlag        = flag.String("sql-file", "", "Identifies the file that contains the SQL commands to analyze")
	schemaFlag         = flag.String("schema", "", "The SQL table schema")
	schemaFileFlag     = flag.String("schema-file", "", "Identifies the file that contains the SQL table schema")
	vschemaFlag        = flag.String("vschema", "", "Identifies the VTGate routing schema")
	vschemaFileFlag    = flag.String("vschema-file", "", "Identifies the VTGate routing schema file")
	ksShardMapFlag     = flag.String("ks-shard-map", "", "JSON map of keyspace name -> shard name -> ShardReference object. The inner map is the same as the output of FindAllShardsInKeyspace")
	ksShardMapFileFlag = flag.String("ks-shard-map-file", "", "File containing json blob of keyspace name -> shard name -> ShardReference object")
	numShards          = flag.Int("shards", 2, "Number of shards per keyspace. Passing --ks-shard-map/--ks-shard-map-file causes this flag to be ignored.")
	executionMode      = flag.String("execution-mode", "multi", "The execution mode to simulate -- must be set to multi, legacy-autocommit, or twopc")
	replicationMode    = flag.String("replication-mode", "ROW", "The replication mode to simulate -- must be set to either ROW or STATEMENT")
	normalize          = flag.Bool("normalize", false, "Whether to enable vtgate normalization")
	outputMode         = flag.String("output-mode", "text", "Output in human-friendly text or json")
	dbName             = flag.String("dbname", "", "Optional database target to override normal routing")
	plannerVersionStr  = flag.String("planner-version", "", "Sets the query planner version to use when generating the explain output. Valid values are V3 and Gen4")

	// vtexplainFlags lists all the flags that should show in usage
	vtexplainFlags = []string{
		"output-mode",
		"planner-version",
		"normalize",
		"shards",
		"replication-mode",
		"schema",
		"schema-file",
		"sql",
		"sql-file",
		"vschema",
		"vschema-file",
		"ks-shard-map",
		"ks-shard-map-file",
		"dbname",
		"queryserver-config-passthrough-dmls",
	}
)

func init() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	_flag.SetUsage(flag.CommandLine, _flag.UsageOptions{
		FlagFilter: func(f *flag.Flag) bool {
			for _, name := range vtexplainFlags {
				if f.Name == name {
					return true
				}
			}

			return false
		},
	})
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

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	servenv.ParseFlags("vtexplain")

	err := parseAndRun()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		exit.Return(1)
	}
}

func parseAndRun() error {
	plannerVersion, _ := plancontext.PlannerNameToVersion(*plannerVersionStr)
	if plannerVersion != querypb.ExecuteOptions_V3 && plannerVersion != querypb.ExecuteOptions_Gen4 {
		return fmt.Errorf("invalid value specified for planner-version of '%s' -- valid values are V3 and Gen4", *plannerVersionStr)
	}

	sql, err := getFileParam(*sqlFlag, *sqlFileFlag, "sql", true)
	if err != nil {
		return err
	}

	schema, err := getFileParam(*schemaFlag, *schemaFileFlag, "schema", true)
	if err != nil {
		return err
	}

	vschema, err := getFileParam(*vschemaFlag, *vschemaFileFlag, "vschema", true)
	if err != nil {
		return err
	}

	ksShardMap, err := getFileParam(*ksShardMapFlag, *ksShardMapFileFlag, "ks-shard-map", false)
	if err != nil {
		return err
	}

	opts := &vtexplain.Options{
		ExecutionMode:   *executionMode,
		PlannerVersion:  plannerVersion,
		ReplicationMode: *replicationMode,
		NumShards:       *numShards,
		Normalize:       *normalize,
		Target:          *dbName,
	}

	log.V(100).Infof("sql %s\n", sql)
	log.V(100).Infof("schema %s\n", schema)
	log.V(100).Infof("vschema %s\n", vschema)

	vte, err := vtexplain.Init(vschema, schema, ksShardMap, opts)
	if err != nil {
		return err
	}
	defer vte.Stop()

	plans, err := vte.Run(sql)
	if err != nil {
		return err
	}

	if *outputMode == "text" {
		fmt.Print(vte.ExplainsAsText(plans))
	} else {
		fmt.Print(vtexplain.ExplainsAsJSON(plans))
	}

	return nil
}
