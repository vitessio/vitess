/*
Copyright 2017 Google Inc.

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
	"io/ioutil"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtexplain"
)

var (
	sqlFlag         = flag.String("sql", "", "A list of semicolon-delimited SQL commands to analyze")
	sqlFileFlag     = flag.String("sql-file", "", "Identifies the file that contains the SQL commands to analyze")
	schemaFlag      = flag.String("schema", "", "The SQL table schema")
	schemaFileFlag  = flag.String("schema-file", "", "Identifies the file that contains the SQL table schema")
	vschemaFlag     = flag.String("vschema", "", "Identifies the VTGate routing schema")
	vschemaFileFlag = flag.String("vschema-file", "", "Identifies the VTGate routing schema file")
	numShards       = flag.Int("shards", 2, "Number of shards per keyspace")
	replicationMode = flag.String("replication-mode", "ROW", "The replication mode to simulate -- must be set to either ROW or STATEMENT")
	normalize       = flag.Bool("normalize", false, "Whether to enable vtgate normalization")
	outputMode      = flag.String("output-mode", "text", "Output in human-friendly text or json")

	// vtexplainFlags lists all the flags that should show in usage
	vtexplainFlags = []string{
		"output-mode",
		"normalize",
		"shards",
		"replication-mode",
		"schema",
		"schema-file",
		"sql",
		"sql-file",
		"vschema",
		"vschema-file",
	}
)

func usage() {
	fmt.Printf("usage of vtexplain:\n")
	for _, name := range vtexplainFlags {
		f := flag.Lookup(name)
		if f == nil {
			panic("unkown flag " + name)
		}
		flagUsage(f)
	}
}

// Cloned from the source to print out the usage for a given flag
func flagUsage(f *flag.Flag) {
	s := fmt.Sprintf("  -%s", f.Name) // Two spaces before -; see next two comments.
	name, usage := flag.UnquoteUsage(f)
	if len(name) > 0 {
		s += " " + name
	}
	// Boolean flags of one ASCII letter are so common we
	// treat them specially, putting their usage on the same line.
	if len(s) <= 4 { // space, space, '-', 'x'.
		s += "\t"
	} else {
		// Four spaces before the tab triggers good alignment
		// for both 4- and 8-space tab stops.
		s += "\n    \t"
	}
	s += usage
	if name == "string" {
		// put quotes on the value
		s += fmt.Sprintf(" (default %q)", f.DefValue)
	} else {
		s += fmt.Sprintf(" (default %v)", f.DefValue)
	}
	fmt.Printf(s + "\n")
}

func init() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	flag.Usage = usage
}

// getFileParam returns a string containing either flag is not "",
// or the content of the file named flagFile
func getFileParam(flag, flagFile, name string) (string, error) {
	if flag != "" {
		if flagFile != "" {
			return "", fmt.Errorf("action requires only one of %v or %v-file", name, name)
		}
		return flag, nil
	}

	if flagFile == "" {
		return "", fmt.Errorf("action requires one of %v or %v-file", name, name)
	}
	data, err := ioutil.ReadFile(flagFile)
	if err != nil {
		return "", fmt.Errorf("Cannot read file %v: %v", flagFile, err)
	}
	return string(data), nil
}

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	flag.Parse()

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	args := flag.Args()

	if len(args) != 0 {
		flag.Usage()
		exit.Return(1)
	}

	err := parseAndRun()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		exit.Return(1)
	}
}

func parseAndRun() error {
	sql, err := getFileParam(*sqlFlag, *sqlFileFlag, "sql")
	if err != nil {
		return err
	}

	schema, err := getFileParam(*schemaFlag, *schemaFileFlag, "schema")
	if err != nil {
		return err
	}

	vschema, err := getFileParam(*vschemaFlag, *vschemaFileFlag, "vschema")
	if err != nil {
		return err
	}

	opts := &vtexplain.Options{
		ReplicationMode: *replicationMode,
		NumShards:       *numShards,
		Normalize:       *normalize,
	}

	log.V(100).Infof("sql %s\n", sql)
	log.V(100).Infof("schema %s\n", schema)
	log.V(100).Infof("vschema %s\n", vschema)

	err = vtexplain.Init(vschema, schema, opts)
	if err != nil {
		return err
	}

	plans, err := vtexplain.Run(sql)
	if err != nil {
		return err
	}

	if *outputMode == "text" {
		fmt.Print(vtexplain.ExplainsAsText(plans))
	} else {
		fmt.Print(vtexplain.ExplainsAsJSON(plans))
	}

	return nil
}
