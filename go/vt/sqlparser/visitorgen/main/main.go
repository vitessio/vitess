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
	"os"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/sqlparser/visitorgen"
)

var (
	inputFile  = flag.String("input", "", "input file to use")
	outputFile = flag.String("output", "", "output file")
	compare    = flag.Bool("compareOnly", false, "instead of writing to the output file, compare if the generated visitor is still valid for this ast.go")
)

const usage = `Usage of visitorgen:

go run /path/to/visitorgen/main -input=/path/to/ast.go -output=/path/to/rewriter.go
`

func main() {
	defer exit.Recover()
	flag.Usage = printUsage
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		printUsage()
		exit.Return(1)
	}

	err := visitorgen.GenerateRewriter(*inputFile, *outputFile, *compare)
	if err != nil {
		log.Error(err)
		exit.Return(1)
	}
	exit.Return(0)
}

func printUsage() {
	os.Stderr.WriteString(usage)
	os.Stderr.WriteString("\nOptions:\n")
	flag.PrintDefaults()
}
