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

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtaclcheck"
)

var (
	aclFileFlag        = flag.String("acl_file", "", "The path of the JSON ACL file to check")
	staticAuthFileFlag = flag.String("static_auth_file", "", "The path of the auth_server_static JSON file to check")

	// vtaclcheckFlags lists all the flags that should show in usage
	vtaclcheckFlags = []string{
		"acl_file",
		"static_auth_file",
	}
)

func usage() {
	fmt.Printf("usage of vtaclcheck:\n")
	for _, name := range vtaclcheckFlags {
		f := flag.Lookup(name)
		if f == nil {
			panic("unknown flag " + name)
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

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	servenv.ParseFlags("vtaclcheck")

	err := parseAndRun()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		exit.Return(1)
	}
}

func parseAndRun() error {
	opts := &vtaclcheck.Options{
		ACLFile:        *aclFileFlag,
		StaticAuthFile: *staticAuthFileFlag,
	}

	log.V(100).Infof("acl_file %s\nstatic_auth_file %s\n", *aclFileFlag, *staticAuthFileFlag)

	if err := vtaclcheck.Init(opts); err != nil {
		return err
	}

	return vtaclcheck.Run()
}
