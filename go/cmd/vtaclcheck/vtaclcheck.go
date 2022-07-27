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

	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
	_flag "vitess.io/vitess/go/internal/flag"
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

func init() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	_flag.SetUsage(flag.CommandLine, _flag.UsageOptions{
		FlagFilter: func(f *flag.Flag) bool {
			for _, name := range vtaclcheckFlags {
				if f.Name == name {
					return true
				}
			}

			return false
		},
	})
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
