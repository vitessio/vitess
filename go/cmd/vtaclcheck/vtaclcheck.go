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
	"fmt"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtaclcheck"
)

var aclFile, staticAuthFile string

func init() {
	logger := logutil.NewConsoleLogger()
	servenv.OnParse(func(fs *pflag.FlagSet) {
		fs.StringVar(&aclFile, "acl-file", aclFile, "The path of the JSON ACL file to check")
		fs.StringVar(&staticAuthFile, "static-auth-file", staticAuthFile, "The path of the auth_server_static JSON file to check")

		acl.RegisterFlags(fs)

		fs.SetOutput(logutil.NewLoggerWriter(logger))
	})
}

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	servenv.ParseFlags("vtaclcheck")

	err := run()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		exit.Return(1)
	}
}

func run() error {
	opts := &vtaclcheck.Options{
		ACLFile:        aclFile,
		StaticAuthFile: staticAuthFile,
	}

	if err := vtaclcheck.Init(opts); err != nil {
		return err
	}

	return vtaclcheck.Run()
}
