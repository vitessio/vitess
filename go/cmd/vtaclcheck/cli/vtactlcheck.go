/*
Copyright 2023 The Vitess Authors

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
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtaclcheck"
)

var (
	aclFile        string
	staticAuthFile string

	Main = &cobra.Command{
		Use:     "vtaclcheck",
		Short:   "vtaclcheck checks that the access-control list (ACL) rules in a given file are valid.",
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		PostRun: func(cmd *cobra.Command, args []string) {
			logutil.Flush()
		},
		RunE: run,
	}
)

func run(cmd *cobra.Command, args []string) error {
	servenv.Init()

	opts := &vtaclcheck.Options{
		ACLFile:        aclFile,
		StaticAuthFile: staticAuthFile,
	}

	if err := vtaclcheck.Init(opts); err != nil {
		return err
	}

	return vtaclcheck.Run()
}

func init() {
	servenv.MoveFlagsToCobraCommand(Main)

	Main.Flags().StringVar(&aclFile, "acl-file", aclFile, "The path of the JSON ACL file to check")
	Main.Flags().StringVar(&staticAuthFile, "static-auth-file", staticAuthFile, "The path of the auth_server_static JSON file to check")

	acl.RegisterFlags(Main.Flags())
}
