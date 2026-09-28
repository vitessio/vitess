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

// Package cli is the implementation of vtgateclienttest.
// This program has a chain of vtgateservice.VTGateService implementations,
// each one being responsible for one test scenario.
package cli

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd/vtgateclienttest/services"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate"
)

var Main = &cobra.Command{
	Use:     "vtgateclienttest",
	Short:   "vtgateclienttest is a chain of vtgateservice.VTGateService implementations, each one being responsible for one test scenario.",
	Args:    cobra.NoArgs,
	PreRunE: servenv.CobraPreRunE,
	RunE:    run,
}

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()

	servenv.MoveFlagsToCobraCommand(Main)

	acl.RegisterFlags(Main.Flags())
}

func run(cmd *cobra.Command, args []string) error {
	servenv.Init()

	// The implementation chain.
	servenv.OnRun(func() {
		s := services.CreateServices()
		for _, f := range vtgate.RegisterVTGates {
			f(s)
		}
	})

	servenv.RunDefault()
	return nil
}
