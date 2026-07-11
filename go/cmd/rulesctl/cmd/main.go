/*
Copyright 2026 The Vitess Authors.

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

package cmd

import (
	"github.com/spf13/cobra"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
)

var configFile string

func Main() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "rulesctl",
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			_flag.TrickGlog()

			if err := log.Init(cmd.Flags()); err != nil {
				return err
			}

			logutil.PurgeLogs()

			return nil
		},
		Run: func(cmd *cobra.Command, _ []string) { cmd.Help() },
	}

	rootCmd.PersistentFlags().StringVarP(
		&configFile,
		"config-file", "f",
		"rules.json",
		"the config file we will be using to store query rules")
	rootCmd.MarkPersistentFlagFilename("config-file")

	rootCmd.AddCommand(List())
	rootCmd.AddCommand(Remove())
	rootCmd.AddCommand(Add())
	rootCmd.AddCommand(Explain())
	rootCmd.AddCommand(Validate())

	return rootCmd
}
