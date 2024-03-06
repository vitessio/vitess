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

package command

import "github.com/spf13/cobra"

var Cp = &cobra.Command{
	Use: "cp <src> <dst>",
	Example: `zk cp /zk/path .
zk cp ./config /zk/path/config

# Trailing slash indicates directory
zk cp ./config /zk/path/`,
	Args: cobra.MinimumNArgs(2),
	RunE: commandCp,
}

func commandCp(cmd *cobra.Command, args []string) error {
	switch cmd.Flags().NArg() {
	case 2:
		return fs.CopyContext(cmd.Context(), cmd.Flags().Arg(0), cmd.Flags().Arg(1))
	default:
		return fs.MultiCopyContext(cmd.Context(), cmd.Flags().Args())
	}
}

func init() {
	Root.AddCommand(Cp)
}
