/*
Copyright 2022 The Vitess Authors.

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
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/internal/docgen"
	"vitess.io/vitess/go/cmd/vtctldclient/command"
)

func main() {
	var dir string
	cmd := cobra.Command{
		Use: "docgen [-d <dir>]",
		RunE: func(cmd *cobra.Command, args []string) error {
			return docgen.GenerateMarkdownTree(command.Root, dir)
		},
	}

	cmd.Flags().StringVarP(&dir, "dir", "d", "doc", "output directory to write documentation")
	_ = cmd.Execute()
}
