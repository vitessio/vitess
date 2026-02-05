/*
Copyright 2021 The Vitess Authors.

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

import "github.com/spf13/cobra"

// FinishedParsing transitions a cobra.Command from treating RunE errors as
// usage errors to treating them just as normal runtime errors that should be
// propagated up to the root command's Execute method without also printing the
// subcommand's usage text on stderr. A subcommand should call this function
// from its RunE function when it has finished processing its flags and is
// moving into the pure "business logic" of its entrypoint.
//
// Package vitess.io/vitess/go/cmd/vtctldclient/internal/command has more
// details on why this exists.
func FinishedParsing(cmd *cobra.Command) {
	cmd.SilenceUsage = true
}
