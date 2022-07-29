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

package command

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

var (
	// LegacyVtctlCommand provides a shim to make legacy ExecuteVtctlCommand
	// RPCs. This allows users to use a single binary to make RPCs against both
	// the new and old vtctld gRPC APIs.
	LegacyVtctlCommand = &cobra.Command{
		Use:                   "LegacyVtctlCommand -- <command> [flags ...] [args ...]",
		Short:                 "Invoke a legacy vtctlclient command. Flag parsing is best effort.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cli.FinishedParsing(cmd)
			return runLegacyCommand(args)
		},
		Long: strings.TrimSpace(`
LegacyVtctlCommand uses the legacy vtctl grpc client to make an ExecuteVtctlCommand
rpc to a vtctld.

This command exists to support a smooth transition of any scripts that relied on
vtctlclient during the migration to the new vtctldclient, and will be removed,
following the Vitess project's standard deprecation cycle, once all commands
have been migrated to the new VtctldServer api.

To see the list of available legacy commands, run "LegacyVtctlCommand -- help".
Note that, as with the old client, this requires a running server, as the flag
parsing and help/usage text generation, is done server-side.

Also note that, in order to defer that flag parsing to the server side, you must
use the double-dash ("--") after the LegacyVtctlCommand subcommand string, or
the client-side flag parsing library we are using will attempt to parse those
flags (and fail).
`),
		Example: strings.TrimSpace(`
LegacyVtctlCommand help # displays this help message
LegacyVtctlCommand -- help # displays help for supported legacy vtctl commands

# When using legacy command that take arguments, a double dash must be used
# before the first flag argument, like in the first example. The double dash may
# be used, however, at any point after the "LegacyVtctlCommand" string, as in
# the second example.
LegacyVtctlCommand AddCellInfo -- --server_address "localhost:1234" --root "/vitess/cell1"
LegacyVtctlCommand -- AddCellInfo --server_address "localhost:5678" --root "/vitess/cell1"`),
	}
)

func runLegacyCommand(args []string) error {
	// Duplicated (mostly) from go/cmd/vtctlclient/main.go.
	logger := logutil.NewConsoleLogger()

	ctx, cancel := context.WithTimeout(context.Background(), actionTimeout)
	defer cancel()

	err := vtctlclient.RunCommandAndWait(ctx, server, args, func(e *logutilpb.Event) {
		logutil.LogEvent(logger, e)
	})
	if err != nil {
		if strings.Contains(err.Error(), "flag: help requested") {
			// Help is caught by SetHelpFunc, so we don't want to indicate this as an error.
			return nil
		}

		errStr := strings.Replace(err.Error(), "remote error: ", "", -1)
		fmt.Printf("%s Error: %s\n", flag.Arg(0), errStr)
		log.Error(err)
	}

	return err
}

func init() {
	Root.AddCommand(LegacyVtctlCommand)
}
