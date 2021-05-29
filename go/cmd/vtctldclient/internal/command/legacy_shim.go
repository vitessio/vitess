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
	"k8s.io/apimachinery/pkg/util/sets"

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
		Use:   "LegacyVtctlCommand",
		Short: "Invoke a legacy vtctlclient command. Flag parsing is best effort.",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cli.FinishedParsing(cmd)
			return runLegacyCommand(args)
		},
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
	LegacyVtctlCommand.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		// PreRun (and PersistentPreRun) do not run when a Help command is
		// being executed, so we need to duplicate the `--server` flag check
		// here before we attempt to invoke the legacy help command.
		if err := ensureServerArg(); err != nil {
			log.Error(err)
			return
		}

		realArgs := cmd.Flags().Args()

		if len(realArgs) == 0 {
			realArgs = append(realArgs, "help")
		}

		argSet := sets.NewString(realArgs...)
		if !argSet.HasAny("help", "-h", "--help") {
			// Cobra tends to swallow the help flag, so we need to put it back
			// into the arg slice that we pass to runLegacyCommand.
			realArgs = append(realArgs, "-h")
		}

		_ = runLegacyCommand(realArgs)
	})
	Root.AddCommand(LegacyVtctlCommand)
}
