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
