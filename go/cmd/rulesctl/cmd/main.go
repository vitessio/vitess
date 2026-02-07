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

			if err := log.Init(); err != nil {
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

	return rootCmd
}
