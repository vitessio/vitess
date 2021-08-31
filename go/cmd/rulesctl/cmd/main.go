package cmd

import (
	"flag"
	"os"

	"github.com/spf13/cobra"
)

var configFile string

func Main() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "rulesctl",
		Args: cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			tmp := os.Args
			os.Args = os.Args[0:1]
			flag.Parse()
			os.Args = tmp
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
