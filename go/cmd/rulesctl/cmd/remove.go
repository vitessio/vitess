package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/rulesctl/common"
)

func Remove() *cobra.Command {
	var removeOptName string
	var removeOptDryRun bool

	removeCmd := &cobra.Command{
		Use:   "remove-rule",
		Short: "Removes a named rule from the config file",
		Args:  cobra.NoArgs,
	}

	removeCmd.Flags().StringVarP(
		&removeOptName,
		"name", "n",
		"",
		"The named rule to remove (required)")
	removeCmd.Flags().BoolVarP(
		&removeOptDryRun,
		"dry-run", "d",
		false,
		"Instead of writing the config file back print the result to stdout")
	removeCmd.MarkFlagRequired("name")

	removeCmd.Run = func(cmd *cobra.Command, args []string) {
		rules := common.GetRules(configFile)
		if deleted := rules.Delete(removeOptName); deleted == nil {
			fmt.Printf("No rule found: '%v'", removeOptName)
			return
		}

		if removeOptDryRun {
			common.MustPrintJSON(rules)
		} else {
			common.MustWriteJSON(rules, configFile)
		}
	}

	return removeCmd
}
