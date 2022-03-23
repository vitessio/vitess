package cmd

import (
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/rulesctl/common"
)

func List() *cobra.Command {
	var listOptName string
	var listOptNamesOnly bool
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "Display the rules in the config file",
		Args:  cobra.NoArgs,
	}

	listCmd.Flags().StringVarP(
		&listOptName,
		"name", "n",
		"",
		"Display a named rule (optional)")
	listCmd.Flags().BoolVar(
		&listOptNamesOnly,
		"names-only",
		false,
		"Lists only the names of the rules in the config file")

	listCmd.Run = func(cmd *cobra.Command, args []string) {
		rules := common.GetRules(configFile)

		var out any
		if listOptName == "" {
			if listOptNamesOnly {
				out = []string{}
				for _, r := range rules.CopyUnderlying() {
					out = append(out.([]string), r.Name)
				}
			} else {
				out = rules
			}
		} else {
			rule := rules.Find(listOptName)
			if listOptNamesOnly && rule != nil {
				out = listOptName
			} else if listOptNamesOnly {
				out = ""
			} else {
				out = rule
			}
		}

		common.MustPrintJSON(out)
	}

	return listCmd
}
