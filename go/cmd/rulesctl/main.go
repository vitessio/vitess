package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/spf13/cobra"

	vtfcr "vitess.io/vitess/go/vt/vttablet/customrule/filecustomrule"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var configFile string

func main() {
	rootCmd := &cobra.Command{
		Use:  "rulesctl",
		Args: cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			tmp := os.Args
			os.Args = os.Args[0:1]
			flag.Parse()
			os.Args = tmp
		},
	}
	rootCmd.Run = func(_ *cobra.Command, _ []string) { rootCmd.Help() }

	rootCmd.PersistentFlags().StringVarP(
		&configFile,
		"config-file", "f",
		"rules.json",
		"the config file we will be using to store query rules")
	rootCmd.MarkPersistentFlagFilename("config-file")

	rootCmd.AddCommand(getListCmd())
	rootCmd.AddCommand(getRemoveCmd())
	rootCmd.AddCommand(getAddCmd())
	rootCmd.AddCommand(getExplainCmd())

	if err := rootCmd.Execute(); err != nil {
		log.Printf("%v", err)
	}
}

func getRemoveCmd() *cobra.Command {
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
		rules := getRules()
		if deleted := rules.Delete(removeOptName); deleted == nil {
			fmt.Printf("No rule found: '%v'", removeOptName)
			return
		}

		if removeOptDryRun {
			mustPrintJSON(rules)
		} else {
			mustWriteJSON(rules, configFile)
		}
	}

	return removeCmd
}

func getListCmd() *cobra.Command {
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
		rules := getRules()

		var out interface{}
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
			out = rules.Find(listOptName)
			if listOptNamesOnly && out != nil {
				out = listOptName
			} else if listOptNamesOnly {
				out = ""
			}
		}

		mustPrintJSON(out)
	}

	return listCmd
}

func getRules() *rules.Rules {
	rules, err := vtfcr.ParseRules(configFile)
	if err != nil {
		log.Fatalf("Failure attempting to parse rules: %v", err)
	}
	return rules
}

func mustPrintJSON(obj interface{}) {
	enc, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatalf("Unable to marshal object: %v", err)
	}
	fmt.Printf("%v\n", string(enc))
}

func mustWriteJSON(obj interface{}, path string) {
	enc, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatalf("Unable to marshal object: %v", err)
	}

	err = ioutil.WriteFile(path, enc, 0400)
	if err != nil {
		log.Fatalf("Unable to save new JSON: %v", err)
	}
}
