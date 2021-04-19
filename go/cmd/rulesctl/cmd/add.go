package cmd

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/rulesctl/common"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	vtrules "vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	addOptDryrun      bool
	addOptName        string
	addOptDescription string
	addOptAction      string
	addOptPlans       []string
	addOptTables      []string
	addOptQueryRE     string
	// TODO: other stuff, bind vars etc
)

func runAdd(cmd *cobra.Command, args []string) {
	rulePlans := mkPlanSlice()
	ruleAction := mkAction()

	rule := vtrules.NewQueryRule(addOptDescription, addOptName, ruleAction)
	for _, pt := range rulePlans {
		rule.AddPlanCond(pt)
	}

	for _, t := range addOptTables {
		rule.AddTableCond(t)
	}

	if err := rule.SetQueryCond(addOptQueryRE); err != nil {
		log.Fatalf("Query condition invalid '%v': %v", addOptQueryRE, err)
	}

	var rules *vtrules.Rules
	_, err := os.Stat(configFile)
	if os.IsNotExist(err) {
		rules = vtrules.New()
	} else {
		rules = common.GetRules(configFile)
	}
	existingRule := rules.Find(rule.Name)
	if existingRule != nil {
		log.Fatalf("Rule by name %q already exists", rule.Name)
	}
	rules.Add(rule)

	if addOptDryrun {
		common.MustPrintJSON(rules)
	} else {
		common.MustWriteJSON(rules, configFile)
	}
}

func mkPlanSlice() []planbuilder.PlanType {
	if len(addOptPlans) == 0 {
		return nil
	}

	plans := []planbuilder.PlanType{}
	badPlans := []string{}

	for _, p := range addOptPlans {
		if pbn, ok := planbuilder.PlanByNameIC(p); ok {
			plans = append(plans, pbn)
		} else {
			badPlans = append(badPlans, p)
		}
	}

	if len(badPlans) != 0 {
		log.Fatalf("Unknown PlanType(s) %q", badPlans)
	}

	return plans
}

func mkAction() vtrules.Action {
	switch strings.ToLower(addOptAction) {
	case "fail":
		return vtrules.QRFail
	case "fail_retry":
		return vtrules.QRFailRetry
	case "continue":
		return vtrules.QRContinue
	default:
		log.Fatalf("Unknown action '%v'", addOptAction)
	}

	panic("Nope")
}

func Add() *cobra.Command {
	addCmd := &cobra.Command{
		Use:   "add-rule",
		Short: "Adds a rule to the config file",
		Args:  cobra.NoArgs,
		Run:   runAdd,
	}

	addCmd.Flags().BoolVarP(
		&addOptDryrun,
		"dry-run", "d",
		false,
		"Instead of writing the config file back print the result to stdout")
	addCmd.Flags().StringVarP(
		&addOptName,
		"name", "n",
		"",
		"The name of the rule to add (required)")
	addCmd.Flags().StringVarP(
		&addOptDescription,
		"description", "e",
		"",
		"The purpose/description of the rule being added")
	addCmd.Flags().StringVarP(
		&addOptAction,
		"action", "a",
		"",
		"What action should be taken when this rule is matched {continue, fail, fail-retry} (required)")
	addCmd.Flags().StringSliceVarP(
		&addOptPlans,
		"plan", "p",
		nil,
		"Which query plan types does this rule match; see \"explain query-plans\" for details; may be specified multiple times")
	addCmd.Flags().StringSliceVarP(
		&addOptTables,
		"table", "t",
		nil,
		"Queries will only match if running against these tables; may be specified multiple times")
	addCmd.Flags().StringVarP(
		&addOptQueryRE,
		"query", "q",
		"",
		"A regexp that will be applied to a query in order to determine if it matches")

	for _, f := range []string{"name", "action"} {
		addCmd.MarkFlagRequired(f)
	}

	return addCmd
}
