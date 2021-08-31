package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
)

func Explain() *cobra.Command {
	explain := &cobra.Command{
		Use:   "explain [concept]",
		Short: "Explains a concept, valid options are: query-plans",
		Args:  cobra.ExactArgs(1),
		Run:   runExplain,
	}
	return explain
}

func runExplain(cmd *cobra.Command, args []string) {
	lookup := map[string]func(){
		"query-plans": helpQueryPlans,
	}

	if fn, ok := lookup[args[0]]; ok {
		fn()
	} else {
		fmt.Printf("I don't know anything about %q, sorry!", args[0])
	}
}

func helpQueryPlans() {
	fmt.Printf(`Query Plans!

A query plan is the type of work the Tablet is about to do. When used in a rule
it can be used to limit the class of queries that a rule can impact. In other
words it will allow you to say "this rule only fails inserts" or "this rule only
fails selects."

The list of valid plan types that can be used follows:
`)
	for i := 0; i < int(planbuilder.NumPlans); i++ {
		fmt.Printf("  - %v\n", planbuilder.PlanType(i).String())
	}
}
