package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/engine"
)

// Update is the logicalPlan for engine.Filter.
type update struct {
	edml *engine.DML

	// changedVindexValues contains values for updated Vindexes during an update statement.
	changedVindexValues map[string]*engine.VindexValues
}

var _ logicalPlan = (*filter)(nil)

func init() { // TODO: remove - here to get rid of linter warnings
	x := &update{}
	fmt.Println(x)
}
