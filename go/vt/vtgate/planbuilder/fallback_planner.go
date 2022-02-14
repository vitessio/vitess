package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type fallbackPlanner struct {
	primary, fallback selectPlanner
}

var _ selectPlanner = (*fallbackPlanner)(nil).plan

func (fp *fallbackPlanner) safePrimary(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (res engine.Primitive, err error) {
	defer func() {
		// if the primary planner panics, we want to catch it here so we can fall back
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r) // not using vterror since this will only be used for logging
		}
	}()
	res, err = fp.primary(stmt, reservedVars, vschema)
	return
}

func (fp *fallbackPlanner) plan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	res, err := fp.safePrimary(sqlparser.CloneStatement(stmt), reservedVars, vschema)
	if err != nil {
		return fp.fallback(stmt, reservedVars, vschema)
	}
	return res, nil
}
