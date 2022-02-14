package planbuilder

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// buildLockPlan plans lock tables statement.
func buildLockPlan(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, _ plancontext.VSchema) (engine.Primitive, error) {
	log.Warningf("Lock Tables statement is ignored: %v", stmt)
	return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0)), nil
}

// buildUnlockPlan plans lock tables statement.
func buildUnlockPlan(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, _ plancontext.VSchema) (engine.Primitive, error) {
	log.Warningf("Unlock Tables statement is ignored: %v", stmt)
	return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0)), nil
}
