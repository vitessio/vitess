package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildUsePlan(stmt *sqlparser.Use, vschema plancontext.VSchema) (engine.Primitive, error) {
	return &engine.UpdateTarget{
		Target: stmt.DBName.String(),
	}, nil
}
