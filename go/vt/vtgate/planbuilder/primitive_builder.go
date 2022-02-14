package planbuilder

import "vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

// primitiveBuilder is the top level type for building plans.
// It contains the current logicalPlan tree, the symtab and
// the jointab. It can create transient planBuilders due
// to the recursive nature of SQL.
type primitiveBuilder struct {
	vschema plancontext.VSchema
	jt      *jointab
	plan    logicalPlan
	st      *symtab
}

func newPrimitiveBuilder(vschema plancontext.VSchema, jt *jointab) *primitiveBuilder {
	return &primitiveBuilder{
		vschema: vschema,
		jt:      jt,
	}
}
