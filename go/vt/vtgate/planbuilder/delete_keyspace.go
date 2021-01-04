package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildDeleteKeyspacePlan(keyspaceName string, ifExists bool) engine.Primitive {
	return &engine.DeleteKeyspace{
		Keyspace:    keyspaceName,
		IfExists: ifExists,
	}
}
