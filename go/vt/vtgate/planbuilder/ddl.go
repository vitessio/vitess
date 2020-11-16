package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave compeltely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, in sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	stmt := in.(*sqlparser.DDL)
	normalDDLPlan, err := buildDDLPlan(sql, stmt, vschema)
	if err != nil {
		return nil, err
	}
	onlineDDLPlan, err := buildOnlineDDLPlan(sql, stmt, vschema)
	if err != nil {
		return nil, err
	}
	return &engine.DDL{
		NormalDDL: normalDDLPlan,
		OnlineDDL: onlineDDLPlan,
	}, nil
}

func buildDDLPlan(sql string, in sqlparser.Statement, vschema ContextVSchema) (*engine.Send, error) {
	stmt := in.(*sqlparser.DDL)
	// This method call will validate the destination != nil check.
	destination, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}

	if destination == nil {
		destination = key.DestinationAllShards{}
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, //This is original sql query to be passed as the parser can provide partial ddl AST.
		IsDML:             false,
		SingleShardOnly:   false,
	}, nil
}

func buildOnlineDDLPlan(query string, stmt *sqlparser.DDL, vschema ContextVSchema) (*engine.OnlineDDL, error) {
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	strategy := schema.DDLStrategyNormal
	options := ""
	if stmt.OnlineHint != nil {
		strategy = stmt.OnlineHint.Strategy
		options = stmt.OnlineHint.Options
	}
	switch strategy {
	case schema.DDLStrategyGhost, schema.DDLStrategyPTOSC, schema.DDLStrategyNormal: // OK, do nothing
	default:
		return nil, fmt.Errorf("Unknown online DDL strategy: '%v'", stmt.OnlineHint.Strategy)
	}
	return &engine.OnlineDDL{
		Keyspace: keyspace,
		DDL:      stmt,
		SQL:      query,
		Strategy: strategy,
		Options:  options,
	}, nil
}

func buildVSchemaDDLPlan(stmt *sqlparser.DDL, vschema ContextVSchema) (engine.Primitive, error) {
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	return &engine.AlterVSchema{
		Keyspace: keyspace,
		DDL:      stmt,
	}, nil
}
