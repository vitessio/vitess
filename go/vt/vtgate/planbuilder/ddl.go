package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave compeltely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, in sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	ddlStatement := in.(*sqlparser.DDL)
	destination, keyspace, _, err := vschema.TargetDestination(ddlStatement.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	normalDDLPlan, err := buildDDLPlan(sql, ddlStatement, vschema, destination, keyspace)
	if err != nil {
		return nil, err
	}
	onlineDDLPlan, err := buildOnlineDDLPlan(sql, ddlStatement, vschema, keyspace)
	if err != nil {
		return nil, err
	}
	return &engine.DDL{
		Keyspace:  keyspace,
		SQL:       sql,
		DDL:       ddlStatement,
		NormalDDL: normalDDLPlan,
		OnlineDDL: onlineDDLPlan,
	}, nil
}

func buildDDLPlan(sql string, ddlStatement *sqlparser.DDL, vschema ContextVSchema, destination key.Destination, keyspace *vindexes.Keyspace) (*engine.Send, error) {
	// This method call will validate the destination != nil check.

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

func buildOnlineDDLPlan(query string, ddlStatement *sqlparser.DDL, vschema ContextVSchema, keyspace *vindexes.Keyspace) (*engine.OnlineDDL, error) {
	strategy := schema.DDLStrategyNormal
	options := ""
	if ddlStatement.OnlineHint != nil {
		strategy = ddlStatement.OnlineHint.Strategy
		options = ddlStatement.OnlineHint.Options
	}
	switch strategy {
	case schema.DDLStrategyGhost, schema.DDLStrategyPTOSC, schema.DDLStrategyNormal: // OK, do nothing
	default:
		return nil, fmt.Errorf("Unknown online DDL strategy: '%v'", ddlStatement.OnlineHint.Strategy)
	}
	return &engine.OnlineDDL{
		Keyspace: keyspace,
		DDL:      ddlStatement,
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
