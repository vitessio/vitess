package planbuilder

import (
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// queryTimeout returns DirectiveQueryTimeout value if set, otherwise returns 0.
func queryTimeout(d sqlparser.CommentDirectives) int {
	if val, ok := d[sqlparser.DirectiveQueryTimeout]; ok {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return 0
}

// scatterErrorsAsWarnings returns an engine ErrorMode based on the contents of the ScatterErrorsAsWarnings comment directive.
func scatterErrorsAsWarnings(d sqlparser.CommentDirectives) engine.ErrorMode {
	if d.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings) {
		return engine.ErrorModeAsWarningsIfResultElseAggregate
	}

	return engine.ErrorModeAggregate
}
