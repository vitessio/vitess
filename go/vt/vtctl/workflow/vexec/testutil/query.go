package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// ParsedQueryFromString is a test helper that returns a *sqlparser.ParsedQuery
// from a plain string. It marks the test as a failure if the query cannot be
// parsed.
func ParsedQueryFromString(t *testing.T, query string) *sqlparser.ParsedQuery {
	t.Helper()

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", StatementFromString(t, query))

	return buf.ParsedQuery()
}

// StatementFromString is a test helper that returns a sqlparser.Statement from
// a plain string. It marks the test as a failure if the query cannot be parsed.
func StatementFromString(t *testing.T, query string) sqlparser.Statement {
	t.Helper()

	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err, "could not parse query %v", query)

	return stmt
}
