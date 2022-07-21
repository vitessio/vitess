/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
