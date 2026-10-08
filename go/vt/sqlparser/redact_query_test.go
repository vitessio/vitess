/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactSQLStatements(t *testing.T) {
	parser := NewTestParser()
	sql := "select a,b,c from t where x = 1234 and y = 1234 and z = 'apple'"
	redactedSQL, err := parser.RedactSQLQuery(sql)
	require.NoError(t, err)

	require.Equal(t, "select a, b, c from t where x = :x /* INT64 */ and y = :x /* INT64 */ and z = :z /* VARCHAR */", redactedSQL)
}

func TestRedactSQLQueryWithoutComments(t *testing.T) {
	parser := NewTestParser()
	tcases := []struct {
		name string
		sql  string
		want string
	}{{
		name: "no comments",
		sql:  "select a from t where x = 1234",
		want: "select a from t where x = :x /* INT64 */",
	}, {
		name: "statement comment",
		sql:  "select /* customer@example.com */ a from t where x = 1234",
		want: "select a from t where x = :x /* INT64 */",
	}, {
		name: "margin comments",
		sql:  "/* leading@example.com */ select a from t where x = 1234 /* trailing@example.com */",
		want: "select a from t where x = :x /* INT64 */",
	}, {
		name: "double slash comment",
		sql:  "select // customer@example.com\n a from t where x = 1234",
		want: "select a from t where x = :x /* INT64 */",
	}, {
		name: "union",
		sql:  "select /* left@example.com */ a from t union select /* right@example.com */ b from u",
		want: "select a from t union select b from u",
	}, {
		name: "insert",
		sql:  "insert /* secret@example.com */ into t(a) values (1)",
		want: "insert into t(a) values (:redacted1 /* INT64 */)",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			got, err := parser.RedactSQLQueryWithoutComments(tcase.sql)
			require.NoError(t, err)
			require.Equal(t, tcase.want, got)
		})
	}

	// RedactSQLQuery keeps the comments as written.
	got, err := parser.RedactSQLQuery("select /* customer@example.com */ a from t")
	require.NoError(t, err)
	require.Contains(t, got, "customer@example.com")
}
