/*
Copyright 2026 The Vitess Authors.

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

package planbuilder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// The alias carries the name MySQL derives from the text as written: the text
// itself up to 255 bytes, cut at a character boundary. A longer alias would be
// cut at 256 bytes with a warning instead, neither of which MySQL does for the
// original query.
func TestImplicitColumnAliasLength(t *testing.T) {
	testcases := []struct {
		name  string
		text  string
		alias string
	}{{
		name:  "short text is the alias",
		text:  "concat( 'a' , 'b' )",
		alias: "concat( 'a' , 'b' )",
	}, {
		name:  "255 bytes pass whole",
		text:  "concat( '" + strings.Repeat("a", 243) + "' )",
		alias: "concat( '" + strings.Repeat("a", 243) + "' )",
	}, {
		name:  "longer text is cut to 255 bytes",
		text:  "concat( '" + strings.Repeat("a", 300) + "' )",
		alias: "concat( '" + strings.Repeat("a", 246),
	}, {
		// concat( ' is 9 bytes; 123 two-byte characters fill the remaining
		// 246, and the 124th would straddle the limit
		name:  "the cut does not split a character",
		text:  "concat( '" + strings.Repeat("é", 300) + "' )",
		alias: "concat( '" + strings.Repeat("é", 123),
	}}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse("select " + tc.text + " from t")
			require.NoError(t, err)
			aliased := addImplicitColumnAliases(stmt).(*sqlparser.Select)
			ae := aliased.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
			assert.Equal(t, tc.alias, ae.As.String())
		})
	}
}
