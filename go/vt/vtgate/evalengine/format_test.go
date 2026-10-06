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

package evalengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestFormatTextRepertoire(t *testing.T) {
	value := "é\n"
	literal := NewLiteralString([]byte(value), collations.TypedCollation{Collation: collations.CollationUtf8mb4ID})
	encoded := "'' " + sqltypes.EncodeStringSQL(value)
	binds := map[string]*querypb.BindVariable{
		"v": sqltypes.StringBindVariable(value),
		"vals": {
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewVarChar(value))},
		},
	}
	for _, tc := range []struct {
		name string
		node sqlparser.SQLNode
		want string
	}{
		{name: "literal", node: literal, want: encoded},
		{name: "tuple", node: &Literal{inner: &evalTuple{t: []eval{literal.inner}}}, want: "(" + encoded + ")"},
		{name: "bind", node: &BindVariable{Key: "v", Type: sqltypes.VarChar}, want: encoded},
		{name: "list bind", node: &BindVariable{Key: "vals", Type: sqltypes.Tuple}, want: "(" + encoded + ")"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := sqlparser.NewParsedQuery(tc.node).GenerateQuery(binds, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.want, query)
		})
	}
}
