/*
Copyright 2024 The Vitess Authors.

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

package operators

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestMarshalUnmarshal(t *testing.T) {
	// Test that marshalling and unmarshalling a struct works as expected
	original := VExplainKeys{
		StatementType: "SELECT",
		TableName:     []string{"users", "orders"},
		GroupingColumns: []Column{
			{Table: "orders", Name: "category"},
			{Table: "users", Name: "department"},
		},
		JoinColumns: []ColumnUse{
			{Column: Column{Table: "users", Name: "id"}, Uses: sqlparser.EqualOp},
			{Column: Column{Table: "orders", Name: "user_id"}, Uses: sqlparser.EqualOp},
		},
		FilterColumns: []ColumnUse{
			{Column: Column{Table: "users", Name: "age"}, Uses: sqlparser.GreaterThanOp},
			{Column: Column{Table: "orders", Name: "total"}, Uses: sqlparser.LessThanOp},
			{Column: Column{Table: "orders", Name: "`tricky name not`"}, Uses: sqlparser.InOp},
		},
		SelectColumns: []Column{
			{Table: "users", Name: "name"},
			{Table: "users", Name: "email"},
			{Table: "orders", Name: "amount"},
		},
	}

	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	t.Logf("Marshalled JSON: %s", jsonData)

	var unmarshalled VExplainKeys
	err = json.Unmarshal(jsonData, &unmarshalled)
	require.NoError(t, err)

	if diff := cmp.Diff(original, unmarshalled); diff != "" {
		t.Errorf("Unmarshalled struct does not match original (-want +got):\n%s", diff)
		t.FailNow()
	}
}
