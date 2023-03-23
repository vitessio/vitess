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
	sql := "select a,b,c from t where x = 1234 and y = 1234 and z = 'apple'"
	redactedSQL, err := RedactSQLQuery(sql)
	if err != nil {
		t.Fatalf("redacting sql failed: %v", err)
	}

	require.Equal(t, "select a, b, c from t where x = :x /* INT64 */ and y = :x /* INT64 */ and z = :z /* VARCHAR */", redactedSQL)
}
