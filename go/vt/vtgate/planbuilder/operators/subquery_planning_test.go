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

package operators

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestEvaluatesPerShard(t *testing.T) {
	for expr, want := range map[string]bool{
		"a + rand()":                 true,
		"a + microsecond(now(6))":    true,
		"second(current_timestamp)":  true,
		"hour(utc_timestamp())":      true,
		"minute(curtime())":          true,
		"day(curdate())":             true,
		"day(utc_date())":            true,
		"unix_timestamp()":           true,
		"unix_timestamp('2026-1-1')": false,
		"a + 3":                      false,
	} {
		t.Run(expr, func(t *testing.T) {
			e, err := sqlparser.NewTestParser().ParseExpr(expr)
			require.NoError(t, err)
			assert.Equal(t, want, evaluatesPerShard(e))
		})
	}
}
