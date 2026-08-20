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

package materialize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Set runs during command-line parsing, before the command's RunE has had any
// chance to initialize state — it must work on a zero tableSettings, string
// literals in the source expressions included.
func TestTableSettingsSet(t *testing.T) {
	var ts tableSettings
	err := ts.Set(`[{"target_table": "rollup", "source_expression": "select 'total' as rollupname, count(*) as kount from customer group by rollupname"}]`)
	require.NoError(t, err)
	require.Len(t, ts.val, 1)
	assert.Equal(t, "rollup", ts.val[0].TargetTable)

	err = ts.Set(`[{"target_table": "t", "source_expression": "not valid sql"}]`)
	require.ErrorContains(t, err, "invalid source_expression")
}
