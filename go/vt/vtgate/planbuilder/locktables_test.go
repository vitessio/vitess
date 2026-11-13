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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
)

// TestBuildLockPlanLogsStatement checks that the ignored statement is logged
// as SQL. fmt printed the parsed statement as pointer addresses.
func TestBuildLockPlanLogsStatement(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	stmt, err := sqlparser.NewTestParser().Parse("lock tables t1 read, t2 write")
	require.NoError(t, err)

	_, err = buildLockPlan(stmt, nil, nil)
	require.NoError(t, err)

	record := logs.Record(t, "Lock Tables statement is ignored")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "lock tables t1 read, t2 write", record["query"])
	assert.NotContains(t, logs.String(), "0x")
}
