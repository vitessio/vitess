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

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/querylogignore"
)

// TestExecutorQueryLogIgnore verifies that the executor suppresses query-log
// emission for SQL whose normalized shape matches an operator-configured
// pattern, while still emitting non-matching queries.
func TestExecutorQueryLogIgnore(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	// Configure the ignore-list and reset it after the test.
	parser := executor.env.Parser()
	original := querylogignore.IgnorePatterns.Get()
	querylogignore.IgnorePatterns.Set(querylogignore.NewIgnoreSet("select id from main1", parser))
	t.Cleanup(func() {
		querylogignore.IgnorePatterns.Set(original)
	})

	logChan := executor.queryLogger.Subscribe("TestExecutorQueryLogIgnore")
	defer executor.queryLogger.Unsubscribe(logChan)

	session := &vtgatepb.Session{TargetString: "@primary", Autocommit: true}

	// A query matching the configured shape (different literal still matches
	// the canonical form) must not produce a log entry.
	_, err := executorExec(ctx, executor, session, "select id from main1", nil)
	require.NoError(t, err)
	assert.Nil(t, getQueryLog(logChan), "matching query must be suppressed")

	// A non-matching query must still be logged.
	_, err = executorExec(ctx, executor, session, "select name from user", nil)
	require.NoError(t, err)
	logStats := getQueryLog(logChan)
	require.NotNil(t, logStats, "non-matching query must still be logged")
	assert.Contains(t, logStats.SQL, "name")
}
