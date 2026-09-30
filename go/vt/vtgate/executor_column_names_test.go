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

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func fieldNames(fields []*querypb.Field) []string {
	var names []string
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return names
}

// TestColumnNamesOfConstantSelects checks that columns vtgate computes itself
// are named after the query text, as MySQL names them, also when statements
// that spell their select expressions differently are planned one after the
// other.
func TestColumnNamesOfConstantSelects(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	session := &vtgatepb.Session{TargetString: "@primary"}

	qr, err := executorExec(ctx, executor, session, "select 1, 1+1, 'a' 'b', '  c', NuLl", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "1+1", "a", "c", "NULL"}, fieldNames(qr.Fields))

	qr, err = executorExec(ctx, executor, session, "select 2, 2 + 2, 'd' 'e', 'f', null", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"2", "2 + 2", "d", "f", "NULL"}, fieldNames(qr.Fields))
}

// TestColumnNamesFollowTheClientCharset checks that vtgate reads the select
// expressions of a client that set a latin1 charset as latin1, as MySQL does.
func TestColumnNamesFollowTheClientCharset(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	session := &vtgatepb.Session{TargetString: "@primary"}

	qr, err := executorExec(ctx, executor, session, "select 'é', 1 as `😀`", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"é", "?"}, fieldNames(qr.Fields))

	_, err = executorExec(ctx, executor, session, "set names latin1", nil)
	require.NoError(t, err)
	qr, err = executorExec(ctx, executor, session, "select 'é', 1 as `😀`", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"Ã©", "ðŸ˜€"}, fieldNames(qr.Fields))
}

// TestPreparedColumnNamesFollowTheClientCharset checks that sessions with
// different charsets do not share prepared plans, which are cached by the
// statement text and carry the column names.
func TestPreparedColumnNamesFollowTheClientCharset(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	utf8Session := &vtgatepb.Session{TargetString: "@primary"}
	latin1Session := &vtgatepb.Session{TargetString: "@primary", CharacterSetClient: "latin1", CharacterSetConnection: "latin1", CharacterSetResults: "latin1"}

	fields, _, err := executorPrepare(ctx, executor, utf8Session, "select 'é' from dual")
	require.NoError(t, err)
	assert.Equal(t, []string{"é"}, fieldNames(fields))

	fields, _, err = executorPrepare(ctx, executor, latin1Session, "select 'é' from dual")
	require.NoError(t, err)
	assert.Equal(t, []string{"Ã©"}, fieldNames(fields))
}

// TestColumnNamesInRoutedQueries checks that the SQL vtgate sends to the
// shards names the columns the way MySQL names them in the query.
func TestColumnNamesInRoutedQueries(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	session := &vtgatepb.Session{TargetString: "@primary"}

	_, err := executorExec(ctx, executor, session, "select count(*), COUNT(*), id+1 from user where id = 1", nil)
	require.NoError(t, err)
	require.Len(t, sbc1.Queries, 1)
	assert.Equal(t, "select count(*), count(*) as `COUNT(*)`, id + :vtg1 /* INT64 */ as `id+1` from `user` where id = :vtg1 /* INT64 */", sbc1.Queries[0].Sql)
}

func TestColumnNamesOfPreparedStatements(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	session := &vtgatepb.Session{TargetString: "@primary"}

	for range 2 {
		// The second time, the plan comes from the cache.
		fields, paramsCount, err := executorPrepare(ctx, executor, session, "select ?, ? + 1, 'a' 'b' from dual")
		require.NoError(t, err)
		assert.EqualValues(t, 2, paramsCount)
		assert.Equal(t, []string{"?", "? + 1", "a"}, fieldNames(fields))
	}
}
