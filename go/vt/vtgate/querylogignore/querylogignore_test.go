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

package querylogignore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func newParser(t *testing.T) *sqlparser.Parser {
	t.Helper()
	p, err := sqlparser.New(sqlparser.Options{})
	require.NoError(t, err)
	return p
}

func TestShouldIgnore_EmptyFastPath(t *testing.T) {
	parser := newParser(t)
	s := NewIgnoreSet("", parser)

	assert.False(t, s.ShouldIgnore("select 1", parser))
	assert.False(t, s.ShouldIgnore("select $$", parser))
	assert.False(t, s.ShouldIgnore("", parser))

	// nil receiver must also be safe and return false.
	var nilSet *IgnoreSet
	assert.False(t, nilSet.ShouldIgnore("select 1", parser))
}

func TestShouldIgnore_NormalizedShapeMatchesLiterals(t *testing.T) {
	parser := newParser(t)
	s := NewIgnoreSet("select :vtg1 from dual", parser)

	assert.True(t, s.ShouldIgnore("select 1 from dual", parser))
	assert.True(t, s.ShouldIgnore("select 9999 from dual", parser))
	assert.True(t, s.ShouldIgnore("SELECT 7 FROM dual", parser))
}

func TestShouldIgnore_SelectDollarVariants(t *testing.T) {
	// The motivating example from the issue. Current sqlparser parses
	// "select $$" successfully (it normalizes to "select $$ from dual"),
	// so all whitespace/case variants land in the same canonical bucket.
	parser := newParser(t)
	s := NewIgnoreSet("select $$", parser)

	assert.True(t, s.ShouldIgnore("select $$", parser))
	assert.True(t, s.ShouldIgnore("  select $$  ", parser))
	assert.True(t, s.ShouldIgnore("SELECT $$", parser))
	assert.True(t, s.ShouldIgnore("select $$ from dual", parser))
}

func TestShouldIgnore_UnparseableRawFallback(t *testing.T) {
	// A truly unparseable input (no SELECT/INSERT prefix) goes through the
	// raw-string fallback path. Verify the configured pattern matches
	// itself, plus whitespace-trimmed and case-insensitive variants.
	parser := newParser(t)
	s := NewIgnoreSet("PING", parser)

	assert.True(t, s.ShouldIgnore("PING", parser))
	assert.True(t, s.ShouldIgnore("  ping  ", parser))
	assert.True(t, s.ShouldIgnore("Ping", parser))
}

func TestShouldIgnore_DistinctShapesDiffer(t *testing.T) {
	parser := newParser(t)
	s := NewIgnoreSet("select $$", parser)

	// A genuinely different shape must not match.
	assert.False(t, s.ShouldIgnore("select $$ from mytable", parser))
	assert.False(t, s.ShouldIgnore("select $$, $$ from dual", parser))
}

func TestShouldIgnore_NonMatchingQueriesLogNormally(t *testing.T) {
	parser := newParser(t)
	s := NewIgnoreSet("select :vtg1", parser)

	assert.False(t, s.ShouldIgnore("insert into t values (1)", parser))
	assert.False(t, s.ShouldIgnore("select id from t where x = 1", parser))
}

func TestNewIgnoreSet_MultiplePatterns(t *testing.T) {
	parser := newParser(t)
	s := NewIgnoreSet("select :vtg1,select $$,select :vtg1 from dual", parser)

	assert.True(t, s.ShouldIgnore("select 1", parser))
	assert.True(t, s.ShouldIgnore("select $$", parser))
	assert.True(t, s.ShouldIgnore("select 42 from dual", parser))
	assert.False(t, s.ShouldIgnore("update t set a = 1", parser))
}

func TestNewIgnoreSet_FromFile(t *testing.T) {
	parser := newParser(t)

	path := filepath.Join(t.TempDir(), "ignore.txt")
	contents := `# health-check pings
select $$

# raw selects of any literal
select :vtg1
`
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))

	s := NewIgnoreSet("@"+path, parser)

	assert.True(t, s.ShouldIgnore("select $$", parser))
	assert.True(t, s.ShouldIgnore("select 99", parser))
	assert.False(t, s.ShouldIgnore("delete from t", parser))
}

func TestNewIgnoreSet_MissingFileWarnsAndContinues(t *testing.T) {
	parser := newParser(t)

	missing := filepath.Join(t.TempDir(), "does-not-exist.txt")
	s := NewIgnoreSet("@"+missing, parser)

	// Constructor must not panic; ShouldIgnore returns false for everything.
	assert.False(t, s.ShouldIgnore("select $$", parser))
	assert.False(t, s.ShouldIgnore("select 1", parser))
}

func TestNewIgnoreSet_TrimsAndSkipsEmptyEntries(t *testing.T) {
	parser := newParser(t)
	s := NewIgnoreSet("  select :vtg1  ,, , select $$ ", parser)

	assert.True(t, s.ShouldIgnore("select 1", parser))
	assert.True(t, s.ShouldIgnore("select $$", parser))
}

func TestIgnoreSet_SourceRoundTrip(t *testing.T) {
	parser := newParser(t)
	raw := "select :vtg1,select $$"
	s := NewIgnoreSet(raw, parser)
	assert.Equal(t, raw, s.Source())

	var nilSet *IgnoreSet
	assert.Equal(t, "", nilSet.Source())
}

func BenchmarkShouldIgnore_EmptyFastPath(b *testing.B) {
	parser, err := sqlparser.New(sqlparser.Options{})
	require.NoError(b, err)
	s := NewIgnoreSet("", parser)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if s.ShouldIgnore("select id from t where x = 42", parser) {
			b.Fatal("expected false on empty set")
		}
	}
}

func TestGetFunc_CachesIgnoreSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "patterns.txt")
	require.NoError(t, os.WriteFile(path, []byte("select :vtg1\n"), 0o644))

	parser := newParser(t)
	rawVal := "@" + path

	// Reset the cache to a known state.
	ignoreSetCache.mu.Lock()
	ignoreSetCache.rawVal = ""
	ignoreSetCache.parsed = nil
	ignoreSetCache.mu.Unlock()

	t.Cleanup(func() {
		ignoreSetCache.mu.Lock()
		ignoreSetCache.rawVal = ""
		ignoreSetCache.parsed = nil
		ignoreSetCache.mu.Unlock()
	})

	// First call: builds the IgnoreSet (reads the file).
	first := cachedIgnoreSet(rawVal)
	require.NotNil(t, first)
	assert.True(t, first.ShouldIgnore("select 1", parser))

	// Second call with the same raw value: must return the exact same pointer.
	second := cachedIgnoreSet(rawVal)
	assert.Same(t, first, second, "repeated Get with unchanged config must return cached *IgnoreSet")

	// Mutate the file on disk — the cache should NOT notice (keyed on raw string).
	require.NoError(t, os.WriteFile(path, []byte("select :vtg1\nselect $$\n"), 0o644))
	third := cachedIgnoreSet(rawVal)
	assert.Same(t, first, third, "file content change without config reload must not bust cache")

	// Change the raw value — cache must invalidate.
	fourth := cachedIgnoreSet("select $$")
	assert.NotSame(t, first, fourth, "different raw value must produce a new *IgnoreSet")
}
