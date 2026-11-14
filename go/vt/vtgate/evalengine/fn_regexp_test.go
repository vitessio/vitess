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

package evalengine_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// TestCompiledRegexpConcurrency evaluates one compiled program from several
// goroutines at once, each with its own ExpressionEnv, the way a cached plan
// is run by concurrent queries. The regexp functions compile a constant
// pattern once at compile time; the state of a match must not be shared
// between evaluations with it, or they overwrite each other's input and
// backtracking stack and return the result for another row, or panic.
func TestCompiledRegexpConcurrency(t *testing.T) {
	const (
		goroutines = 8
		iterations = 200
		// runLength is the number of 'a' that each row starts with.
		runLength = 100
	)

	// The long run of 'a' keeps every evaluation inside the matcher for long
	// enough that evaluations overlap, also when the goroutines share one
	// CPU. The rows only differ in the last character, so a match that reads
	// the input of another evaluation gives the result for the other row.
	prefix := strings.Repeat("a", runLength)
	rows := [][]sqltypes.Value{
		{sqltypes.NewVarChar(prefix + "b")},
		{sqltypes.NewVarChar(prefix + "c")},
	}

	testCases := []struct {
		expression string
		// expected has the result for each of the rows.
		expected [2]string
	}{
		{
			expression: `REGEXP_LIKE(column0, 'a+b')`,
			expected:   [2]string{`INT64(1)`, `INT64(0)`},
		},
		{
			expression: `REGEXP_INSTR(column0, 'a+b', 1, 1, 1)`,
			// The position after the match, which is the whole row.
			expected: [2]string{fmt.Sprintf(`INT64(%d)`, runLength+2), `INT64(0)`},
		},
		{
			expression: `REGEXP_SUBSTR(column0, 'a+b')`,
			expected:   [2]string{fmt.Sprintf(`VARCHAR(%q)`, prefix+"b"), `NULL`},
		},
		{
			expression: `REGEXP_REPLACE(column0, 'a+b', 'x')`,
			expected:   [2]string{`TEXT("x")`, fmt.Sprintf(`TEXT(%q)`, prefix+"c")},
		},
	}

	venv := vtenv.NewTestEnv()
	fields := evalengine.FieldResolver(makeFields(rows[0]))

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			expr, err := venv.Parser().ParseExpr(tc.expression)
			require.NoError(t, err)

			translated, err := evalengine.Translate(expr, &evalengine.Config{
				ResolveColumn: fields.Column,
				ResolveType:   fields.Type,
				Collation:     collations.CollationUtf8mb4ID,
				Environment:   venv,
			})
			require.NoError(t, err)
			// The interpreter builds a matcher for every evaluation, so
			// only a compiled program has one that could be shared.
			require.IsType(t, &evalengine.CompiledExpr{}, translated)

			// evaluate turns a panic into an error, so that broken matcher
			// state fails this test instead of ending the test binary.
			evaluate := func(env *evalengine.ExpressionEnv) (res string, err error) {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("evaluation panicked: %v", r)
					}
				}()
				result, err := env.Evaluate(translated)
				if err != nil {
					return "", err
				}
				return result.String(), nil
			}

			// Each goroutine reports the first evaluation that went wrong
			// and how many did.
			type outcome struct {
				failures int
				err      error
				expected string
				actual   string
			}
			outcomes := make([]outcome, goroutines)

			var wg sync.WaitGroup
			for g := range goroutines {
				wg.Go(func() {
					out := &outcomes[g]
					env := evalengine.NewExpressionEnv(t.Context(), nil, evalengine.NewEmptyVCursor(venv, nil))
					for i := range iterations {
						// Goroutines start on different rows so that both
						// rows are being matched at any one time.
						which := (g + i) % len(rows)
						env.Row = rows[which]

						actual, err := evaluate(env)
						if err == nil && actual == tc.expected[which] {
							continue
						}
						if out.failures == 0 {
							out.err, out.expected, out.actual = err, tc.expected[which], actual
						}
						out.failures++
					}
				})
			}
			wg.Wait()

			for g, out := range outcomes {
				if !assert.Zerof(t, out.failures, "goroutine %d: %d of %d evaluations failed", g, out.failures, iterations) {
					require.NoErrorf(t, out.err, "goroutine %d: first failed evaluation", g)
					require.Equalf(t, out.expected, out.actual, "goroutine %d: first failed evaluation", g)
				}
			}
		})
	}
}

// BenchmarkCompiledRegexp measures the regexp functions with a constant
// pattern in a compiled program, evaluated by one goroutine and by all of them
// at once.
func BenchmarkCompiledRegexp(b *testing.B) {
	testCases := []struct {
		name       string
		expression string
		input      string
	}{
		{"like", `REGEXP_LIKE(column0, 'a+b')`, "xxaab"},
		{"like_backtracking", `REGEXP_LIKE(column0, 'a+b')`, strings.Repeat("a", 200) + "b"},
		{"instr", `REGEXP_INSTR(column0, 'a+b')`, "xxaab"},
		{"substr", `REGEXP_SUBSTR(column0, 'a+b')`, "xxaab"},
		{"replace", `REGEXP_REPLACE(column0, '([a-z]+)-([0-9]+)', '$2-$1')`, "abc-123 def-456"},
	}

	venv := vtenv.NewTestEnv()
	for _, tc := range testCases {
		row := []sqltypes.Value{sqltypes.NewVarChar(tc.input)}
		fields := evalengine.FieldResolver(makeFields(row))

		expr, err := venv.Parser().ParseExpr(tc.expression)
		require.NoError(b, err)
		translated, err := evalengine.Translate(expr, &evalengine.Config{
			ResolveColumn: fields.Column,
			ResolveType:   fields.Type,
			Collation:     collations.CollationUtf8mb4ID,
			Environment:   venv,
		})
		require.NoError(b, err)
		require.IsType(b, &evalengine.CompiledExpr{}, translated)

		b.Run(tc.name+"/serial", func(b *testing.B) {
			b.ReportAllocs()
			env := evalengine.NewExpressionEnv(b.Context(), nil, evalengine.NewEmptyVCursor(venv, nil))
			env.Row = row
			for b.Loop() {
				_, _ = env.Evaluate(translated)
			}
		})

		b.Run(tc.name+"/parallel", func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				env := evalengine.NewExpressionEnv(b.Context(), nil, evalengine.NewEmptyVCursor(venv, nil))
				env.Row = row
				for pb.Next() {
					_, _ = env.Evaluate(translated)
				}
			})
		})
	}
}
