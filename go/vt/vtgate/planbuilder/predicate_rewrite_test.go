/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/slice"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type testCase struct {
	nodes int
	depth int
}

type nodeType int

const (
	NODE nodeType = iota
	NOT
	AND
	OR
	XOR
	SIZE
)

func (tc testCase) createPredicate(lvl int) sqlparser.Expr {
	if lvl >= tc.depth {
		// we're at max depth, so we just return one of the nodes
		n := rand.IntN(tc.nodes)
		return sqlparser.NewColName(fmt.Sprintf("n%d", n))
	}
	switch nodeType(rand.IntN(int(SIZE))) {
	case NODE:
		n := rand.IntN(tc.nodes)
		return sqlparser.NewColName(fmt.Sprintf("n%d", n))
	case NOT:
		return &sqlparser.NotExpr{
			Expr: tc.createPredicate(lvl + 1),
		}
	case AND:
		return sqlparser.AndExpressions(
			tc.createPredicate(lvl+1),
			tc.createPredicate(lvl+1),
		)
	case OR:
		return &sqlparser.OrExpr{
			Left:  tc.createPredicate(lvl + 1),
			Right: tc.createPredicate(lvl + 1),
		}
	case XOR:
		return &sqlparser.XorExpr{
			Left:  tc.createPredicate(lvl + 1),
			Right: tc.createPredicate(lvl + 1),
		}
	}
	panic("unexpected nodeType")
}

func TestOneRewriting(t *testing.T) {
	// This test is a simple test that takes a single expression and simplifies it.
	// While simplifying, it also collects all the steps that were taken to simplify the expression,
	// and then runs both the original and simplified expressions with all possible values for the columns.
	// If the two expressions do not return the same value, this is considered a test failure.
	// This test is useful for debugging and understanding how the simplification works.
	venv := vtenv.NewTestEnv()
	sqlparser.DebugRewrite = true

	// Modify these
	const numberOfColumns = 2
	const expr = "not (n1 xor n0 or n1)"

	predicate, err := sqlparser.NewTestParser().ParseExpr(expr)
	require.NoError(t, err)

	var steps []sqlparser.SQLNode

	simplified := sqlparser.RewritePredicateInternal(predicate, func(n sqlparser.SQLNode) {
		steps = append(steps, n)
	})
	fmt.Println(sqlparser.String(simplified))

	cfg := &evalengine.Config{
		Environment:   venv,
		Collation:     collations.MySQL8().DefaultConnectionCharset(),
		ResolveColumn: resolveForFuzz,
	}
	original, err := evalengine.Translate(predicate, cfg)
	require.NoError(t, err)

	for _, step := range steps {
		name := sqlparser.String(step)
		t.Run(name, func(t *testing.T) {
			simpler, err := evalengine.Translate(step.(sqlparser.Expr), cfg)
			require.NoError(t, err)

			env := evalengine.EmptyExpressionEnv(venv)
			env.Row = make([]sqltypes.Value, numberOfColumns)
			for i := range env.Row {
				env.Row[i] = sqltypes.NULL
			}

			testValues(t, env, 0, original, simpler)
		})
	}

}

func TestFuzzRewriting(t *testing.T) {
	// This test, that runs for one second only, will produce lots of random boolean expressions,
	// mixing AND, NOT, OR, XOR and column expressions.
	// It then takes the predicate and simplifies it
	// Finally, it runs both the original and simplified predicate with all combinations of column
	// values - trying TRUE, FALSE and NULL. If the two expressions do not return the same value,
	// this is considered a test failure.
	start := time.Now()
	for time.Since(start) < 1*time.Second {
		tc := testCase{
			nodes: rand.IntN(4) + 1,
			depth: rand.IntN(4) + 1,
		}

		predicate := tc.createPredicate(0)
		name := sqlparser.String(predicate)
		t.Run(name, func(t *testing.T) {
			venv := vtenv.NewTestEnv()
			simplified := sqlparser.RewritePredicate(predicate)

			cfg := &evalengine.Config{
				Environment:       venv,
				Collation:         collations.MySQL8().DefaultConnectionCharset(),
				ResolveColumn:     resolveForFuzz,
				NoConstantFolding: true,
				NoCompilation:     true,
			}
			original, err := evalengine.Translate(predicate, cfg)
			require.NoError(t, err)
			simpler, err := evalengine.Translate(simplified.(sqlparser.Expr), cfg)
			require.NoError(t, err)

			env := evalengine.EmptyExpressionEnv(venv)
			env.Row = make([]sqltypes.Value, tc.nodes)
			for i := range env.Row {
				env.Row[i] = sqltypes.NewInt32(1)
			}

			testValues(t, env, 0, original, simpler)
		})
	}
}

func testValues(t *testing.T, env *evalengine.ExpressionEnv, i int, original, simpler evalengine.Expr) {
	for n := 0; n < 3; n++ {
		switch n {
		case 0:
			env.Row[i] = sqltypes.NewInt32(0)
		case 1:
			env.Row[i] = sqltypes.NewInt32(1)
		case 2:
			env.Row[i] = sqltypes.NULL
		}

		v1, err := env.Evaluate(original)
		require.NoError(t, err)
		v2, err := env.Evaluate(simpler)
		require.NoError(t, err)
		v1Value := v1.Value(collations.MySQL8().DefaultConnectionCharset())
		v2Value := v2.Value(collations.MySQL8().DefaultConnectionCharset())
		row := strings.Join(slice.Map(env.Row, func(i sqltypes.Value) string {
			return i.String()
		}), " | ")
		msg := fmt.Sprintf("original: %v (%s)\nsimplified: %v (%s)\nrow: %v", sqlparser.String(original), v1Value.String(), sqlparser.String(simpler), v2Value.String(), row)
		require.True(
			t,
			v1Value.Equal(v2Value),
			msg,
		)
		if len(env.Row) > i+1 {
			testValues(t, env, i+1, original, simpler)
		}
	}
}

func resolveForFuzz(colname *sqlparser.ColName) (int, error) {
	offsetStr := colname.Name.String()[1:]
	return strconv.Atoi(offsetStr)
}
