/*
Copyright 2021 The Vitess Authors.

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

package evalengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func knownBadQuery(expr Expr) bool {
	isNullSafeComparison := func(expr Expr) bool {
		if cmp, ok := expr.(*ComparisonExpr); ok {
			return cmp.Op.String() == "<=>"
		}
		return false
	}

	if isNullSafeComparison(expr) {
		cmp := expr.(*ComparisonExpr)
		return isNullSafeComparison(cmp.Left) || isNullSafeComparison(cmp.Right)
	}
	return false
}

var errKnownBadQuery = errors.New("this query is known to give bad results in MySQL")

func testSingle(t *testing.T, query string) (EvalResult, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}

	astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	converted, err := ConvertEx(astExpr, dummyCollation(45), false)
	if err == nil {
		if knownBadQuery(converted) {
			return EvalResult{}, errKnownBadQuery
		}
		return noenv.Evaluate(converted)
	}
	return EvalResult{}, err
}

func TestMySQLGolden(t *testing.T) {
	golden, _ := filepath.Glob("integration/testdata/*.json")
	for _, gld := range golden {
		t.Run(filepath.Base(gld), func(t *testing.T) {
			var testcases []struct {
				Query string
				Value string
				Error string
			}

			infile, err := os.Open(gld)
			if err != nil {
				t.Fatal(err)
			}

			if err := json.NewDecoder(infile).Decode(&testcases); err != nil {
				t.Fatal(err)
			}

			var ok int

			for _, tc := range testcases {
				debug := fmt.Sprintf("\n// Debug\neval, err := testSingle(t, `%s`)\nt.Logf(\"eval=%%s err=%%v\", eval.Value(), err) // want value=%q\n", tc.Query, tc.Value)
				eval, err := testSingle(t, tc.Query)
				if err == errKnownBadQuery {
					ok++
					continue
				}
				if err != nil {
					if tc.Error == "" {
						t.Errorf("query: %s\nmysql val: %s\nvitess err: %s\n%s", tc.Query, tc.Value, err.Error(), debug)
					} else if !strings.HasPrefix(tc.Error, err.Error()) {
						t.Errorf("query: %s\nmysql err: %s\nvitess err: %s\n%s", tc.Query, tc.Error, err.Error(), debug)
					} else {
						ok++
					}
					continue
				}
				if tc.Error != "" {
					t.Errorf("query: %s\nmysql err: %s\nvitess val: %s\n%s", tc.Query, tc.Error, eval.Value(), debug)
					continue
				}
				if eval.Value().String() != tc.Value {
					t.Errorf("query: %s\nmysql val: %s\nvitess val: %s\n%s", tc.Query, tc.Value, eval.Value(), debug)
					continue
				}
				ok++
			}

			t.Logf("passed %d/%d tests (%.02f%%)", ok, len(testcases), 100*float64(ok)/float64(len(testcases)))
		})
	}
}

func TestDebug1(t *testing.T) {
	// Debug
	eval, err := testSingle(t, `SELECT LEAST(0.0,'foobar')`)
	t.Logf("eval=%s err=%v", eval.Value(), err) // want value=""
}
