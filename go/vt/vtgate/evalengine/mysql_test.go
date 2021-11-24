package evalengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func testSingle(t *testing.T, query string) (EvalResult, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}

	astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	converted, err := ConvertEx(astExpr, dummyCollation(45), true)
	if err == nil {
		return converted.Evaluate(nil)
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

			for _, tc := range testcases {
				debug := fmt.Sprintf("\n// Debug\neval, err := testSingle(t, %q)\nt.Logf(\"eval=%%s err=%%s\", eval.Value(), err)\n", tc.Query)
				eval, err := testSingle(t, tc.Query)
				if err != nil {
					if tc.Error == "" {
						t.Fatalf("query: %s\nmysql val: %s\nvitess err: %s\n%s", tc.Query, tc.Value, err.Error(), debug)
					}
					if !strings.Contains(tc.Error, err.Error()) {
						t.Fatalf("query: %s\nmysql err: %s\nvitess err: %s\n%s", tc.Query, tc.Error, err.Error(), debug)
					}
					continue
				}
				if tc.Error != "" {
					t.Fatalf("query: %s\nmysql err: %s\nvitess val: %s\n%s", tc.Query, tc.Error, eval.Value(), debug)
				}
				if eval.Value().String() != tc.Value {
					t.Fatalf("query: %s\nmysql val: %s\nvitess val: %s\n%s", tc.Query, tc.Value, eval.Value(), debug)
				}
			}
		})
	}
}

func TestDebug1(t *testing.T) {
	// Debug
	eval, err := testSingle(t, "SELECT 1 - \"fOo\"")
	t.Logf("eval=%s err=%s", eval.Value(), err)
}
