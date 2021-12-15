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

package integration

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type (
	gencase struct {
		rand         *rand.Rand
		ratioTuple   int
		ratioSubexpr int
		tupleLen     int

		operators  []string
		primitives []string
	}

	dummyCollation collations.ID
)

func (g *gencase) arg(tuple bool) string {
	if tuple || g.rand.Intn(g.ratioTuple) == 0 {
		var exprs []string
		for i := 0; i < g.tupleLen; i++ {
			exprs = append(exprs, g.arg(false))
		}
		return fmt.Sprintf("(%s)", strings.Join(exprs, ", "))
	}
	if g.rand.Intn(g.ratioSubexpr) == 0 {
		return fmt.Sprintf("(%s)", g.expr())
	}
	return g.primitives[g.rand.Intn(len(g.primitives))]
}

func (g *gencase) expr() string {
	op := g.operators[g.rand.Intn(len(g.operators))]
	return fmt.Sprintf("%s %s %s", g.arg(false), op, g.arg(op == "IN" || op == "NOT IN"))
}

func (d dummyCollation) ColumnLookup(_ *sqlparser.ColName) (int, error) {
	panic("not supported")
}

func (d dummyCollation) CollationIDLookup(_ sqlparser.Expr) collations.ID {
	return collations.ID(d)
}

func TestTypes(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	var queries = []string{
		"1 > 3",
		"3 > 1",
		"-1 > -1",
		"1 = 1",
		"-1 = 1",
		"1 IN (1, -2, 3)",
		"1 LIKE 1",
		"-1 LIKE -1",
		"-1 LIKE 1",
		`"foo" IN ("bar", "FOO", "baz")`,
		`'pokemon' LIKE 'poke%'`,
		`(1, 2) = (1, 2)`,
		`1 = 'sad'`,
		`(1, 2) = (1, 3)`,
	}

	for _, query := range queries {
		query = "SELECT " + query
		remote, err := conn.ExecuteFetch(query, 1, false)
		if err != nil {
			t.Fatal(err)
		}
		local, _, err := safeEvaluate(query)
		if err != nil {
			t.Fatal(err)
		}
		if local.Value().String() != remote.Rows[0][0].String() {
			t.Errorf("mismatch for query %q: local=%v, remote=%v", query, local.Value().String(), remote.Rows[0][0].String())
		}
	}
}

var fuzzMaxFailures = flag.Int("fuzz-total", 0, "maximum number of failures to fuzz for")
var fuzzSeed = flag.Int64("fuzz-seed", 1234, "RNG seed when generating fuzz expressions")
var extractError = regexp.MustCompile(`(.*?) \(errno (\d+)\) \(sqlstate (\d+)\) during query: (.*?)`)

var knownErrors = []*regexp.Regexp{
	regexp.MustCompile(`value is out of range in '(.*?)'`),
	regexp.MustCompile(`Operand should contain (\d+) column\(s\)`),
}

func errorsMatch(remote, local error) bool {
	rem := extractError.FindStringSubmatch(remote.Error())
	if rem == nil {
		panic("could not extract error message")
	}

	remoteMessage := rem[1]
	localMessage := local.Error()

	if remoteMessage == localMessage {
		return true
	}
	for _, re := range knownErrors {
		if re.MatchString(remoteMessage) /* && re.MatchString(localMessage) */ {
			return true
		}
	}
	return false
}

func safeEvaluate(query string) (evalengine.EvalResult, bool, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return evalengine.EvalResult{}, false, err
	}

	astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	local, err := func() (expr evalengine.Expr, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}()
		expr, err = evalengine.ConvertEx(astExpr, dummyCollation(45), true)
		return
	}()

	var eval evalengine.EvalResult
	var evaluated bool
	if err == nil {
		evaluated = true
		eval, err = func() (eval evalengine.EvalResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			eval, err = (*evalengine.ExpressionEnv)(nil).Evaluate(local)
			return
		}()
	}
	return eval, evaluated, err
}

func TestGenerateFuzzCases(t *testing.T) {
	if *fuzzMaxFailures <= 0 {
		t.Skipf("skipping fuzz test generation")
	}

	type evaltest struct {
		Query string
		Value string `json:",omitempty"`
		Error string `json:",omitempty"`
	}

	var golden []evaltest
	var gen = gencase{
		rand:         rand.New(rand.NewSource(*fuzzSeed)),
		ratioTuple:   8,
		ratioSubexpr: 8,
		tupleLen:     4,
		operators: []string{
			"+", "-", "/", "*", "=", "!=", "<=>", "<", "<=", ">", ">=", "IN", "NOT IN", "LIKE", "NOT LIKE",
		},
		primitives: []string{
			"1", "0", "-1", `"foo"`, `"FOO"`, `"fOo"`, "NULL",
		},
	}

	var conn = mysqlconn(t)
	defer conn.Close()

nextCase:
	for len(golden) < *fuzzMaxFailures {
		query := "SELECT " + gen.expr()

		eval, evaluated, localErr := safeEvaluate(query)
		remote, remoteErr := conn.ExecuteFetch(query, 1, false)

		if localErr != nil {
			if remoteErr == nil {
				t.Errorf("local query %q failed: %v (eval=%v); mysql response: %s", query, localErr, evaluated, remote.Rows[0][0].String())
				goto failed
			}
			if !errorsMatch(remoteErr, localErr) {
				t.Errorf("mismatch in errors for %q: local=%q (eval=%v), remote=%q", query, localErr.Error(), evaluated, remoteErr.Error())
				goto failed
			}
			continue
		}

		if remoteErr != nil {
			for _, ke := range knownErrors {
				if ke.MatchString(remoteErr.Error()) {
					continue nextCase
				}
			}
			t.Errorf("remote query %q failed: %v, local=%s", query, remoteErr.Error(), eval.Value().String())
			goto failed
		}

		if eval.Value().String() != remote.Rows[0][0].String() {
			t.Errorf("mismatch for query %q: local=%v, remote=%v", query, eval.Value().String(), remote.Rows[0][0].String())
			goto failed
		}

		// OK -- all match
		continue

	failed:
		if remoteErr != nil {
			golden = append(golden, evaltest{
				Query: query,
				Error: remoteErr.Error(),
			})
		} else {
			golden = append(golden, evaltest{
				Query: query,
				Value: remote.Rows[0][0].String(),
			})
		}
	}

	out, err := os.Create(fmt.Sprintf("testdata/mysql_golden_%d.json", time.Now().Unix()))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	enc.SetIndent("", "    ")
	enc.Encode(golden)
}
