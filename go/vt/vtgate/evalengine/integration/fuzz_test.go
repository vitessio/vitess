//go:build !race

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
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/evalengine/testcases"
	"vitess.io/vitess/go/vt/vtgate/simplifier"
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
)

var rhsOfIs = []string{
	"null",
	"not null",
	"true",
	"not true",
	"false",
	"not false",
}

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
	rhs := g.arg(op == "IN" || op == "NOT IN")
	if op == "IS" {
		rhs = rhsOfIs[g.rand.Intn(len(rhsOfIs))]
	}
	return fmt.Sprintf("%s %s %s", g.arg(false), op, rhs)
}

var (
	fuzzMaxTime     = 30 * time.Second
	fuzzMaxFailures = 0
	fuzzSeed        = time.Now().Unix()
	extractError    = regexp.MustCompile(`(.*?) \(errno (\d+)\) \(sqlstate (\w+)\) during query: (.*?)`)
	knownErrors     = []*regexp.Regexp{
		regexp.MustCompile(`value is out of range in '(.*?)'`),
		regexp.MustCompile(`Operand should contain (\d+) column\(s\)`),
		regexp.MustCompile(`You have an error in your SQL syntax; (.*?)`),
		regexp.MustCompile(`Cannot convert string '(.*?)' from \w+ to \w+`),
		regexp.MustCompile(`Invalid JSON text in argument (\d+) to function (\w+): (.*?)`),
		regexp.MustCompile(`Illegal mix of collations`),
		regexp.MustCompile(`Incorrect (DATE|DATETIME) value`),
		regexp.MustCompile(`Syntax error in regular expression`),
		regexp.MustCompile(`The regular expression contains an unclosed bracket expression`),
		regexp.MustCompile(`Illegal argument to a regular expression`),
		regexp.MustCompile(`Incorrect arguments to regexp_substr`),
		regexp.MustCompile(`Incorrect arguments to regexp_replace`),
	}
)

func init() {
	pflag.DurationVar(&fuzzMaxTime, "fuzz-duration", fuzzMaxTime, "Maximum time to fuzz for")
	pflag.IntVar(&fuzzMaxFailures, "fuzz-total", fuzzMaxFailures, "Maximum number of failures to fuzz for")
	pflag.Int64Var(&fuzzSeed, "fuzz-seed", fuzzSeed, "RNG seed when generating fuzz expressions")
}

func errorsMatch(remote, local error) bool {
	rem := extractError.FindStringSubmatch(remote.Error())
	if rem == nil {
		panic(fmt.Sprintf("could not extract error message: %q", remote.Error()))
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

func evaluateLocalEvalengine(env *evalengine.ExpressionEnv, query string, fields []*querypb.Field) (evalengine.EvalResult, error) {
	stmt, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		return evalengine.EvalResult{}, err
	}

	astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	local, err := func() (expr evalengine.Expr, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC during translate: %v", r)
			}
		}()
		cfg := &evalengine.Config{
			ResolveColumn:     evalengine.FieldResolver(fields).Column,
			Collation:         collations.CollationUtf8mb4ID,
			Environment:       env.VCursor().Environment(),
			NoConstantFolding: !debugSimplify,
		}
		expr, err = evalengine.Translate(astExpr, cfg)
		return
	}()

	if err != nil {
		return evalengine.EvalResult{}, err
	}

	return func() (eval evalengine.EvalResult, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}()
		eval, err = env.Evaluate(local)
		return
	}()
}

const syntaxErr = `You have an error in your SQL syntax; (errno 1064) (sqlstate 42000) during query: SQL`
const localSyntaxErr = `You have an error in your SQL syntax;`

type GoldenTest struct {
	Query string
	Value string `json:",omitempty"`
	Error string `json:",omitempty"`
}

func TestGenerateFuzzCases(t *testing.T) {
	if fuzzMaxFailures <= 0 {
		t.Skipf("skipping fuzz test generation")
	}
	var gen = gencase{
		rand:         rand.New(rand.NewSource(fuzzSeed)),
		ratioTuple:   8,
		ratioSubexpr: 8,
		tupleLen:     4,
		operators: []string{
			"+", "-", "/", "*", "=", "!=", "<=>", "<", "<=", ">", ">=", "IN", "NOT IN", "LIKE", "NOT LIKE", "IS",
		},
		primitives: []string{
			"1", "0", "-1", `"foo"`, `"FOO"`, `"fOo"`, "NULL", "12.0",
		},
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	venv := vtenv.NewTestEnv()
	compareWithMySQL := func(expr sqlparser.Expr) *mismatch {
		query := "SELECT " + sqlparser.String(expr)

		env := evalengine.EmptyExpressionEnv(venv)
		eval, localErr := evaluateLocalEvalengine(env, query, nil)
		remote, remoteErr := conn.ExecuteFetch(query, 1, false)

		if localErr != nil && strings.Contains(localErr.Error(), "syntax error at position") {
			localErr = fmt.Errorf(localSyntaxErr)
		}

		if remoteErr != nil && strings.Contains(remoteErr.Error(), "You have an error in your SQL syntax") {
			remoteErr = fmt.Errorf(syntaxErr)
		}

		res := mismatch{
			expr:      expr,
			localErr:  localErr,
			remoteErr: remoteErr,
		}
		if localErr == nil {
			res.localVal = eval.Value(collations.MySQL8().DefaultConnectionCharset())
		}
		if remoteErr == nil {
			res.remoteVal = remote.Rows[0][0]
		}
		if res.Error() != "" {
			return &res
		}
		return nil
	}

	var failures []*mismatch
	var start = time.Now()
	for len(failures) < fuzzMaxFailures {
		query := "SELECT " + gen.expr()
		stmt, err := sqlparser.NewTestParser().Parse(query)
		if err != nil {
			t.Fatal(err)
		}

		astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr

		if fail := compareWithMySQL(astExpr); fail != nil {
			failures = append(failures, fail)
			t.Errorf("mismatch: %v", fail.Error())
		}

		if time.Since(start) > fuzzMaxTime {
			break
		}
	}

	if len(failures) == 0 {
		return
	}

	var golden []GoldenTest

	for _, fail := range failures {
		failErr := fail.Error()
		start := time.Now()
		simplified := simplifier.SimplifyExpr(fail.expr, func(expr sqlparser.Expr) bool {
			err := compareWithMySQL(expr)
			if err == nil {
				return false
			}
			return err.Error() == failErr
		})

		t.Logf("simplified\n\t%s\n\t%s\n(%v)", sqlparser.String(fail.expr), sqlparser.String(simplified), time.Since(start))

		query := "SELECT " + sqlparser.String(simplified)
		if fail.remoteErr != nil {
			golden = append(golden, GoldenTest{
				Query: query,
				Error: fail.remoteErr.Error(),
			})
		} else {
			golden = append(golden, GoldenTest{
				Query: query,
				Value: fail.remoteVal.String(),
			})
		}
	}

	writeGolden(t, golden)
}

func writeGolden(t *testing.T, golden []GoldenTest) {
	out, err := os.Create(fmt.Sprintf("testdata/mysql_golden_%d.json", time.Now().Unix()))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	enc.SetIndent("", "    ")
	enc.SetEscapeHTML(false)
	enc.Encode(golden)
}

type mismatch struct {
	expr                sqlparser.Expr
	localErr, remoteErr error
	localVal, remoteVal sqltypes.Value
}

type Result struct {
	Error     error
	Value     sqltypes.Value
	Collation collations.ID
}

func compareResult(local, remote Result, cmp *testcases.Comparison) error {
	if local.Error != nil {
		if remote.Error == nil {
			return fmt.Errorf("%w: mysql response: %s", local.Error, remote.Value)
		}
		if !errorsMatch(remote.Error, local.Error) {
			return fmt.Errorf("mismatch in errors: eval=%w; mysql response: %w", local.Error, remote.Error)
		}
		return nil
	}

	if remote.Error != nil {
		for _, ke := range knownErrors {
			if ke.MatchString(remote.Error.Error()) {
				return nil
			}
		}
		return fmt.Errorf("%v; mysql failed with: %w", local.Value, remote.Error)
	}

	var localCollationName string
	var remoteCollationName string
	env := collations.MySQL8()
	if coll := local.Collation; coll != collations.Unknown {
		localCollationName = env.LookupName(coll)
	}
	if coll := remote.Collation; coll != collations.Unknown {
		remoteCollationName = env.LookupName(coll)
	}

	equals, err := cmp.Equals(local.Value, remote.Value, time.Now())
	if err != nil {
		return err
	}
	if !equals {
		return fmt.Errorf("different results: %s; mysql response: %s (local collation: %s; mysql collation: %s)",
			local.Value.String(), remote.Value.String(), localCollationName, remoteCollationName)
	}
	if local.Collation != remote.Collation {
		return fmt.Errorf("different collations: %s; mysql response: %s (local result: %s; mysql result: %s)",
			localCollationName, remoteCollationName, local.Value.String(), remote.Value.String(),
		)
	}
	return nil
}

func (cr *mismatch) Error() string {
	return compareResult(
		Result{
			Error: cr.localErr,
			Value: cr.localVal,
		},
		Result{
			Error: cr.remoteErr,
			Value: cr.remoteVal,
		},
		&testcases.Comparison{},
	).Error()
}
