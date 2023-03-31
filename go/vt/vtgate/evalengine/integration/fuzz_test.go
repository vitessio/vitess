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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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

func evaluateLocalEvalengine(env *evalengine.ExpressionEnv, query string, fields []*querypb.Field) (evalengine.EvalResult, sqltypes.Type, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return evalengine.EvalResult{}, 0, err
	}

	astExpr := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr
	local, err := func() (expr evalengine.Expr, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC during translate: %v", r)
			}
		}()
		cfg := &evalengine.Config{
			ResolveColumn: evalengine.FieldResolver(fields).Column,
			Collation:     collations.CollationUtf8mb4ID,
			Optimization:  evalengine.OptimizationLevelNone,
		}
		if debugSimplify {
			cfg.Optimization = evalengine.OptimizationLevelSimplify
		}
		expr, err = evalengine.Translate(astExpr, cfg)
		return
	}()

	if err != nil {
		return evalengine.EvalResult{}, 0, err
	}

	return func() (eval evalengine.EvalResult, tt sqltypes.Type, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}()
		eval, err = env.Evaluate(local)
		if err == nil && debugCheckTypes {
			tt, err = env.TypeOf(local, fields)
			if errors.Is(err, evalengine.ErrAmbiguousType) {
				tt = -1
				err = nil
			}
		}
		return
	}()
}

const syntaxErr = `You have an error in your SQL syntax; (errno 1064) (sqlstate 42000) during query: SQL`
const localSyntaxErr = `You have an error in your SQL syntax;`

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

	compareWithMySQL := func(expr sqlparser.Expr) *mismatch {
		query := "SELECT " + sqlparser.String(expr)

		env := evalengine.NewExpressionEnv(context.Background(), nil, nil)
		eval, _, localErr := evaluateLocalEvalengine(env, query, nil)
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
			res.localVal = eval.Value()
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
		stmt, err := sqlparser.Parse(query)
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

	type evaltest struct {
		Query string
		Value string `json:",omitempty"`
		Error string `json:",omitempty"`
	}
	var golden []evaltest

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
			golden = append(golden, evaltest{
				Query: query,
				Error: fail.remoteErr.Error(),
			})
		} else {
			golden = append(golden, evaltest{
				Query: query,
				Value: fail.remoteVal.String(),
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
	enc.SetEscapeHTML(false)
	enc.Encode(golden)
}

type mismatch struct {
	expr                sqlparser.Expr
	localErr, remoteErr error
	localVal, remoteVal sqltypes.Value
}

const tolerance = 1e-14

func closeFloat(a, b float64, decimals uint32) bool {
	if decimals > 0 {
		ratio := math.Pow(10, float64(decimals))
		a = math.Round(a*ratio) / ratio
		b = math.Round(b*ratio) / ratio
	}

	if a == b {
		return true
	}
	if b == 0 {
		return math.Abs(a) < tolerance
	}
	return math.Abs((a-b)/b) < tolerance
}

func closeDatetime(a, b time.Time, diff time.Duration) bool {
	d := a.Sub(b)
	if d < 0 {
		d = -d
	}
	return d < diff
}

func compareResult(localErr, remoteErr error, localVal, remoteVal sqltypes.Value, localCollation, remoteCollation collations.ID, decimals uint32) string {
	if localErr != nil {
		if remoteErr == nil {
			return fmt.Sprintf("%v; mysql response: %s", localErr, remoteVal)
		}
		if !errorsMatch(remoteErr, localErr) {
			return fmt.Sprintf("mismatch in errors: eval=%s; mysql response: %s", localErr.Error(), remoteErr.Error())
		}
		return ""
	}

	if remoteErr != nil {
		for _, ke := range knownErrors {
			if ke.MatchString(remoteErr.Error()) {
				return ""
			}
		}
		return fmt.Sprintf("%v; mysql failed with: %s", localVal, remoteErr.Error())
	}

	var localCollationName string
	var remoteCollationName string
	if coll := localCollation.Get(); coll != nil {
		localCollationName = coll.Name()
	}
	if coll := remoteCollation.Get(); coll != nil {
		remoteCollationName = coll.Name()
	}

	if localVal.IsFloat() && remoteVal.IsFloat() {
		localFloat, err := localVal.ToFloat64()
		if err != nil {
			return fmt.Sprintf("error converting local value to float: %v", err)
		}
		remoteFloat, err := remoteVal.ToFloat64()
		if err != nil {
			return fmt.Sprintf("error converting remote value to float: %v", err)
		}
		if !closeFloat(localFloat, remoteFloat, decimals) {
			return fmt.Sprintf("different results: %s; mysql response: %s (local collation: %s; mysql collation: %s)",
				localVal.String(), remoteVal.String(), localCollationName, remoteCollationName)
		}
	} else if localVal.IsDateTime() && remoteVal.IsDateTime() {
		localDatetime, err := time.Parse("2006-01-02 15:04:05.999999", localVal.ToString())
		if err != nil {
			return fmt.Sprintf("error converting local value to datetime: %v", err)
		}
		remoteDatetime, err := time.Parse("2006-01-02 15:04:05.999999", remoteVal.ToString())
		if err != nil {
			return fmt.Sprintf("error converting remote value to datetime: %v", err)
		}
		if !closeDatetime(localDatetime, remoteDatetime, 100*time.Millisecond) {
			return fmt.Sprintf("different results: %s; mysql response: %s (local collation: %s; mysql collation: %s)",
				localVal.String(), remoteVal.String(), localCollationName, remoteCollationName)
		}

	} else if localVal.String() != remoteVal.String() {
		return fmt.Sprintf("different results: %s; mysql response: %s (local collation: %s; mysql collation: %s)",
			localVal.String(), remoteVal.String(), localCollationName, remoteCollationName)
	}
	if localCollation != remoteCollation {
		return fmt.Sprintf("different collations: %s; mysql response: %s (local result: %s; mysql result: %s)",
			localCollationName, remoteCollationName, localVal.String(), remoteVal.String(),
		)
	}

	return ""
}

func (cr *mismatch) Error() string {
	return compareResult(cr.localErr, cr.remoteErr, cr.localVal, cr.remoteVal, collations.Unknown, collations.Unknown, 0)
}
