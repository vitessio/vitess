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

package abstract

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/engine"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type lineCountingReader struct {
	line int
	r    *bufio.Reader
}

func (lcr *lineCountingReader) nextLine() (string, error) {
	queryBytes, err := lcr.r.ReadBytes('\n')
	lcr.line++
	return string(queryBytes), err
}

func readTestCase(lcr *lineCountingReader) (testCase, error) {
	query := ""
	var err error
	for query == "" || query == "\n" || strings.HasPrefix(query, "#") {
		query, err = lcr.nextLine()
		if err != nil {
			return testCase{}, err
		}
	}

	tc := testCase{query: query, line: lcr.line}

	for {
		jsonPart, err := lcr.nextLine()
		if err != nil {
			if err == io.EOF {
				return tc, fmt.Errorf("test data is bad. expectation not finished")
			}
			return tc, err
		}
		if jsonPart == "}\n" {
			tc.expected += "}"
			break
		}
		tc.expected += jsonPart
	}
	return tc, nil
}

type testCase struct {
	line            int
	query, expected string
}

func TestOperator(t *testing.T) {
	fd, err := os.OpenFile("operator_test_data.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	r := bufio.NewReader(fd)

	hash, _ := vindexes.NewHash("user_index", map[string]string{})
	si := &semantics.FakeSI{VindexTables: map[string]vindexes.Vindex{"user_index": hash}}
	lcr := &lineCountingReader{r: r}
	for {
		tc, err := readTestCase(lcr)
		if err == io.EOF {
			break
		}
		t.Run(fmt.Sprintf("%d:%s", tc.line, tc.query), func(t *testing.T) {
			require.NoError(t, err)
			tree, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)
			semTable, err := semantics.Analyze(tree, "", si)
			require.NoError(t, err)
			optree, err := CreateLogicalOperatorFromAST(tree, semTable)
			require.NoError(t, err)
			output := testString(optree)
			if tc.expected != output {
				fmt.Println(1)
			}
			assert.Equal(t, tc.expected, output)
			if t.Failed() {
				fmt.Println(output)
			}
		})
	}
}

func testString(op Operator) string {
	switch op := op.(type) {
	case *QueryGraph:
		return fmt.Sprintf("QueryGraph: %s", op.testString())
	case *Join:
		leftStr := indent(testString(op.LHS))
		rightStr := indent(testString(op.RHS))
		if op.LeftJoin {
			return fmt.Sprintf("OuterJoin: {\n\tInner: %s\n\tOuter: %s\n\tPredicate: %s\n}", leftStr, rightStr, sqlparser.String(op.Predicate))
		}
		return fmt.Sprintf("Join: {\n\tLHS: %s\n\tRHS: %s\n\tPredicate: %s\n}", leftStr, rightStr, sqlparser.String(op.Predicate))
	case *Derived:
		inner := indent(testString(op.Inner))
		query := sqlparser.String(op.Sel)
		return fmt.Sprintf("Derived %s: {\n\tQuery: %s\n\tInner:%s\n}", op.Alias, query, inner)
	case *SubQuery:
		var inners []string
		for _, sqOp := range op.Inner {
			subquery := fmt.Sprintf("{\n\tType: %s", engine.PulloutOpcode(sqOp.ExtractedSubquery.OpCode).String())
			if sqOp.ExtractedSubquery.GetArgName() != "" {
				subquery += fmt.Sprintf("\n\tArgName: %s", sqOp.ExtractedSubquery.GetArgName())
			}
			subquery += fmt.Sprintf("\n\tQuery: %s\n}", indent(testString(sqOp.Inner)))
			subquery = indent(subquery)
			inners = append(inners, subquery)
		}
		outer := indent(testString(op.Outer))
		join := strings.Join(inners, "\n")
		sprintf := fmt.Sprintf("SubQuery: {\n\tSubQueries: [\n%s]\n\tOuter: %s\n}", join, outer)
		return sprintf
	case *Vindex:
		value := sqlparser.String(op.Value)
		return fmt.Sprintf("Vindex: {\n\tName: %s\n\tValue: %s\n}", op.Vindex.String(), value)
	case *Concatenate:
		var inners []string
		for _, source := range op.Sources {
			inners = append(inners, indent(testString(source)))
		}
		if len(op.OrderBy) > 0 {
			inners = append(inners, indent(sqlparser.String(op.OrderBy)[1:]))
		}
		if op.Limit != nil {
			inners = append(inners, indent(sqlparser.String(op.Limit)[1:]))
		}
		dist := ""
		if op.Distinct {
			dist = "(distinct)"
		}
		return fmt.Sprintf("Concatenate%s {\n%s\n}", dist, strings.Join(inners, ",\n"))
	case *Update:
		tbl := "table: " + op.Table.testString()
		var assignments []string
		// sort to produce stable results, otherwise test is flaky
		keys := make([]string, 0, len(op.Assignments))
		for k := range op.Assignments {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			assignments = append(assignments, fmt.Sprintf("\t%s = %s", k, sqlparser.String(op.Assignments[k])))
		}
		return fmt.Sprintf("Update {\n\t%s\nassignments:\n%s\n}", tbl, strings.Join(assignments, "\n"))
	}
	panic(fmt.Sprintf("%T", op))
}

func indent(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = "\t" + line
	}
	return strings.Join(lines, "\n")
}

// the following code is only used by tests

func (qt *QueryTable) testString() string {
	var alias string
	if !qt.Alias.As.IsEmpty() {
		alias = " AS " + sqlparser.String(qt.Alias.As)
	}
	var preds []string
	for _, predicate := range qt.Predicates {
		preds = append(preds, sqlparser.String(predicate))
	}
	var where string
	if len(preds) > 0 {
		where = " where " + strings.Join(preds, " and ")
	}

	return fmt.Sprintf("\t%v:%s%s%s", qt.ID, sqlparser.String(qt.Table), alias, where)
}

func (qg *QueryGraph) testString() string {
	return fmt.Sprintf(`{
Tables:
%s%s%s
}`, strings.Join(qg.tableNames(), "\n"), qg.crossPredicateString(), qg.noDepsString())
}

func (qg *QueryGraph) crossPredicateString() string {
	if len(qg.innerJoins) == 0 {
		return ""
	}
	var joinPreds []string
	for _, join := range qg.innerJoins {
		deps, predicates := join.deps, join.exprs
		var expressions []string
		for _, expr := range predicates {
			expressions = append(expressions, sqlparser.String(expr))
		}

		exprConcat := strings.Join(expressions, " and ")
		joinPreds = append(joinPreds, fmt.Sprintf("\t%v - %s", deps, exprConcat))
	}
	sort.Strings(joinPreds)
	return fmt.Sprintf("\nJoinPredicates:\n%s", strings.Join(joinPreds, "\n"))
}

func (qg *QueryGraph) tableNames() []string {
	var tables []string
	for _, t := range qg.Tables {
		tables = append(tables, t.testString())
	}
	return tables
}

func (qg *QueryGraph) noDepsString() string {
	if qg.NoDeps == nil {
		return ""
	}
	return fmt.Sprintf("\nForAll: %s", sqlparser.String(qg.NoDeps))
}
