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

package random

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/vtenv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/simplifier"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSimplifyResultsMismatchedQuery(t *testing.T) {
	t.Skip("Skip CI")

	var queries []string
	queries = append(queries, "select (68 - -16) / case false when -45 then 3 when 28 then -43 else -62 end as crandom0 from dept as tbl0, (select distinct not not false and count(*) from emp as tbl0, emp as tbl1 where tbl1.ename) as tbl1 limit 1",
		"select distinct case true when 'burro' then 'trout' else 'elf' end < case count(distinct true) when 'bobcat' then 'turkey' else 'penguin' end from dept as tbl0, emp as tbl1 where 'spider'",
		"select distinct sum(distinct tbl1.deptno) from dept as tbl0, emp as tbl1 where tbl0.deptno and tbl1.comm in (12, tbl0.deptno, case false when 67 then -17 when -78 then -35 end, -76 >> -68)",
		"select count(*) + 1 from emp as tbl0 order by count(*) desc",
		"select count(2 >> tbl2.mgr), sum(distinct tbl2.empno <=> 15) from emp as tbl0 left join emp as tbl2 on -32",
		"select sum(case false when true then tbl1.deptno else -154 / 132 end) as caggr1 from emp as tbl0, dept as tbl1",
		"select tbl1.dname as cgroup0, tbl1.dname as cgroup1 from dept as tbl0, dept as tbl1 group by tbl1.dname, tbl1.deptno order by tbl1.deptno desc",
		"select tbl0.ename as cgroup1 from emp as tbl0 group by tbl0.job, tbl0.ename having sum(tbl0.mgr) = sum(tbl0.mgr) order by tbl0.job desc, tbl0.ename asc limit 8",
		"select distinct count(*) as caggr1 from dept as tbl0, emp as tbl1 group by tbl1.sal having max(tbl1.comm) != true",
		"select distinct sum(tbl1.loc) as caggr0 from dept as tbl0, dept as tbl1 group by tbl1.deptno having max(tbl1.dname) <= 1",
		"select min(tbl0.deptno) as caggr0 from dept as tbl0, emp as tbl1 where case when false then tbl0.dname end group by tbl1.comm",
		"select count(*) as caggr0, 1 as crandom0 from dept as tbl0, emp as tbl1 where 1 = 0",
		"select count(*) as caggr0, 1 as crandom0 from dept as tbl0, emp as tbl1 where 'octopus'",
		"select distinct 'octopus' as crandom0 from dept as tbl0, emp as tbl1 where tbl0.deptno = tbl1.empno having count(*) = count(*)",
		"select max(tbl0.deptno) from dept as tbl0 right join emp as tbl1 on tbl0.deptno = tbl1.empno and tbl0.deptno = tbl1.deptno group by tbl0.deptno",
		"select count(tbl1.comm) from emp as tbl1 right join emp as tbl2 on tbl1.mgr = tbl2.sal")

	for _, query := range queries {
		var simplified string
		t.Run("simplification "+query, func(t *testing.T) {
			simplified = simplifyResultsMismatchedQuery(t, query)
		})

		t.Run("simplified "+query, func(t *testing.T) {
			mcmp, closer := start(t)
			defer closer()

			mcmp.ExecAllowAndCompareError(simplified)
		})

		fmt.Printf("final simplified query: %s\n", simplified)
	}
}

// given a query that errors with results mismatched, simplifyResultsMismatchedQuery returns a simpler version with the same error
func simplifyResultsMismatchedQuery(t *testing.T, query string) string {
	t.Helper()
	mcmp, closer := start(t)
	defer closer()

	_, err := mcmp.ExecAllowAndCompareError(query)
	if err == nil {
		t.Fatalf("query (%s) does not error", query)
	} else if !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("query (%s) does not error with results mismatched\nError: %v", query, err)
	}

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	formal, err := vindexes.LoadFormal("svschema.json")
	require.NoError(t, err)
	vSchema := vindexes.BuildVSchema(formal, sqlparser.NewTestParser())
	vSchemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:       vSchema,
		Version: planbuilder.Gen4,
		Env:     vtenv.NewTestEnv(),
	}

	stmt, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)

	simplified := simplifier.SimplifyStatement(
		stmt.(sqlparser.SelectStatement),
		vSchemaWrapper.CurrentDb(),
		vSchemaWrapper,
		func(statement sqlparser.SelectStatement) bool {
			q := sqlparser.String(statement)
			_, newErr := mcmp.ExecAllowAndCompareError(q)
			if newErr == nil {
				return false
			} else {
				return strings.Contains(newErr.Error(), "mismatched")
			}
		},
	)

	return sqlparser.String(simplified)
}
