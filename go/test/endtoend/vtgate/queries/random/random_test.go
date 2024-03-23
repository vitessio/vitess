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
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// this test uses the AST defined in the sqlparser package to randomly generate queries

// if true then execution will always stop on a "must fix" error: a results mismatched or EOF
const stopOnMustFixError = false

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"emp", "dept"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	// disable only_full_group_by
	// mcmp.Exec("set sql_mode=''")

	// insert data
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20), (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30), (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30), (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20), (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30), (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30), (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10), (7788,'SCOTT','ANALYST',7566,'1982-12-09',3000,NULL,20), (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10), (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30), (7876,'ADAMS','CLERK',7788,'1983-01-12',1100,NULL,20), (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30), (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20), (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10)")
	mcmp.Exec("INSERT INTO dept(deptno, dname, loc) VALUES ('10','ACCOUNTING','NEW YORK'), ('20','RESEARCH','DALLAS'), ('30','SALES','CHICAGO'), ('40','OPERATIONS','BOSTON')")

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func helperTest(t *testing.T, query string) {
	t.Helper()
	t.Run(query, func(t *testing.T) {
		mcmp, closer := start(t)
		defer closer()

		result, err := mcmp.ExecAllowAndCompareError(query)
		fmt.Println(result)
		fmt.Println(err)
	})
}

func TestMustFix(t *testing.T) {
	t.Skip("Skip CI")

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	// results mismatched
	helperTest(t, "select distinct case count(*) when 0 then -0 end from emp as tbl0, emp as tbl1 where 0")

	// results mismatched (maybe derived tables)
	helperTest(t, "select 0 as crandom0 from dept as tbl0, (select distinct count(*) from emp as tbl1 where 0) as tbl1")

	// results mismatched
	helperTest(t, "select distinct case count(distinct true) when 'b' then 't' end from emp as tbl1 where 's'")

	// results mismatched
	helperTest(t, "select distinct sum(distinct tbl1.deptno) from dept as tbl0, emp as tbl1")

	// mismatched number of columns
	helperTest(t, "select count(*) + 0 from emp as tbl0 order by count(*) desc")

	// results mismatched (mismatched types)
	helperTest(t, "select count(0 >> 0), sum(distinct tbl2.empno) from emp as tbl0 left join emp as tbl2 on -32")

	// results mismatched (decimals off by a little; evalengine problem)
	helperTest(t, "select sum(case false when true then tbl1.deptno else -154 / 132 end) as caggr1 from emp as tbl0, dept as tbl1")

	// EOF
	helperTest(t, "select tbl1.dname as cgroup0, tbl1.dname as cgroup1, tbl1.deptno as crandom0 from dept as tbl0, dept as tbl1 group by tbl1.dname, tbl1.deptno order by tbl1.deptno desc")

	// results mismatched
	// limit >= 9 works
	helperTest(t, "select tbl0.ename as cgroup1 from emp as tbl0 group by tbl0.job, tbl0.ename having sum(tbl0.mgr) order by tbl0.job desc, tbl0.ename asc limit 8")

	// results mismatched
	helperTest(t, "select distinct count(*) as caggr1 from emp as tbl1 group by tbl1.sal having max(0) != true")

	// results mismatched
	helperTest(t, "select distinct 0 as caggr0 from dept as tbl0, dept as tbl1 group by tbl1.deptno having max(0) <= 0")

	// results mismatched
	helperTest(t, "select min(0) as caggr0 from dept as tbl0, emp as tbl1 where case when false then tbl0.dname end group by tbl1.comm")

	// results mismatched
	helperTest(t, "select count(*) as caggr0, 0 as crandom0 from dept as tbl0, emp as tbl1 where 0")

	// results mismatched
	helperTest(t, "select count(*) as caggr0, 0 as crandom0 from dept as tbl0, emp as tbl1 where 'o'")

	// similar to previous two
	// results mismatched
	helperTest(t, "select distinct 'o' as crandom0 from dept as tbl0, emp as tbl1 where 0 having count(*) = count(*)")

	// results mismatched (group by + right join)
	// left instead of right works
	// swapping tables and predicates and changing to left fails
	helperTest(t, "select 0 from dept as tbl0 right join emp as tbl1 on tbl0.deptno = tbl1.empno and tbl0.deptno = tbl1.deptno group by tbl0.deptno")

	// results mismatched (count + right join)
	// left instead of right works
	// swapping tables and predicates and changing to left fails
	helperTest(t, "select count(tbl1.comm) from emp as tbl1 right join emp as tbl2 on tbl1.mgr = tbl2.sal")

	// Passes with different errors
	// vitess error: EOF
	// mysql error: Operand should contain 1 column(s)
	helperTest(t, "select 8 < -31 xor (-29, sum((tbl0.deptno, 'wren', 'ostrich')), max(distinct (tbl0.dname, -15, -8))) in ((sum(distinct (tbl0.dname, 'bengal', -10)), 'ant', true)) as caggr0 from dept as tbl0 where tbl0.deptno * (77 - 61)")

	// EOF
	helperTest(t, "select tbl1.deptno as cgroup0, tbl1.loc as cgroup1, count(distinct tbl1.loc) as caggr1, tbl1.loc as crandom0 from dept as tbl0, dept as tbl1 group by tbl1.deptno, tbl1.loc")

	// EOF
	helperTest(t, "select count(*) from dept as tbl0, (select count(*) from emp as tbl0, emp as tbl1 limit 18) as tbl1")
}

func TestKnownFailures(t *testing.T) {
	t.Skip("Skip CI")

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	// logs more stuff
	// clusterInstance.EnableGeneralLog()

	// VT13001: [BUG] failed to find the corresponding column
	helperTest(t, "select tbl1.dname as cgroup0, tbl1.dname as cgroup1 from dept as tbl0, dept as tbl1 group by tbl1.dname, tbl1.deptno order by tbl1.deptno desc")

	// vitess error: <nil>
	// mysql error: Operand should contain 1 column(s)
	helperTest(t, "select (count('sheepdog') ^ (-71 % sum(emp.mgr) ^ count('koi')) and count(*), 'fly') from emp, dept")

	// rhs of an In operation should be a tuple
	helperTest(t, "select (case when true then min(distinct tbl1.job) else 'bee' end, 'molly') not in (('dane', 0)) as caggr1 from emp as tbl0, emp as tbl1")

	// VT13001: [BUG] in scatter query: complex ORDER BY expression: :vtg1 /* VARCHAR */
	helperTest(t, "select tbl1.job as cgroup0, sum(distinct 'mudfish'), tbl1.job as crandom0 from emp as tbl0, emp as tbl1 group by tbl1.job order by tbl1.job asc limit 8, 1")

	// unsupported: min/max on types that are not comparable is not supported
	helperTest(t, "select max(case true when false then 'gnu' when true then 'meerkat' end) as caggr0 from dept as tbl0")

	// vttablet: rpc error: code = InvalidArgument desc = BIGINT UNSIGNED value is out of range in '(-(273) + (-(15) & 124))'
	helperTest(t, "select -273 + (-15 & 124) as crandom0 from emp as tbl0, emp as tbl1 where tbl1.sal >= tbl1.mgr")

	// vitess error: <nil>
	// mysql error: Incorrect DATE value: 'tuna'
	helperTest(t, "select min(tbl0.empno) as caggr0 from emp as tbl0 where case 'gator' when false then 314 else 'weevil' end > tbl0.job having min(tbl0.hiredate) <=> 'tuna'")

	// vitess error: <nil>
	// mysql error: Unknown column 'tbl0.deptno' in 'having clause'
	helperTest(t, "select count(*) as caggr0 from dept as tbl0 having tbl0.deptno")

	// only_full_group_by enabled
	// vitess error: In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'ks_random.tbl0.EMPNO'; this is incompatible with sql_mode=only_full_group_by
	helperTest(t, "select distinct tbl0.empno as cgroup0, count(distinct 56) as caggr0, min('flounder' = 'penguin') as caggr1 from emp as tbl0, (select 'manatee' as crandom0 from dept as tbl0 where -26 limit 2) as tbl2 where 'anteater' like 'catfish' is null and -11 group by tbl0.empno order by tbl0.empno asc, count(distinct 56) asc, min('flounder' = 'penguin') desc")

	// only_full_group_by enabled
	// vitess error: <nil>
	// mysql error: In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'ks_random.tbl0.ENAME'; this is incompatible with sql_mode=only_full_group_by
	helperTest(t, "select tbl0.ename, min(tbl0.comm) from emp as tbl0 left join emp as tbl1 on tbl0.empno = tbl1.comm and tbl0.empno = tbl1.empno")

	// only_full_group_by enabled
	// vitess error: <nil>
	// mysql error: Expression #1 of ORDER BY clause is not in SELECT list, references column 'ks_random.tbl2.DNAME' which is not in SELECT list; this is incompatible with DISTINCT
	helperTest(t, "select distinct count(*) as caggr0 from dept as tbl2 group by tbl2.dname order by tbl2.dname asc")

	// vttablet: rpc error: code = NotFound desc = Unknown column 'cgroup0' in 'field list' (errno 1054) (sqlstate 42S22) (CallerID: userData1)
	helperTest(t, "select tbl1.ename as cgroup0, max(tbl0.comm) as caggr0 from emp as tbl0, emp as tbl1 group by cgroup0")

	// unsupported
	// VT12001: unsupported: only one DISTINCT aggregation is allowed in a SELECT: sum(distinct 1) as caggr1
	helperTest(t, "select sum(distinct tbl0.comm) as caggr0, sum(distinct 1) as caggr1 from emp as tbl0 having 'redfish' < 'blowfish'")

	// unsupported
	// VT12001: unsupported: in scatter query: aggregation function 'avg(tbl0.deptno)'
	helperTest(t, "select avg(tbl0.deptno) from dept as tbl0")

	// unsupported
	// VT12001: unsupported: LEFT JOIN with derived tables
	helperTest(t, "select -1 as crandom0 from emp as tbl2 left join (select count(*) from dept as tbl1) as tbl3 on 6 != tbl2.deptno")

	// unsupported
	// VT12001: unsupported: subqueries in GROUP BY
	helperTest(t, "select exists (select 1) as crandom0 from dept as tbl0 group by exists (select 1)")
}

func TestRandom(t *testing.T) {
	t.Skip("Skip CI; random expressions generate too many failures to properly limit")

	mcmp, closer := start(t)
	defer closer()

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	// specify the schema (that is defined in schema.sql)
	schemaTables := []tableT{
		{tableExpr: sqlparser.NewTableName("emp")},
		{tableExpr: sqlparser.NewTableName("dept")},
	}
	schemaTables[0].addColumns([]column{
		{name: "empno", typ: "bigint"},
		{name: "ename", typ: "varchar"},
		{name: "job", typ: "varchar"},
		{name: "mgr", typ: "bigint"},
		{name: "hiredate", typ: "date"},
		{name: "sal", typ: "bigint"},
		{name: "comm", typ: "bigint"},
		{name: "deptno", typ: "bigint"},
	}...)
	schemaTables[1].addColumns([]column{
		{name: "deptno", typ: "bigint"},
		{name: "dname", typ: "varchar"},
		{name: "loc", typ: "varchar"},
	}...)

	endBy := time.Now().Add(1 * time.Second)

	var queryCount, queryFailCount int
	// continue testing after an error if and only if testFailingQueries is true
	for time.Now().Before(endBy) && (!t.Failed() || !testFailingQueries) {
		genConfig := sqlparser.NewExprGeneratorConfig(sqlparser.CannotAggregate, "", 0, false)
		qg := newQueryGenerator(genConfig, 2, 2, 2, schemaTables)
		qg.randomQuery()
		query := sqlparser.String(qg.stmt)
		_, vtErr := mcmp.ExecAllowAndCompareError(query)

		// this assumes all queries are valid mysql queries
		if vtErr != nil {
			fmt.Println(query)
			fmt.Println(vtErr)

			if stopOnMustFixError {
				// results mismatched
				if strings.Contains(vtErr.Error(), "results mismatched") {
					simplified := simplifyResultsMismatchedQuery(t, query)
					fmt.Printf("final simplified query: %s\n", simplified)
					break
				}
				// EOF
				if sqlError, ok := vtErr.(*sqlerror.SQLError); ok && strings.Contains(sqlError.Message, "EOF") {
					break
				}
			}

			// restart the mysql and vitess connections in case something bad happened
			closer()
			mcmp, closer = start(t)

			fmt.Printf("\n\n\n")
			queryFailCount++
		}
		queryCount++
	}
	fmt.Printf("Queries successfully executed: %d\n", queryCount)
	fmt.Printf("Queries failed: %d\n", queryFailCount)
}

// these queries were previously failing and have now been fixed
func TestBuggyQueries(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	mcmp.Exec("select sum(tbl1.sal) as caggr1 from emp as tbl0, emp as tbl1 group by tbl1.ename order by tbl1.ename asc")
	mcmp.Exec("select count(*), count(*), count(*) from dept as tbl0, emp as tbl1 where tbl0.deptno = tbl1.deptno group by tbl1.empno order by tbl1.empno")
	mcmp.Exec("select count(tbl0.deptno) from dept as tbl0, emp as tbl1 group by tbl1.job order by tbl1.job limit 3")
	mcmp.Exec("select count(*), count(*) from emp as tbl0 group by tbl0.empno order by tbl0.empno")
	mcmp.Exec("select distinct count(*), tbl0.loc from dept as tbl0 group by tbl0.loc")
	mcmp.Exec("select distinct count(*) from dept as tbl0 group by tbl0.loc")
	mcmp.Exec("select sum(tbl1.comm) from emp as tbl0, emp as tbl1")
	mcmp.Exec("select tbl1.mgr, tbl1.mgr, count(*) from emp as tbl1 group by tbl1.mgr")
	mcmp.Exec("select tbl1.mgr, tbl1.mgr, count(*) from emp as tbl0, emp as tbl1 group by tbl1.mgr")
	mcmp.Exec("select count(*), count(*), count(tbl0.comm) from emp as tbl0, emp as tbl1 join dept as tbl2")
	mcmp.Exec("select count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0, dept as tbl1")
	mcmp.Exec("select count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0")
	mcmp.Exec("select min(tbl0.loc) from dept as tbl0")
	mcmp.Exec("select tbl1.empno, max(tbl1.job) from dept as tbl0, emp as tbl1 group by tbl1.empno")
	mcmp.Exec("select tbl1.ename, max(tbl0.comm) from emp as tbl0, emp as tbl1 group by tbl1.ename")
	mcmp.Exec("select tbl0.dname, tbl0.dname, min(tbl0.deptno) from dept as tbl0, dept as tbl1 group by tbl0.dname, tbl0.dname")
	mcmp.Exec("select tbl0.dname, min(tbl1.deptno) from dept as tbl0, dept as tbl1 group by tbl0.dname, tbl1.dname")
	mcmp.Exec("select max(tbl0.hiredate) from emp as tbl0")
	mcmp.Exec("select min(tbl0.deptno) as caggr0, count(*) as caggr1 from dept as tbl0 left join dept as tbl1 on tbl1.loc = tbl1.dname")
	mcmp.Exec("select count(tbl1.loc) as caggr0 from dept as tbl1 left join dept as tbl2 on tbl1.loc = tbl2.loc where (tbl2.deptno)")
	mcmp.Exec("select sum(tbl1.ename), min(tbl0.empno) from emp as tbl0, emp as tbl1 left join dept as tbl2 on tbl1.job = tbl2.loc and tbl1.comm = tbl2.deptno where ('trout') and tbl0.deptno = tbl1.comm")
	mcmp.Exec("select distinct max(tbl0.deptno), count(tbl0.job) from emp as tbl0, dept as tbl1 left join dept as tbl2 on tbl1.dname = tbl2.loc and tbl1.dname = tbl2.loc where (tbl2.loc) and tbl0.deptno = tbl1.deptno")
	mcmp.Exec("select count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0")
	mcmp.Exec("select distinct max(tbl0.dname) as caggr0, 'cattle' as crandom0 from dept as tbl0, emp as tbl1 where tbl0.deptno != tbl1.sal group by tbl1.comm")
	mcmp.Exec("select -26 in (tbl2.mgr, -8, tbl0.deptno) as crandom0 from dept as tbl0, emp as tbl1 left join emp as tbl2 on tbl2.ename")
	mcmp.Exec("select max(tbl1.dname) as caggr1 from dept as tbl0, dept as tbl1 group by tbl1.dname order by tbl1.dname asc")
	mcmp.Exec("select distinct tbl1.hiredate as cgroup0, count(tbl1.mgr) as caggr0 from emp as tbl1 group by tbl1.hiredate, tbl1.ename")
	mcmp.Exec("select distinct 347 as crandom0 from emp as tbl0")
	mcmp.Exec("select distinct count(*) from dept as tbl0 group by tbl0.deptno")
	mcmp.Exec("select count(*) from dept as tbl1 join (select count(*) from emp as tbl0, dept as tbl1 group by tbl1.loc) as tbl2")
	mcmp.Exec("select (select count(*) from emp as tbl0) from emp as tbl0")
	mcmp.Exec("select count(tbl1.dname) as caggr1 from dept as tbl0 left join dept as tbl1 on tbl1.dname > tbl1.loc where tbl1.loc <=> tbl1.dname group by tbl1.dname order by tbl1.dname asc")
	mcmp.Exec("select count(*) from (select count(*) from dept as tbl0) as tbl0")
	mcmp.Exec("select count(*), count(*) from (select count(*) from dept as tbl0) as tbl0, dept as tbl1")
	mcmp.Exec(`select distinct case max(tbl0.ename) when min(tbl0.job) then 'sole' else count(case when false then -27 when 'gazelle' then tbl0.deptno end) end as caggr0 from emp as tbl0`)
}
