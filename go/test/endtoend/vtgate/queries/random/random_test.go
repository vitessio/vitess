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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// this test uses the AST defined in the sqlparser package to randomly generate queries

// if true then execution will always stop on a "must fix" error: a mismatched results or EOF
const stopOnMustFixError = true

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"dept", "emp"}
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

	// mismatched results
	// sum values returned as int64 instead of decimal
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ sum(tbl1.sal) as caggr1 from emp as tbl0, emp as tbl1 group by tbl1.ename order by tbl1.ename asc")

	// mismatched results
	// limit >= 9 works
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ tbl0.ename as cgroup1 from emp as tbl0 group by tbl0.job, tbl0.ename having sum(tbl0.mgr) = sum(tbl0.mgr) order by tbl0.job desc, tbl0.ename asc limit 8")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct count(*) as caggr1 from dept as tbl0, emp as tbl1 group by tbl1.sal having max(tbl1.comm) != true")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct sum(tbl1.loc) as caggr0 from dept as tbl0, dept as tbl1 group by tbl1.deptno having max(tbl1.dname) <= 1")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct max(tbl0.dname) as caggr0, 'cattle' as crandom0 from dept as tbl0, emp as tbl1 where tbl0.deptno != tbl1.sal group by tbl1.comm")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) as caggr0, 1 as crandom0 from dept as tbl0, emp as tbl1 where 1 = 0")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) as caggr0, 1 as crandom0 from dept as tbl0, emp as tbl1 where 'octopus'")

	// similar to previous two
	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct 'octopus' as crandom0 from dept as tbl0, emp as tbl1 where tbl0.deptno = tbl1.empno having count(*) = count(*)")

	// mismatched results
	// previously failing, then succeeding query, now failing again
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(tbl0.deptno) from dept as tbl0, emp as tbl1 group by tbl1.job order by tbl1.job limit 3")

	// mismatched results (group by + right join)
	// left instead of right works
	// swapping tables and predicates and changing to left fails
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ max(tbl0.deptno) from dept as tbl0 right join emp as tbl1 on tbl0.deptno = tbl1.empno and tbl0.deptno = tbl1.deptno group by tbl0.deptno")

	// mismatched results (count + right join)
	// left instead of right works
	// swapping tables and predicates and changing to left fails
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(tbl1.comm) from emp as tbl1 right join emp as tbl2 on tbl1.mgr = tbl2.sal")

	// EOF
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) from dept as tbl0, (select count(*) from emp as tbl0, emp as tbl1 limit 18) as tbl1")

	// EOF
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0")
}

func TestKnownFailures(t *testing.T) {
	t.Skip("Skip CI")

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	// logs more stuff
	//clusterInstance.EnableGeneralLog()

	// cannot compare strings, collation is unknown or unsupported (collation ID: 0)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ max(tbl1.dname) as caggr1 from dept as tbl0, dept as tbl1 group by tbl1.dname order by tbl1.dname asc")

	// vitess error: <nil>
	// mysql error: Incorrect DATE value: 'tuna'
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ min(tbl0.empno) as caggr0 from emp as tbl0 where case 'gator' when false then 314 else 'weevil' end > tbl0.job having min(tbl0.hiredate) <=> 'tuna'")

	// vitess error: <nil>
	// mysql error: Unknown column 'tbl0.deptno' in 'having clause'
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) as caggr0 from dept as tbl0 having tbl0.deptno")

	// coercion should not try to coerce this value: DATE("1980-12-17")
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct tbl1.hiredate as cgroup0, count(tbl1.mgr) as caggr0 from emp as tbl1 group by tbl1.hiredate, tbl1.ename")

	// only_full_group_by enabled (vitess sometimes (?) produces the correct result assuming only_full_group_by is disabled)
	// vitess error: nil
	// mysql error: In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'ks_random.tbl0.ENAME'; this is incompatible with sql_mode=only_full_group_by
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ tbl0.ename, min(tbl0.comm) from emp as tbl0 left join emp as tbl1 on tbl0.empno = tbl1.comm and tbl0.empno = tbl1.empno")

	// only_full_group_by enabled
	// vitess error:  nil
	// mysql error: Expression #1 of ORDER BY clause is not in SELECT list, references column 'ks_random.tbl2.DNAME' which is not in SELECT list; this is incompatible with DISTINCT
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct count(*) as caggr0 from dept as tbl2 group by tbl2.dname order by tbl2.dname asc")

	// vttablet: rpc error: code = NotFound desc = Unknown column 'cgroup0' in 'field list' (errno 1054) (sqlstate 42S22) (CallerID: userData1)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ tbl1.ename as cgroup0, max(tbl0.comm) as caggr0 from emp as tbl0, emp as tbl1 group by cgroup0")

	// vttablet: rpc error: code = NotFound desc = Unknown column '347' in 'group statement'
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct 347 as crandom0 from emp as tbl0")

	// vttablet: rpc error: code = InvalidArgument desc = Can't group on 'count(*)' (errno 1056) (sqlstate 42000) (CallerID: userData1)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct count(*) from dept as tbl0 group by tbl0.deptno")

	// [BUG] push projection does not yet support: *planbuilder.memorySort (errno 1815) (sqlstate HY000)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) from dept as tbl1 join (select count(*) from emp as tbl0, dept as tbl1 group by tbl1.loc) as tbl2")

	// unsupported
	// unsupported: in scatter query: complex aggregate expression (errno 1235) (sqlstate 42000)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ (select count(*) from emp as tbl0) from emp as tbl0")

	// unsupported
	// unsupported: using aggregation on top of a *planbuilder.filter plan
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(tbl1.dname) as caggr1 from dept as tbl0 left join dept as tbl1 on tbl1.dname > tbl1.loc where tbl1.loc <=> tbl1.dname group by tbl1.dname order by tbl1.dname asc")

	// unsupported
	// unsupported: using aggregation on top of a *planbuilder.orderedAggregate plan
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) from (select count(*) from dept as tbl0) as tbl0")

	// unsupported
	// unsupported: using aggregation on top of a *planbuilder.orderedAggregate plan
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0) as tbl0, dept as tbl1")

	// unsupported
	// unsupported: in scatter query: aggregation function 'avg(tbl0.deptno)'
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ avg(tbl0.deptno) from dept as tbl0")
}

func TestRandom(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	// specify the schema (that is defined in schema.sql)
	schemaTables := []tableT{
		{name: sqlparser.NewTableName("emp")},
		{name: sqlparser.NewTableName("dept")},
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

	var queryCount int
	for time.Now().Before(endBy) && (!t.Failed() || testFailingQueries) {
		query := sqlparser.String(randomQuery(schemaTables, 3, 3))
		_, vtErr := mcmp.ExecAllowAndCompareError(query)

		// this assumes all queries are valid mysql queries
		if vtErr != nil {
			fmt.Println(query)
			fmt.Println(vtErr)

			if stopOnMustFixError {
				// EOF
				if sqlError, ok := vtErr.(*mysql.SQLError); ok && strings.Contains(sqlError.Message, "EOF") {
					break
				}
				// mismatched results
				if strings.Contains(vtErr.Error(), "results mismatched") {
					break
				}
			}

			// restart the mysql and vitess connections in case something bad happened
			closer()
			mcmp, closer = start(t)
		}
		queryCount++
	}
	fmt.Printf("Queries successfully executed: %d\n", queryCount)
}

func TestBuggyQueries(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*), count(*) from dept as tbl0, emp as tbl1 where tbl0.deptno = tbl1.deptno group by tbl1.empno order by tbl1.empno",
		`[[INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)]]`)
	//mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(tbl0.deptno) from dept as tbl0, emp as tbl1 group by tbl1.job order by tbl1.job limit 3",
	//	`[[INT64(8)] [INT64(16)] [INT64(12)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*) from emp as tbl0 group by tbl0.empno order by tbl0.empno",
		`[[INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct count(*), tbl0.loc from dept as tbl0 group by tbl0.loc",
		`[[INT64(1) VARCHAR("BOSTON")] [INT64(1) VARCHAR("CHICAGO")] [INT64(1) VARCHAR("DALLAS")] [INT64(1) VARCHAR("NEW YORK")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct count(*) from dept as tbl0 group by tbl0.loc",
		`[[INT64(1)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ sum(tbl1.comm) from emp as tbl0, emp as tbl1",
		`[[DECIMAL(30800)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.mgr, tbl1.mgr, count(*) from emp as tbl1 group by tbl1.mgr",
		`[[NULL NULL INT64(1)] [INT64(7566) INT64(7566) INT64(2)] [INT64(7698) INT64(7698) INT64(5)] [INT64(7782) INT64(7782) INT64(1)] [INT64(7788) INT64(7788) INT64(1)] [INT64(7839) INT64(7839) INT64(3)] [INT64(7902) INT64(7902) INT64(1)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.mgr, tbl1.mgr, count(*) from emp as tbl0, emp as tbl1 group by tbl1.mgr",
		`[[NULL NULL INT64(14)] [INT64(7566) INT64(7566) INT64(28)] [INT64(7698) INT64(7698) INT64(70)] [INT64(7782) INT64(7782) INT64(14)] [INT64(7788) INT64(7788) INT64(14)] [INT64(7839) INT64(7839) INT64(42)] [INT64(7902) INT64(7902) INT64(14)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*), count(tbl0.comm) from emp as tbl0, emp as tbl1 join dept as tbl2",
		`[[INT64(784) INT64(784) INT64(224)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0, dept as tbl1",
		`[[INT64(16) INT64(16)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0",
		`[[INT64(4)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ min(tbl0.loc) from dept as tbl0",
		`[[VARCHAR("BOSTON")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.empno, max(tbl1.job) from dept as tbl0, emp as tbl1 group by tbl1.empno",
		`[[INT64(7369) VARCHAR("CLERK")] [INT64(7499) VARCHAR("SALESMAN")] [INT64(7521) VARCHAR("SALESMAN")] [INT64(7566) VARCHAR("MANAGER")] [INT64(7654) VARCHAR("SALESMAN")] [INT64(7698) VARCHAR("MANAGER")] [INT64(7782) VARCHAR("MANAGER")] [INT64(7788) VARCHAR("ANALYST")] [INT64(7839) VARCHAR("PRESIDENT")] [INT64(7844) VARCHAR("SALESMAN")] [INT64(7876) VARCHAR("CLERK")] [INT64(7900) VARCHAR("CLERK")] [INT64(7902) VARCHAR("ANALYST")] [INT64(7934) VARCHAR("CLERK")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.ename, max(tbl0.comm) from emp as tbl0, emp as tbl1 group by tbl1.ename",
		`[[VARCHAR("ADAMS") INT64(1400)] [VARCHAR("ALLEN") INT64(1400)] [VARCHAR("BLAKE") INT64(1400)] [VARCHAR("CLARK") INT64(1400)] [VARCHAR("FORD") INT64(1400)] [VARCHAR("JAMES") INT64(1400)] [VARCHAR("JONES") INT64(1400)] [VARCHAR("KING") INT64(1400)] [VARCHAR("MARTIN") INT64(1400)] [VARCHAR("MILLER") INT64(1400)] [VARCHAR("SCOTT") INT64(1400)] [VARCHAR("SMITH") INT64(1400)] [VARCHAR("TURNER") INT64(1400)] [VARCHAR("WARD") INT64(1400)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl0.dname, tbl0.dname, min(tbl0.deptno) from dept as tbl0, dept as tbl1 group by tbl0.dname, tbl0.dname",
		`[[VARCHAR("ACCOUNTING") VARCHAR("ACCOUNTING") INT64(10)] [VARCHAR("OPERATIONS") VARCHAR("OPERATIONS") INT64(40)] [VARCHAR("RESEARCH") VARCHAR("RESEARCH") INT64(20)] [VARCHAR("SALES") VARCHAR("SALES") INT64(30)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl0.dname, min(tbl1.deptno) from dept as tbl0, dept as tbl1 group by tbl0.dname, tbl1.dname",
		`[[VARCHAR("ACCOUNTING") INT64(10)] [VARCHAR("ACCOUNTING") INT64(40)] [VARCHAR("ACCOUNTING") INT64(20)] [VARCHAR("ACCOUNTING") INT64(30)] [VARCHAR("OPERATIONS") INT64(10)] [VARCHAR("OPERATIONS") INT64(40)] [VARCHAR("OPERATIONS") INT64(20)] [VARCHAR("OPERATIONS") INT64(30)] [VARCHAR("RESEARCH") INT64(10)] [VARCHAR("RESEARCH") INT64(40)] [VARCHAR("RESEARCH") INT64(20)] [VARCHAR("RESEARCH") INT64(30)] [VARCHAR("SALES") INT64(10)] [VARCHAR("SALES") INT64(40)] [VARCHAR("SALES") INT64(20)] [VARCHAR("SALES") INT64(30)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ max(tbl0.hiredate) from emp as tbl0",
		`[[DATE("1983-01-12")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ min(tbl0.deptno) as caggr0, count(*) as caggr1 from dept as tbl0 left join dept as tbl1 on tbl1.loc = tbl1.dname",
		`[[INT64(10) INT64(4)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(tbl1.loc) as caggr0 from dept as tbl1 left join dept as tbl2 on tbl1.loc = tbl2.loc where (tbl2.deptno)",
		`[[INT64(4)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ sum(tbl1.ename), min(tbl0.empno) from emp as tbl0, emp as tbl1 left join dept as tbl2 on tbl1.job = tbl2.loc and tbl1.comm = tbl2.deptno where ('trout') and tbl0.deptno = tbl1.comm",
		`[[NULL NULL]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct max(tbl0.deptno), count(tbl0.job) from emp as tbl0, dept as tbl1 left join dept as tbl2 on tbl1.dname = tbl2.loc and tbl1.dname = tbl2.loc where (tbl2.loc) and tbl0.deptno = tbl1.deptno",
		`[[NULL INT64(0)]]`)

}
