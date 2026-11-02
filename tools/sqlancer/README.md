# Testing vtgate with SQLancer

[SQLancer](https://github.com/sqlancer/sqlancer) generates random schemas, data and queries. `vitess-mode.patch`
lets its MySQL provider run against vtgate, in unsharded and sharded keyspaces, and adds a comparison oracle.
Together they find four kinds of problems:

- MySQL syntax that vtgate does not parse or does not support, especially in sharded keyspaces.
- SQL that vtgate rewrites into invalid SQL, which vttablet or MySQL then rejects.
- Wrong results, when vtgate's planning, rewriting or evaluation changes the meaning of a query.
- Queries that make vtgate panic, which closes the connection, or crash.

## Oracles

SQLancer's own oracles find logic bugs without a reference database. They compare queries that must return the
same result. For example, TLP_WHERE checks that `SELECT ... FROM t` returns the same rows as the `UNION ALL` of
the same query with `WHERE p`, `WHERE NOT p` and `WHERE p IS NULL`. These oracles miss bugs where vtgate is wrong
in the same way on both sides. Also, SQLancer's MySQL query generator produces neither joins nor aggregates.

The patch therefore adds the oracle `VITESS_DIFF`, which compares vtgate with MySQL:

- Every statement that succeeds on vtgate also runs on a database `ref_KEYSPACE` on a plain MySQL server
  (`--vitess-reference-url`). After each round of DDL and DML, SQLancer compares the schemas and the data of both,
  and in a sharded keyspace also whether every row is on the shard that its vindex value maps to.
- Its queries join up to three tables (inner, outer, cross and `STRAIGHT_JOIN`, often on the sharding key), and
  use aggregates, `GROUP BY`, `HAVING`, `DISTINCT`, `ORDER BY` with `LIMIT`, subqueries (`IN`, `EXISTS`, scalar,
  `ANY`/`ALL`), `UNION` and derived tables.
- It runs each query on vtgate and on MySQL and compares the results as multisets. Each problem is a line in
  `logs/vitess/findings.jsonl`, and the first ones of each kind get a reproduction file. The run continues after
  a finding.

## Patch options

- `--vitess` and `--vitess-keyspaces=ks1,ks2`: vtgate cannot `CREATE DATABASE` without a dbddl plugin, so each
  SQLancer thread reuses one existing keyspace and drops its tables before each round.
- `--vitess-sharded`: column `c0` of every table is an integer column and the primary vindex (`--vitess-vindex`,
  default `hash`). Every `INSERT` sets `c0`. After `CREATE TABLE`, SQLancer waits until vtgate's schema tracker
  knows the table. `ALTER TABLE` is not generated, because schema tracking is asynchronous.
- `--vitess-reference-url`: the JDBC URL of the MySQL server for `VITESS_DIFF`.
- `--vitess-lookup-keyspace=lk`: about half of the tables of a sharded keyspace also get a `consistent_lookup`
  vindex on `c1`, whose lookup table is in this unsharded keyspace.
- `--vitess-reference-tables true`, with `--vitess-vtctld` and `--vitess-vtctldclient`: some tables of a sharded
  keyspace become reference tables. vtgate does not support DML on a reference table without a source, so the
  harness writes their rows directly to every shard and to the MySQL reference, and SQLancer's own DML on them
  fails with a known error.
- `--vitess-ignore-errors-file`: errors matching these regexes count as expected. `known_limitations.txt` lists
  limitations and bugs that are already known, so that the oracles can find new ones.

The patch also avoids two known bugs when it generates tables (see the findings): the column attribute `STORAGE`
in sharded keyspaces, and a column with both `PRIMARY KEY` and `UNIQUE`.

## Running

1. Build SQLancer with the patch (tested on SQLancer commit `1d797ee`):

   ```sh
   git clone https://github.com/sqlancer/sqlancer && cd sqlancer
   git apply /path/to/vitess/tools/sqlancer/vitess-mode.patch
   mvn package -DskipTests
   export SQLANCER_JAR=$PWD/target/sqlancer-2.0.0.jar
   ```

   The patched sources also pass SQLancer's `mvn verify` checks (formatter, Checkstyle, PMD and SpotBugs).
   SpotBugs needs JDK 17, because its version cannot read class files of newer JDKs.

2. Start a vttestserver with the keyspaces `u0`, `u1` (unsharded), `s0`, `s1` (two shards each), `w0`, `w1`
   (four shards each) and `lk` (unsharded, for lookup tables). This needs
   the Vitess binaries in `$VTROOT/bin` and `mysqld` on the `PATH`. It must not run as root. The underlying
   mysqld also serves as the reference server, through the user `vt_dba_tcp` on port `BASE_PORT + 2`:

   ```sh
   VTROOT=... VTDATAROOT=/tmp/vtdataroot tools/sqlancer/start_cluster.sh
   ```

3. Run SQLancer. Each run prints the problems grouped by root error; the reproductions are in
   `<out-dir>/logs/`:

   ```sh
   REF='jdbc:mysql://127.0.0.1:33572/?user=vt_dba_tcp&useSSL=false&serverTimezone=UTC'
   export SQLANCER_OPTS='--num-queries 500'   # more database states per run
   tools/sqlancer/run.sh out/unsharded 600 u0,u1 --vitess-reference-url "$REF" --oracle VITESS_DIFF
   tools/sqlancer/run.sh out/sharded   600 s0,s1 --vitess-sharded --vitess-reference-url "$REF" --oracle VITESS_DIFF
   tools/sqlancer/run.sh out/tlp       300 s0,s1 --vitess-sharded --oracle TLP_WHERE
   # four shards, lookup vindexes and reference tables
   tools/sqlancer/run.sh out/wide 900 w0,w1 --vitess-sharded --vitess-reference-url "$REF" \
     --vitess-lookup-keyspace lk --vitess-reference-tables true \
     --vitess-vtctld 127.0.0.1:33571 --vitess-vtctldclient "$VTROOT/bin/vtctldclient" --oracle VITESS_DIFF
   ```

   SQLancer's other MySQL oracles also work: `PQS`, `DQE`, `DQP`, `EET`, `CERT` and `FUZZER`. In a sharded
   keyspace, `DQE` adds columns with `ALTER TABLE` and then queries them right away. vtgate does not track that
   schema change yet, so it reports `column not found`.

   Known bugs that occur often drown everything else. To look past them, patch them locally in the vtgate under
   test, and point `KNOWN_LIMITATIONS` at a copy of `known_limitations.txt` without their entries.

Limitations of this setup:

- vttestserver runs every tablet on one mysqld, so concurrent schema reloads sometimes deadlock on the shared
  `_vt` sidecar tables and vtgate misses a schema change. The patch re-triggers the reload in that case.
- A failing `CREATE INDEX` can leave the index on only some shards, because DDL is not atomic across shards.
  A later `DROP INDEX` then fails on the other shards.
- A unique key that does not contain the sharding key is only unique per shard. `VITESS_DIFF` reports the
  resulting differences as `divergence: ... Duplicate entry` and skips that round.

## Findings (November 2026, `main` at `bf549e2`)

About 2.5 hours of runs (more than 3 million statements and queries) found the following problems. Unless noted
otherwise, all examples are confirmed on an unmodified build of `main`, and a sharded example uses tables `t0`,
`t1`, `t2` with columns `c0` (the `hash` primary vindex) and `c1`, and a few rows each.

### Crashes and panics

0. **One query corrupts `TRUE` and `FALSE` for the whole vtgate process** (sharded, then everywhere). The compiled
   `IN` with a list that is not all literals (`In_slow` in `go/vt/vtgate/evalengine/compiler_asm.go`) pushes the
   shared singletons `evalBoolTrue` and `evalBoolFalse` onto the VM stack (`in.eval()`), and the next `OR`, `AND`,
   `XOR` or negation writes its result into that value in place. vtgate evaluates this expression itself because it
   aggregates across shards:
   ```sql
   SELECT (MAX(t0.c0) IN (2, MIN(t0.c0))) OR (COUNT(*) > 0) FROM t0;
   -- From then on, in every session and keyspace, until vtgate restarts:
   SELECT false;            -- 1
   SELECT false OR false;   -- 1, also in an unsharded keyspace
   ```
   `-(x IN (1, y))` turns `TRUE` into -1 the same way. In SQLancer runs, the corruption also made vtgate drop
   predicates such as `WHERE NULL IS TRUE`, so that queries returned all rows.

1. **A query can crash vtgate** (sharded). `Concatenate.parallelExec` runs each `UNION` source in a goroutine that
   does not recover panics, so a panic in a `UNION` kills the whole process. With bug 9 below, this query does:
   ```sql
   (SELECT t1.c0 AS a0 FROM t1 JOIN t0 ON 5 NOT IN (t1.c0, EXISTS (SELECT 1) < t0.c0)
     WHERE t0.c0 >= (SELECT MAX(sq.c0) FROM t0 AS sq))
   UNION (SELECT EXISTS (SELECT 1 WHERE FALSE) AS a0 FROM t0) ORDER BY 1 ASC
   -- panic: runtime error: index out of range [3] with length 2 (engine/concatenate.go:173)
   ```
   In a run against a vtgate with local workarounds for bugs 3, 4 and 8, a panic in the compiled `NOT`
   (`interface conversion: evalengine.eval is nil, not *evalengine.evalInt64`) crashed vtgate the same way.
   That one is not yet reproduced on `main`. With a random workload and the generator's avoidances, SQLancer
   still crashed an unmodified vtgate within minutes.

2. **Queries that make vtgate panic** (sharded; tables `t0`, `t1`, `t2` with columns `c0` to `c4`). vtgate
   recovers these panics, but closes the connection:
   - `unexpected argument type`: `SELECT LEAST(1, CASE WHEN t0.c0 THEN NULL END) AS a0 FROM t0 ORDER BY 1`
   - `index out of range [-1]`:
     `(SELECT DISTINCT IFNULL(EXISTS (SELECT 1), t0.c0) AS a0 FROM t0) UNION ALL (SELECT t0.c0 FROM t0)`
   - `index out of range [1] with length 1`:
     `(SELECT t0.c1 + 1 AS a0 FROM t0 WHERE EXISTS (SELECT 1 FROM t0 AS sq WHERE sq.c0 = t0.c1)) UNION ALL (SELECT t0.c0 FROM t0)`
   - `alias already used`:
     `SELECT COUNT(*), COUNT(DISTINCT d.a0), MIN(d.a0), MAX(d.a0) FROM (SELECT (((CASE 1901312329 WHEN t1.c0 THEN t0.c0 ELSE t0.c0 END)) && (LEAST(t1.c0, t0.c0))) != ((t1.c0) IS NOT UNKNOWN) AS a0, t0.c0 AS a1, t0.c0 AS a2 FROM t0, t1) AS d`
   - `index out of range [4] with length 3`:
     `SELECT d.a0, COUNT(*) FROM (SELECT CAST(-928735196 AS SIGNED) AS a0, SUM(DISTINCT t1.c0) AS a1 FROM t1, t2 WHERE t2.c0 = (SELECT COUNT(sq.c0) FROM t2 AS sq WHERE sq.c0 >= 1643139141) GROUP BY CAST(-928735196 AS SIGNED)) AS d GROUP BY d.a0 ORDER BY 1 DESC, 2 DESC LIMIT 1`
   - `invalid memory address or nil pointer dereference`:
     `SELECT (t0.c1 XOR 5) LIKE ('' NOT IN (t1.c0)) FROM t1 RIGHT JOIN t0 ON t1.c0 = t0.c1`
   - `index out of range [2] with length 2`:
     `SELECT (0.5 LIKE t1.c0) AND BIT_COUNT(NULL) FROM t0 JOIN t1 ON t1.c0 RIGHT JOIN t2 ON NULL`
   - Type confusion in the evalengine (`interface conversion: evalengine.eval is *evalengine.evalInt64, not
     ...`), when vtgate evaluates an expression over the columns of an outer join:
     `SELECT GREATEST(0.2, COALESCE(t0.c0, 0.5)) FROM t0 RIGHT JOIN t1 ON t0.c1 = t1.c0` and
     `SELECT t1.c0 NOT IN (t0.c0 XOR COALESCE(t1.c0, 'x')) FROM t0 LEFT JOIN t1 ON t0.c0`
   - `vexplain queries` on a query that hits bug 3 panics in `ExecuteLogger.Log` ("query not able to parse").

### Wrong results

3. **The formatter drops parentheses that MySQL needs** (unsharded and sharded). `precedenceFor` gives every
   `ComparisonExpr` (`=`, `<`, `IN`, `LIKE`, `REGEXP`, ...) and `IsExpr` the same precedence (`P11`). The
   formatter treats them as left-associative and omits the parentheses around a left operand. In MySQL's
   grammar, however, the left operand of `IN`, `LIKE`, `REGEXP` and `BETWEEN` is a `bit_expr`. Depending on
   the operators, vtgate sends one of two kinds of SQL to MySQL:
   - SQL with a different meaning:
     ```sql
     CREATE TABLE r (a INT); INSERT INTO r VALUES (2);
     SELECT a FROM r WHERE (a = 1) IN (0);
     -- MySQL returns 2. vtgate returns no row, because it sends `a = 1 in (0)`,
     -- which MySQL reads as `a = (1 IN (0))`.
     ```
   - SQL that Vitess's own parser rejects, so vttablet fails with `syntax error`. Examples:
     `(a IN (1)) IN (1)`, `(a LIKE 'x') LIKE 'y'`, `(a IS NULL) NOT IN (1)`, `(a IS NOT FALSE) >= b`,
     `(a IN (1)) BETWEEN 1 AND 2`.

4. **`NOT NOT x` becomes `x`** (unsharded and sharded). `rewriteNotExpr` in `go/vt/sqlparser/normalizer.go`
   removes a double negation everywhere, but `NOT NOT x` is `0` or `1`. `SELECT NOT NOT a FROM t` returns `5`
   for `a = 5`, where MySQL returns `1`. The simplification is only valid where just the truth value matters,
   as in `WHERE`.

5. **Aggregates over a cross-shard join** (sharded), with `t0` and `t1` holding the values 1, 2, 3 and 10, 20,
   30, 40 in `c0`, and an empty `t2`:
   - `DISTINCT` is ignored: `SELECT COUNT(DISTINCT t0.c0) FROM t1, t0` returns 12 instead of 3, and
     `SELECT SUM(DISTINCT t0.c0) FROM t1, t0` returns 24 instead of 6.
   - When the side without aggregates contributes no rows, vtgate still aggregates the other side:
     `SELECT SUM(t0.c0) FROM t2 CROSS JOIN t0` returns 0 instead of `NULL`,
     `SELECT SUM(t0.c0), MAX(t0.c0) FROM t1, t0 WHERE t1.c1 > 1000` returns 0 and 4 instead of `NULL` and `NULL`,
     and with `GROUP BY t0.c1` it returns a group with `COUNT(*) = 0` for every value of `t0.c1`, where MySQL
     returns no rows.
   - In a `RIGHT JOIN` that groups by the primary vindex column of the inner side, the `NULL` group is split by
     shard: `SELECT t1.c0, COUNT(*) FROM t1 RIGHT JOIN t0 ON t1.c0 = t0.c0 GROUP BY t1.c0` returns two `NULL`
     groups.
   - vtgate adds the values of `SUM(DISTINCT float_column)` with float instead of double precision.

6. **More wrong results of sharded queries**, with `t0.c0` holding 1, 2, 3, 4 and `t1.c0` holding 1, 5:
   - **A positional `ORDER BY` on a `UNION` is ignored.**
     `(SELECT t1.c0 FROM t1) UNION (SELECT t0.c0 FROM t0) ORDER BY 1 DESC` returns the rows unordered, and with
     `LIMIT 1` it returns 4 instead of 5: vtgate sends `select ... union select ... limit :__upper_limit`
     without an `ORDER BY` to the shards. `ORDER BY c0` works without a `LIMIT`.
   - **A `LIMIT` on a `UNION` is pushed to the shards without its `ORDER BY`.**
     `(SELECT t0.c0 FROM t0) UNION (SELECT t1.c0 FROM t1) ORDER BY c0 DESC LIMIT 3` returns 4, 3, 2 instead of
     5, 4, 3. `UNION ALL` is not affected.
   - **`NOT IN` restricts the outer query to the shards of the subquery.**
     `SELECT COUNT(*) FROM t0 WHERE c0 NOT IN (SELECT sq.c0 FROM t0 AS sq WHERE sq.c0 = 3)` returns 2 instead
     of 3: vtgate merges the subquery, which goes to one shard, and the outer query into one route.
   - **Subqueries without a table lose `DISTINCT` and `HAVING`.**
     `SELECT d.a0 FROM (SELECT DISTINCT EXISTS (SELECT 1) AS a0 FROM t0) AS d` returns one row per shard, and
     `SELECT NULL, AVG(c0) FROM t0 GROUP BY NULL HAVING SUM(EXISTS (SELECT 1)) <= NULL` returns a row, where
     MySQL returns none.
   - **`ANY` and `ALL` are evaluated per shard.** vtgate sends `c0 > ALL (SELECT c0 FROM t1)` unchanged to each
     shard, so each shard compares with its own rows only. `SELECT c0 FROM t0 WHERE c0 = ALL (SELECT c0 FROM t0)`
     returns 4 instead of no row, and `c0 > ANY (SELECT c0 FROM t1)` misses 4.
   - **A `LIMIT` is pushed below a scalar aggregation.** `SELECT SUM(DISTINCT c1) FROM t0 ORDER BY 1 LIMIT 1`
     sums only the first value (10 instead of 60), because the shards run
     `select c1 from t0 group by c1 order by c1 asc limit :__upper_limit`.
   - **A `UNION` of numbers and strings is sorted as numbers.** The column type of
     `(SELECT c0 AS a0 FROM t0) UNION (SELECT '5' AS a0 FROM t0)` is a string, so MySQL sorts `ORDER BY a0 DESC`
     as 9, 5, 2, 10, 1 (for `c0` values 1, 2, 9, 10). vtgate returns 10, 9, 2, 1, 5.
   - When vtgate groups the rows of a derived table itself, values of a `ZEROFILL` column lose their padding
     (`7` instead of `000000000007`).
   - **A `UNION` clamps values to the type of a decimal literal.** When vtgate combines the branches itself,
     `(SELECT t0.c0 AS a0 FROM t0 CROSS JOIN t2 WHERE t0.c0 > 1000 OR t0.c0 < -1000) UNION (SELECT 0.19293754110062755 AS a0 FROM t2)`
     returns `999.99999999999999999` and `-999.99999999999999999` instead of 1605664068 and -1551415994: it converts
     the integers to the literal's `DECIMAL(18,17)` instead of a type that holds both.
   - **A `COUNT` subquery that goes to no shard returns `NULL`.** `SELECT (SELECT COUNT(sq.c0) FROM t0 AS sq
     WHERE sq.c0 = '') FROM t2` returns `NULL` instead of 0, because `''` maps to no shard of the `hash` vindex.
     In a `WHERE` clause, the comparison with `NULL` then removes every row. The same `COUNT` alone returns 0.
   - **`x OR x` and `x AND x` become `x` inside a predicate.** `SELECT c0 FROM t1 WHERE CAST(c0 OR c0 AS SIGNED)
     IN (c0)` returns every row instead of the one with `c0 = 1`, because the predicate rewriting simplifies
     `c0 OR c0` to `c0` also where its value is used, not just its truth value. Unsharded keyspaces are not
     affected, because vtgate sends their predicates unchanged.

7. **Outer joins on the sharding key** (sharded), with `t0.c0` holding 1, 2, `t1.c0` holding 1, 2, 3, 4 and
   `t2.c0` holding 2, 3:
   - `SELECT t1.c0 FROM t0 LEFT JOIN t1 ON NULL = 'q'` returns no rows instead of the two rows of `t0` with
     `NULL`. (`ON 1 = 0` works.)
   - `SELECT t0.c1 FROM t0 JOIN t2 ON t0.c0 = t2.c0 RIGHT JOIN t1 ON t0.c0 = t1.c0` returns 1 row instead of 4.
   - `SELECT DISTINCT t0.c0 FROM t1 LEFT JOIN t0 ON t1.c0 = t0.c0` returns `NULL` once per shard, and a derived
     table over it gets a `NULL` group per shard.
   - **An outer join of a reference table returns its NULL-extended rows once per shard.** With `t1` a reference
     table holding 1 and 100, and `t0` holding 1 to 6 in a keyspace with 4 shards,
     `SELECT t1.c0, t0.c0 FROM t1 LEFT JOIN t0 ON t1.c0 = t0.c0` returns 8 rows instead of 2: vtgate runs the
     whole join on every shard.

8. **A subquery whose `WHERE` is false empties the whole query** (sharded). These queries return no rows, even
   though `t` has rows:
   ```sql
   SELECT c0 FROM t WHERE NOT EXISTS (SELECT 1 WHERE FALSE);
   SELECT c0 FROM t WHERE NOT (EXISTS (SELECT 1 WHERE FALSE));
   SELECT c0, EXISTS (SELECT 1 WHERE FALSE) FROM t;
   SELECT c0 FROM t ORDER BY EXISTS (SELECT 1 WHERE FALSE);
   ```
   The plan is a `Route` with variant `None`. The cause is in `mergeSubqueryInputs`
   (`go/vt/vtgate/planbuilder/operators/subquery_planning.go`): the case `inner == none && sameKeyspace`
   merges the subquery into the outer route with the inner's `NoneRouting`. That is correct only for a
   subquery that filters the outer rows, such as `WHERE EXISTS (...)`. The form `NOT (EXISTS (...))` takes
   another path: it still returns no rows when that case is removed.

9. **A cross-shard join predicate with a subquery goes to the wrong side** (sharded). A predicate that uses
   columns of both tables and a subquery is not evaluated where both are available:
   - `SELECT t1.c0, t0.c0 FROM t1 JOIN t0 ON t1.c0 < t0.c0 + EXISTS (SELECT 1)` drops the predicate and
     returns the cross product (9 instead of 6 rows for 3 rows each).
   - `SELECT COUNT(*) FROM t1, t0 WHERE t1.c0 < t0.c0 + (SELECT MAX(c0) FROM t0 AS sq)` sends the predicate to
     the route of `t1`, which fails with `Unknown column 't0.c0'`.
   - With a `UNION`, this crashes vtgate (bug 1).

10. **Rows land on the wrong shard** (sharded). vtgate computes the keyspace ID from the value in the statement,
   before MySQL converts it to the column type. When MySQL stores a different value, for example because
   `INSERT IGNORE` clamps an out-of-range value, the row is on the wrong shard:
   ```sql
   CREATE TABLE z (c0 INT UNSIGNED, c1 INT);  -- primary vindex hash(c0)
   INSERT IGNORE INTO z (c0, c1) VALUES (-1,1),(-2,2),(-3,3),(-4,4),(-5,5),(-6,6),(-7,7),(-8,8),(0,0);
   SELECT COUNT(*) FROM z WHERE c0 = 0;         -- 7 instead of 9
   SELECT c0, COUNT(*) FROM z GROUP BY c0;      -- two groups for 0
   ```
   A unique key on `c0` does not prevent duplicates in this case either.

11. **`PRIMARY KEY UNIQUE` on a column loses the primary key** (unsharded and sharded).
   `CREATE TABLE t (c0 INT PRIMARY KEY UNIQUE)` creates a nullable `c0` with only a `UNIQUE KEY`, because
   `ColumnTypeOptions.KeyOpt` holds only one key option and the last one wins.

12. **A float literal as a bind variable changes `CAST`** (unsharded and sharded). vtgate sends the literal
    `1.7976931348623157E308` as `CAST(:vtg1 AS DOUBLE)`. `SELECT CAST(1.7976931348623157E308 AS SIGNED)`
    returns 9223372036854775807 on MySQL, but fails through vtgate with `BIGINT value is out of range`, and in
    a `WHERE` clause the row disappears.

### Valid queries that fail

13. **Derived tables and `UNION`s over cross-shard joins** (sharded). vtgate generates invalid SQL or reports an
    internal error:
    - `SELECT d.a0, COUNT(*) FROM (SELECT t0.c0 + t1.c0 AS a0 FROM t0, t1) AS d GROUP BY d.a0` fails with
      `missing bind var t0_c0`.
    - `SELECT MIN(d.a0) FROM (SELECT t1.c0 AS a0, t0.c0 AS a1, t0.c0 AS a2 FROM t0, t1) AS d` fails with
      `Unknown column 't0.c0'`.
    - `(SELECT t0.c1 FROM t0 WHERE EXISTS (SELECT 1 FROM t0 AS sq WHERE sq.c1 = t0.c0)) UNION ALL (SELECT t0.c0 FROM t0)`
      and `(SELECT t1.c0 AS a0 FROM t1 JOIN t0 ON t1.c0 + t0.c0 > 0 WHERE t0.c0 >= (SELECT MAX(sq.c0) FROM t0 AS sq)) UNION (SELECT 1 FROM t0)`
      fail with `The used SELECT statements have a different number of columns`.
    - `(SELECT DISTINCT EXISTS (SELECT 1) AS a0 FROM t0) UNION ALL (SELECT t0.c0 FROM t0)` fails with
      `VT13001: [BUG] argument offsets did not line up for UNION`.
    - `SELECT SUM(DISTINCT 5) FROM t0` fails with `Unknown column`.
    - `SELECT d.a0, COUNT(*) FROM (SELECT t1.c2 AS a0, AVG(t1.c0) AS a1 FROM t0 JOIN t1 ON t0.c0 = t1.c0 GROUP BY t1.c2) AS d GROUP BY d.a0`
      fails with `VT13001: [BUG] did not expect to add group by here: c2`, although the join is on the sharding
      key.
    - `SELECT d.a0, COUNT(*) FROM (SELECT 1 AS a0, t0.c0 AS a1 FROM t0 WHERE t0.c0 = (SELECT COUNT(sq.c0) FROM t0 AS sq)) AS d GROUP BY d.a0`
      fails with `Unknown column 't0.c0' in 'where clause'`.

### Schema problems

14. **The column attribute `STORAGE {DISK|MEMORY}` is not parsed.** vtgate passes such a `CREATE TABLE`
    through as partial DDL. The schema tracker cannot parse the `SHOW CREATE TABLE` output
    (`/*!50606 STORAGE MEMORY */`) and skips the table. In a sharded keyspace, `INSERT` without a column list,
    `SELECT *` in a join and similar queries on that table then fail with `VT09004` or
    `VT09015: schema tracking required`.

15. **vtgate rewrites invalid DDL into valid DDL.** MySQL rejects `CREATE INDEX i0 USING HASH ON h (c0, c1 ASC)`
    with `Incorrect usage of spatial/fulltext/hash index and explicit index order`. vtgate drops `USING HASH`
    and creates a B-tree index.

### Unsupported syntax and features

These are valid MySQL statements that vtgate rejects:

- Parser: `INSERT {LOW_PRIORITY|HIGH_PRIORITY|DELAYED}`, `REPLACE {LOW_PRIORITY|DELAYED}`,
  `DELETE LOW_PRIORITY`, `DELETE QUICK IGNORE`, `CHECK TABLE`, `CHECKSUM TABLE`, `ANALYZE TABLE` with several
  tables, `ANALYZE TABLE ... {UPDATE|DROP} HISTOGRAM`, `SET SESSION sql_buffer_result = ...`.
- `DELETE QUICK FROM t` fails with `VT03003: unknown table 'QUICK' in MULTI DELETE`.
- Session variables that vtgate does not know (for example `histogram_generation_max_mem_size` and
  `internal_tmp_mem_storage_engine`) or does not support as a system setting (for example
  `max_sp_recursion_depth`, `rbr_exec_mode`, `myisam_sort_buffer_size` and `sql_log_off`).
- Sharded keyspaces:
  - `REPLACE INTO`, `UPDATE` of a primary vindex column with the `hash` vindex, and
    `UPDATE ... SET vindex_col = DEFAULT`.
  - `BIT_AND`, `BIT_OR` and `BIT_XOR` in any scatter query (`VT12001: unsupported: in scatter query:
    aggregation function`), and `AVG(DISTINCT ...)`.
  - A correlated subquery other than `EXISTS`, an `EXISTS` subquery that cannot be merged inside an `OR`
    (`unmergable subquery can not be inside complex expression`), and a subquery in an outer join predicate.
  - DML on a reference table without a source. The errors are inconsistent: `INSERT` fails with
    `table 'rf' does not have a primary vindex`, `UPDATE` with the same message as `VT09001`, and `DELETE` with
    `unsupported opcode: Reference`.
