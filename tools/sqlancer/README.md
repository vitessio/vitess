# Testing vtgate with SQLancer

[SQLancer](https://github.com/sqlancer/sqlancer) generates random schemas, data and queries, and finds logic bugs
without a reference database. Its *oracles* compare queries that must return the same result. For example,
TLP_WHERE checks that `SELECT ... FROM t` returns the same rows as the `UNION ALL` of the same query with
`WHERE p`, `WHERE NOT p` and `WHERE p IS NULL`. Running SQLancer through vtgate therefore finds three kinds
of problems:

- MySQL syntax that vtgate does not parse or does not support, especially in sharded keyspaces.
- SQL that vtgate rewrites into invalid SQL, which vttablet or MySQL then rejects.
- Wrong results, when vtgate's planning or rewriting changes the meaning of a query.

SQLancer's MySQL provider needs a few changes to run against Vitess, which `vitess-mode.patch` adds:

- `--vitess` and `--vitess-keyspaces=ks1,ks2`: vtgate cannot `CREATE DATABASE` without a dbddl plugin, so each
  SQLancer thread reuses one existing keyspace and drops its tables before each round.
- `--vitess-sharded`: column `c0` of every table is an `INT` and the primary vindex (`--vitess-vindex`, default
  `hash`). Every `INSERT` sets `c0`. After `CREATE TABLE`, SQLancer waits until vtgate's schema tracker knows
  the table. `ALTER TABLE` is not generated, because schema tracking is asynchronous.
- `--vitess-ignore-errors-file`: errors matching these regexes count as expected. `known_limitations.txt` lists
  limitations and bugs that are already known, so that the oracles can find new ones.

## Running

1. Build SQLancer with the patch (tested on SQLancer commit `1d797ee`):

   ```sh
   git clone https://github.com/sqlancer/sqlancer && cd sqlancer
   git apply /path/to/vitess/tools/sqlancer/vitess-mode.patch
   mvn package -DskipTests
   export SQLANCER_JAR=$PWD/target/sqlancer-*.jar
   ```

2. Start a vttestserver with the keyspaces `u0`, `u1` (unsharded) and `s0`, `s1` (two shards each). This needs
   the Vitess binaries in `$VTROOT/bin` and `mysqld` on the `PATH`. It must not run as root:

   ```sh
   VTROOT=... VTDATAROOT=/tmp/vtdataroot tools/sqlancer/start_cluster.sh
   ```

3. Run SQLancer. Each run prints the failures grouped by root error. The full reproduction logs are in
   `<out-dir>/logs/mysql/`:

   ```sh
   tools/sqlancer/run.sh out/unsharded 300 u0,u1 --oracle TLP_WHERE
   tools/sqlancer/run.sh out/sharded   300 s0,s1 --vitess-sharded --oracle TLP_WHERE
   ```

   The other MySQL oracles also work: `PQS`, `DQE`, `DQP`, `EET`, `CERT` and `FUZZER`. In a sharded keyspace,
   `DQE` adds columns with `ALTER TABLE` and then queries them right away. vtgate does not track that schema
   change yet, so it reports `column not found`.

Limitations of this setup:

- vttestserver runs every tablet on one mysqld, so concurrent schema reloads sometimes deadlock on the shared
  `_vt` sidecar tables and vtgate misses a schema change. The patch re-triggers the reload in that case.
- A failing `CREATE INDEX` can leave the index on only some shards, because DDL is not atomic across shards.
  A later `DROP INDEX` then fails on the other shards.

## Findings (November 2026, `main` at `bf549e2`)

About 25 minutes of runs (more than 1.5 million statements) found these problems.

### Wrong results

1. **The formatter drops parentheses that MySQL needs** (unsharded and sharded). `precedenceFor` gives every
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
     `vexplain queries` with such a query panics in `ExecuteLogger.Log` ("query not able to parse"), and
     vtgate closes the connection.

2. **A subquery whose `WHERE` is false empties the whole query** (sharded only). In a sharded keyspace, these
   queries return no rows, even though `t` has rows:
   ```sql
   SELECT c0 FROM t WHERE NOT EXISTS (SELECT 1 WHERE FALSE);
   SELECT c0, EXISTS (SELECT 1 WHERE FALSE) FROM t;
   SELECT c0 FROM t ORDER BY EXISTS (SELECT 1 WHERE FALSE);
   ```
   The plan is a `Route` with variant `None`. The cause is in `mergeSubqueryInputs`
   (`go/vt/vtgate/planbuilder/operators/subquery_planning.go`): the case `inner == none && sameKeyspace`
   merges the subquery into the outer route with the inner's `NoneRouting`. That is correct only for a
   subquery that filters the outer rows, such as `WHERE EXISTS (...)`.

3. **`PRIMARY KEY UNIQUE` on a column loses the primary key** (unsharded and sharded).
   `CREATE TABLE t (c0 INT PRIMARY KEY UNIQUE)` creates a nullable `c0` with only a `UNIQUE KEY`, because
   `ColumnTypeOptions.KeyOpt` holds only one key option and the last one wins.

### Tables that schema tracking cannot handle

4. **The column attribute `STORAGE {DISK|MEMORY}` is not parsed.** vtgate passes such a `CREATE TABLE` through
   as partial DDL. The schema tracker cannot parse the `SHOW CREATE TABLE` output (`/*!50606 STORAGE MEMORY */`)
   and skips the table. In a sharded keyspace, `INSERT` without a column list, `SELECT *` in a join and similar
   queries on that table then fail with `VT09004` or `VT09015: schema tracking required`.

### Unsupported syntax and features

These are valid MySQL statements that vtgate rejects:

- Parser: `INSERT {LOW_PRIORITY|HIGH_PRIORITY|DELAYED}`, `REPLACE {LOW_PRIORITY|DELAYED}`,
  `DELETE LOW_PRIORITY`, `DELETE QUICK IGNORE`, `CHECK TABLE`, `CHECKSUM TABLE`,
  `ANALYZE TABLE ... {UPDATE|DROP} HISTOGRAM`, `SET SESSION sql_buffer_result = ...`.
- `DELETE QUICK FROM t` fails with `VT03003: unknown table 'QUICK' in MULTI DELETE`.
- Session variables that vtgate does not know (for example `histogram_generation_max_mem_size` and
  `internal_tmp_mem_storage_engine`) or does not support as a system setting (for example
  `max_sp_recursion_depth`, `rbr_exec_mode`, `myisam_sort_buffer_size` and `sql_log_off`).
- Sharded keyspaces: `REPLACE INTO`, `UPDATE` of a primary vindex column with the `hash` vindex, and
  `UPDATE ... SET vindex_col = DEFAULT`.
