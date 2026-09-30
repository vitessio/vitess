# VAL-B: bug validation for BUGS #5, #26, #28

Base: worktree HEAD e5e0091d44 (colldata and go/stats are unchanged since aa9ccf9). The cluster used the base
binaries in `/home/vt/bin`, BASE=40000, 2 shards (`-80`, `80-`), MySQL 8.0.46. No fixes are in the deliverable.
Patch: `VAL-B.patch` (3 Go tests, plus repro scripts and raw outputs under `perf-findings/VAL-B/`).

| Bug | Verdict | Priority |
|---|---|---|
| #5 PAD SPACE ignored in Collate/Hash | **REPRODUCED** end to end, and **TEST FAILS ON BASE** | keep P1. The fix plan needs a change, because the weight_string paths are wrong too. |
| #26 `Collation_binary.Hash` panics | **TEST FAILS ON BASE** (panic), no caller can reach it | keep P3, or lower to P4 |
| #28 `counters.String` `%q` gives invalid JSON | **TEST FAILS ON BASE** and **REPRODUCED** on a live cluster | P3 -> P2 suggested. Any client can trigger it with its user name. |

---

## #5 PAD SPACE: REPRODUCED

### PAD attributes (MySQL 8.0.46, `information_schema.COLLATIONS`)
PAD SPACE: `utf8mb4_general_ci`, `utf8mb3_general_ci`, `latin1_swedish_ci`, `latin1_bin`, `utf8mb4_unicode_ci`,
`utf8mb4_unicode_520_ci`, `utf8mb4_bin`, `utf8mb3_bin`, `gbk_chinese_ci`, `gbk_bin`, `ascii_general_ci`.
NO PAD: `utf8mb4_0900_ai_ci`, `utf8mb4_0900_bin`, `binary`.

### Unit test: `go/mysql/collations/colldata/padspace_test.go`
`TestPadSpaceCollateAndHash`: the expected signs are MySQL `STRCMP` results (`perf-findings/VAL-B/matrix.sh`).
The test checks `Collate` and, for equal pairs, `Hash`.
On base it **fails for all 11 PAD SPACE collations** and passes for the 2 NO PAD controls (95 assertion failures). Example for utf8mb4_general_ci:
```
Collate("a", "a ") ...  Hash("a") != Hash("a ") ... Collate("", "  ") ... Collate("A ", "a") ...
Collate("a\t", "a")     <- ordering is also wrong: MySQL says 'a\t' < 'a' (TAB < pad space), vtgate says >
```
The last line shows that the bug affects ordering as well as equality. The Collation interface doc (`collation.go:103-116`)
says that PAD SPACE only applies through `numCodepoints` (CHAR(n)), but every production caller passes 0.

### End to end (scripts in `perf-findings/VAL-B/`: setup.sh, load.sh, queries.sh)
Tables `t_{gen,lat,uni,bin,mb3,ai}` each have a `v VARCHAR(20)` column with the named collation and are sharded by `hash(id)`.
Rows: `(1,'a ')`@-80, `(4,'a')`@80-, `(6,'A ')`@80-, `(2,'b')`@-80, `(7,'b')`@80-. The join partner `t_gen2` has `(5,'a')`@-80 and `(8,'a  ')`@80-.
The same rows are loaded into schema `ref` on one mysqld, and every query runs against both (`result-tracked.txt`).

With the default vtgate (schema tracking on, so vtgate knows the column collation), the results are as follows. The general_ci, latin1_swedish_ci, unicode_ci and mb3_general_ci columns are identical, and utf8mb4_bin differs only where case is involved:

| Shape | vtgate | MySQL | Where it is evaluated |
|---|---|---|---|
| `GROUP BY v` count/sum | 3 groups (1,110,11000) | 2 groups (111,11000) | Ordered Aggregate merge, `Collate` |
| `COUNT(DISTINCT v)` | 3 | 2 | vtgate `count_distinct` |
| `UNION` (distinct) of rows on 2 shards | `a `,`a` | `a ` | Distinct probe table, `Hash` |
| `ORDER BY v, id` (scatter merge sort) | 4,1,6,2,7 | 1,4,6,2,7 | merge sort, `Collate` |
| `ORDER BY v DESC, id` | 2,7,1,6,4 | 2,7,1,4,6 | merge sort |
| filter on a derived aggregate (`where x.v='a'`) | 1;110 | 111 | the WHERE clause is pushed down, and the vtgate group merge splits the groups |
| `HAVING max(v) = 'a'` | empty | 111 | vtgate Filter, evalengine compare |
| `GROUP BY v ... ORDER BY c DESC LIMIT 1` | 2 | 3 | aggregate merge |
| `n>500, COUNT(DISTINCT v)` grouped | 2 | 1 | vtgate |
| **hash join** (`LIMIT` on both sides) | 1 row (4,5) | 6 rows | HashJoin, `Hash` |
| **hash left join** (`LIMIT` on RHS) | 5 rows: ids 1 and 6 wrongly get NULL, 5 matches missing | 8 rows | HashLeftJoin |
| join of 2 derived aggregates | (1,1) | (1,2) | aggregate merge |
| `SELECT _latin1'a' COLLATE latin1_swedish_ci = _latin1'a '` | 0 | 1 | evaluated by vtgate (Projection/SingleRow) |

Results that match MySQL: filters pushed to MySQL (`WHERE v='a'`), nested-loop joins (the predicate is sent to MySQL as a bind var),
`LEFT JOIN`, `IN (subquery)`, and a general_ci literal comparison (routed to a tablet). The NO PAD `t_ai` control matches MySQL on
every PAD-related shape. The hash join needs no hint: the planner picks it by itself when both sides have a LIMIT
(`route_planning.go:306-316`). The `/*vt+ ALLOW_HASH_JOIN */` hint alone did not produce a hash join.

### Are the weight_string paths correct? No
MySQL's `WEIGHT_STRING()` (no `AS CHAR(n)`) keeps trailing spaces for PAD SPACE collations:
`general_ci 'a'=0041, 'a '=00410020`, `latin1 'a '=4120`, `utf8mb4_bin 'a '=000061000020`, `unicode_ci 'a '=0E330209`.
To test this path, I started a second vtgate with `--schema-change-signal=false` (vtgate2.sh). vtgate then has no column types and
plans with `weight_string(v)`, for example `select ..., v, weight_string(v) from t_gen group by v, weight_string(v) order by v`.
The results (`result-untracked.txt`) show the same divergences:
GROUP BY (10;101 vs 111), COUNT(DISTINCT) 3 vs 2, ORDER BY merge 4,1,6 vs 1,4,6, derived aggregates, and grouped count(distinct).
The pushed-down `GROUP BY v, weight_string(v)` also splits groups inside a single shard.
**So the fix that BUGS.md suggests ("trim in Collate/Hash, leave WeightString alone") would fix only the typed paths.** Plans that use
weight_string (no schema tracking, views, untyped expressions) would stay wrong. They need PAD-aware weight comparison, for example
stripping trailing pad weights for PAD SPACE collations, or a different key.

### Conditions
The column uses a PAD SPACE collation, and values that differ only by trailing spaces (or characters below space, such as TAB) reach
vtgate from different shards, or meet in a vtgate-side operator. In practice this happens with CHAR-like data stored in VARCHAR,
user input with trailing blanks, and every 5.7-era `utf8mb4_general_ci` or `latin1` schema. The wrong results are silent.
Keep P1.

### Side findings from the same runs (not PAD related; worth separate triage)
- **S1 (likely P1, wrong results):** `SELECT COUNT(*) FROM (SELECT DISTINCT v FROM t) x` on a sharded table (v is not a vindex) is sent to
  every shard unchanged and the counts are summed (`Aggregate sum_count_star` over a scatter Route). It over-counts for any collation:
  the NO PAD table gives 5 against MySQL's 3.
- **S2 (wrong results):** vtgate types `CONCAT('[',v,']')` and `IFNULL(v,'x')` as `utf8mb4_0900_bin`. MySQL derives the column's
  `utf8mb4_general_ci`, so vtgate groups case-sensitively: `GROUP BY ifnull(v,'x')` returns 1,2,2 on vtgate and 2,3 on MySQL.
- **S3 (schema tracking off):** a cross-shard `UNION` merges different values (`'a '` and `'b'` come back as 1 row). The hash join
  returns extra binary weight_string columns and a near cross-product (it matches `'b'` with `'a'`).
- **S4 (test pitfall):** `metro.Metro128.Sum128()` finalizes in place, so a second call returns a different hash.

---

## #26 `Collation_binary.Hash` panic: TEST FAILS ON BASE, cannot be reached

Test: `go/mysql/collations/colldata/8bit_test.go` `TestCollationBinaryHashNumCodepointsLongerThanInput`
```
Panic value: runtime error: slice bounds out of range [:3] with capacity 2   Hash("ab", numCodepoints=3)
Panic value: runtime error: slice bounds out of range [:40] with capacity 2  Hash("ab", numCodepoints=40)
```
Reach: every production call of `Collation.Hash` passes `numCodepoints=0`: `evalengine/api_hash.go:197,205`,
`eval_enum.go:64`, `eval_set.go:69` and `eval_bytes.go:119`. No code path hashes with a column length or prefix, so the panic
cannot happen today. Two related problems:
- When `src` has spare capacity, `src[:n]` does not panic. It silently hashes bytes that are past the value.
- The interface says NO PAD collations ignore `numCodepoints`, but binary truncates when `n < len`.

I confirmed that the test turns green when the truncation is removed (scratch only). Keep P3, or lower it to P4.

---

## #28 `counters.String` `%q`: TEST FAILS ON BASE and REPRODUCED

Test: `go/stats/counters_test.go` `TestCountersStringIsValidJSONForControlCharacters`. It covers CountersWithSingleLabel,
CountersWithMultiLabels and CountersFuncWithMultiLabels (Gauges* embed the same String), and it also parses the whole
`expvar.Handler()` output:
```
invalid escape sequence `\a` in string   single: {"tab\there": 2, "bell\a": 3, "vt\v": 4, "del\x7f": 5, "nul\x00": 6, "bad\xffutf8": 100, "a\x01b": 1}
invalid escape sequence `\x` in string   multi: ...   func: ...
invalid escape sequence `\v` in string   (/debug/vars document)
```
`%q` is not valid JSON for 0x00-0x1f except `\n\t\r\b\f`, for 0x7f, and for invalid UTF-8. The test turns green when the key
is written with `json.Marshal` (scratch only).

Live cluster (`debugvars-repro.sh`): before the steps, all 3 `/debug/vars` parse with Python `json.loads`.
1. `CREATE TABLE` with the name ``t\x01x``, sent through vtgate, then `SELECT` from it. vtgate `/debug/vars` becomes invalid
   (`"DDL.sbtest_t\x01x": 1`), and so does the vttablet one (`QueryCounts` `"t\x01x.Select"`).
2. Connect to vtgate as user `bob\a` (`--mysql-auth-server-impl none`) and run a query. vtgate becomes invalid
   (`MysqlServerConnCountPerUser {"bob\a": 0}`), and so does the 80- vttablet (`UserTableQueryCount "t_gen.bob\a.Execute"`).

A single stats label makes the whole `/debug/vars` document unparseable for every JSON consumer (scrapers, e2e helpers, tooling)
until the process restarts. MySQL quoted identifiers allow U+0001..U+FFFF. User names are client supplied (with auth `none`, or
any user that exists), and so are caller IDs. Suggest P3 -> P2 because remote clients can trigger it easily. The impact is
monitoring only, with no effect on data.
