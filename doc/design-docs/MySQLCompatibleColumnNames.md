# MySQL-compatible result-set column names

Status: implemented on this branch
Scope: vtgate (parser, executor, MySQL protocol layer). vttablet needs no changes.

## 1. Summary

When a `SELECT` item has no alias, MySQL names the result column after the item's *original query text*. A few item kinds are exceptions and are named by their own value.
Vitess re-parses, normalises, re-prints, splits and partly evaluates queries. At each of those steps it can end up with a different name.

We ran 1,914 queries against MySQL 8.4.6 and against a local Vitess cluster with a sharded and an unsharded keyspace. Each query went through the text protocol, `COM_STMT_PREPARE` metadata and `COM_STMT_EXECUTE` metadata:

- **921 queries (48 %) return at least one different column name.**
- **244 more fail on only one side.** Most of these fail because the name vtgate uses internally does not match the name MySQL uses.

Some examples, from the base commit:

| query | MySQL | Vitess before |
|---|---|---|
| `select 1` | `1` | `:vtg1 /* INT64 */` |
| `select 1+1` | `1+1` | `:vtg1 /* INT64 */ + :vtg1 /* INT64 */` |
| `select COUNT(*) from unsharded_t` | `COUNT(*)` | `count(*)` |
| `select sUm(id) from sharded_t` (run after `select SUM( id ) …`) | `sUm(id)` | `SUM( id )` (plan-cache poisoning) |
| prepared `select ? from t` | `?` | `NULL` at prepare time, then the bound value (`1`) at execute time |
| ``select `1+1` from (select 1+1) x`` | `1+1` | error: column not found |
| `with c as (select 1+1) select * from c` | `1+1` | error: unknown column `:vtg1 /* INT64 */ + …` |

The design has three parts:

1. **The parser records how each select item was spelled**, and `MySQLColumnName` applies MySQL's naming rules (§2) to it.
2. **After normalising a statement, vtgate aliases each select item whose column MySQL would name differently from the SQL that vtgate sends.** The alias carries MySQL's name, so every later step (the planner, the SQL sent to MySQL, vtgate's own projections) propagates the right name without knowing about naming.
3. **Names that contain literals are not aliased.** Literals become bind variables, and statements that differ only in their literals share a plan. Aliasing them would split the plan cache per value, so vtgate gives these columns their names in each result instead.

The session's charsets take part in naming (§4.5), and the MySQL protocol server converts names to `character_set_results`.

## 2. How MySQL names columns

This comes from reading the `mysql-server` 8.0, 8.4 and trunk sources, and was confirmed on MySQL 8.0.43 and 8.4.6 with a corpus of 1,571 queries.

The naming code is identical in 8.0, 8.4 and trunk. The only differences are the version-comment digits (§2.6), and `TRUE`/`FALSE` in 8.0.11–8.0.16.

### 2.1 Where the name of a select item comes from

The grammar rule is `select_item: expr select_alias`, which becomes `PTI_expr_with_alias(@$, $1, @1.cpp, alias)` (`sql_yacc.yy`, `parse_tree_items.cc:350`). The name is chosen in this order:

1. **Explicit alias** (`AS x`, bare `x`, `AS 'x'`, `AS "x"`).
   - The name is the alias as lexed: unquoted, with escapes processed.
   - Leading non-graphic bytes are stripped and warning 1466 is raised. Trailing spaces are kept.
   - The name is capped at 256 bytes of utf8mb3 (§2.3).
   - `AS ''` and ``AS `` `` are valid and give an **empty** name. That is different from having no alias.
   - A 4-byte character (for example emoji) behaves differently depending on quoting:
     - in a *string-quoted* alias (`AS '😀'`) it raises error 3854;
     - in a backticked or bare alias it becomes `?`.
2. **Items that name themselves.** For these the raw text is ignored:
   - **Column reference**: the identifier as typed, unquoted.
     - `t.ID` → `ID`, `` `a``b` `` → ``a`b``, `` ` lead` `` → ` lead`.
     - It is **not** stripped, converted or truncated.
   - **Text literal**: the *value* of the **first** fragment, after escape processing.
     - `'a''b'` → `a'b`, `'x' 'y'` → `x`, `N'n'` → `n`.
     - `_latin1'é'` → the value bytes read as latin1 and converted: `Ã©`.
     - With `NO_BACKSLASH_ESCAPES`, backslashes are kept.
   - **`NULL`**, however it is spelled → `NULL`.
   - **Numbers**: the token exactly as typed: `01`, `1.50`, `1E3`, `.5`, `1.`.
     - Truncation depends on the token type (§2.3).
   - **`?`** → `?`.
   - **`NAME_CONST('n', v)`** → `n`.
   - **Wrappers that return the inner item unchanged**: `( expr )` and unary `+`.
     - `(1)`, `((1))` and `+1` → `1`; `(t.id)` → `id`; `+'a'` → `a`.
     - `(1+2)`, `+(1+2)`, `(-1)`, `(TRUE)` and `(0x41)` keep the raw text, because the inner item has no name of its own.
3. **Everything else**: the query text from the first byte of the first token to the last byte of the last token, read from the lexer's pre-processed (*cpp*) buffer.
   - This covers functions, operators, `-x`, `TRUE`/`FALSE`, hex, bit and temporal literals, `@v`, `@@x`, subqueries, `EXISTS`, `CASE`, `CAST`, `->`, window functions and aggregates.
   - Leading and trailing whitespace and comments are left out.
   - Everything in between is kept verbatim: whitespace, newlines, `/* */`, `/*+ */`, `--` and `#` comments, redundant parentheses, keyword case, and escapes inside quoted strings.

Implicit names can be *referenced* like aliases.
- `select id+1 from t order by `id+1``, ``group by `1-id` `` and ``having `1-id` < 0`` all resolve to the select item.
- The same applies to outer references into derived tables and CTEs.
- Column names compare case-insensitively.

### 2.2 The cpp buffer (versioned comments)

- `/*!` and `/*!NNNNN` markers and their closing `*/` are **removed**; the text inside is kept.
  - `1 /*!+ 2*/` → `1 + 2`
  - `1 /*!80000 +1 */` → `1  +1`
- A comment whose version is newer than the server is removed **entirely**.
  - `1 /*!99999 + 2*/` → `1`
  - `1 /*!99999 +1 */ + 1` → `1  + 1`
- After the closing `*/` of a comment that *is* expanded, one space is inserted when the next raw character and the previous cpp character are both non-whitespace.
  - `1/*!+2*/+3` → `1+2 +3`
  - `/*!1+*/2` → `1+ 2`

### 2.3 Normalising the name (`Name_string::copy`, `item.cc:1368`)

This step applies to raw-text names, text literals, `ULONGLONG`/`FLOAT` tokens and aliases. It does **not** apply to column references, and does not apply to `NUM`, `LONG_NUM` or `DECIMAL_NUM` tokens.

1. **Strip leading bytes that are not graphic in the source charset.**
   - utf8: bytes `≤0x20` and `0x7F`.
   - binary: also every byte `≥0x80`.
   - latin1 has its own table.
   - Examples: `'  abc'` → `abc`, `'\ta'` → `a`.
2. **Convert to utf8mb3.**
   - A character that has no utf8mb3 form (for example emoji) becomes `?`.
   - An invalid byte becomes `?`.
   - An incomplete multibyte sequence at the end stops the conversion silently.
3. **Truncate.**

   | source | limit |
   |---|---|
   | raw text and text literals, source charset **not** utf8mb3 (the utf8mb4 default) | ≤255 bytes, cut on a character boundary |
   | source charset utf8mb3 (a utf8mb3 client, `N''`), aliases, `ULONGLONG_NUM` (`Item_uint`) and `FLOAT_NUM` tokens | 256 bytes, a raw byte cut |
   | `NUM`, `LONG_NUM`, `DECIMAL_NUM` tokens | **never truncated**; a 401-digit literal gives a 401-byte name |
   | column references | never truncated |

   A raw 256-byte cut can split a character. That split is visible only when `character_set_results` is utf8mb3, NULL or binary. With utf8mb4 results the incomplete trailing character is dropped when the name is converted, so the name is ≤256 bytes.
4. **On the wire** (`protocol_classic.cc:3166`):
   - the name is cut at the first NUL byte (`'a\0b'` → `a`);
   - it is converted to `character_set_results`, unless that is NULL or binary.

### 2.4 Structure

| construct | rule |
|---|---|
| `*`, `t.*`, `TABLE t` | column names as defined in the table |
| NATURAL / USING | the joined columns come first, named from the left table (the right table for RIGHT JOIN) |
| UNION / INTERSECT / EXCEPT | names come from the **first query block**, even if it is parenthesised or nested; aliases in later blocks are ignored |
| `VALUES ROW(...)` | `column_0`, `column_1`, … |
| derived table / CTE with a column list `d(a,b)` | the list |
| derived table / CTE / view without a column list | the names of the inner select items, by the rules above |
| duplicate names in a derived table / CTE | error 1060. The comparison ignores case, but trailing spaces count: `(select 'x', 'x ') t` is valid |
| **column ref into a *merged* derived table, CTE or view** | the column name **as defined** there: `select ID from (select id from t) d` → `id`; `select ID from v` → `id`. `information_schema` tables are merged views, so `select table_name …` → `TABLE_NAME` |
| column ref into a *materialized* one | the name **as typed**: `select ID from (select id from t limit 1) d` → `ID` |
| `CREATE VIEW` | an invalid name (empty, ending in a space, >64 characters) becomes `Name_exp_<pos>`; duplicates become `Name_exp_<name>` or `Name_exp_<k>_<name>` |
| `CREATE TABLE … SELECT` | an invalid name raises error 1166; duplicates raise error 1060 |

A derived table, CTE or view is **materialized** instead of merged when any of these hold (`sql_resolver.cc:3344`, `sql_lex.cc:3962`):
- it is a set operation (UNION etc.);
- it is grouped or aggregated;
- it has `HAVING`, `DISTINCT`, `LIMIT` or window functions;
- it has no tables;
- it is `ALGORITHM=TEMPTABLE`, uses the `NO_MERGE` hint, or `optimizer_switch` has `derived_merge=off` (the session or global value);
- it assigns user variables;
- it has a **correlated** subquery in its select list (a non-correlated one still merges);
- the outer statement cannot merge it.

### 2.5 Prepared statements

- Names come from the prepared text, with `?` kept as `?` (for example `? + 1`).
- The metadata from `COM_STMT_PREPARE` and from `COM_STMT_EXECUTE` is identical.
- SQL-level `PREPARE` / `EXECUTE` gives the same names.

### 2.6 Version and mode differences

- **6-digit version comments.** `/*!MMmmdd` followed by whitespace is accepted from 8.1 on.
  - On 8.0 it is a syntax error.
  - Without the trailing whitespace it is also a syntax error on 8.4 (`/*!080000+1`).
  - The Vitess tokenizer used to read only 5 digits.
- **5-digit versions** such as `/*!80400 x*/` are expanded on 8.4 and removed on 8.0, so the name differs between the two.
- **`ANSI_QUOTES`** makes `"x"` an identifier.
- **`NO_BACKSLASH_ESCAPES`** changes the values of string literals.
- **`PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE` and `IGNORE_SPACE`** do not change names; the raw text is kept (`count (*)`).

## 3. Why Vitess diverged

vitessio/vitess#19675 (v25) added `AliasedExpr.InputExpression`, the raw text of each unaliased select item (`sql.y:5673-5693`, `token.go:142`), and `AliasedExpr.ColumnName()` (`ast_funcs.go:2528`) prefers it. Almost all remaining divergences come from the following causes:

| # | root cause | where | what it affects |
|---|---|---|---|
| R1 | **Vitess re-prints the query before MySQL names it.** The SQL sent to MySQL is `sqlparser.String(normalised AST)`. vttablet then re-parses and re-prints it (`tabletserver/planbuilder/query_gen.go:24`). MySQL therefore names columns after the re-printed text (`1 + 1`, `count(*)`, `json_extract(...)`, `cast(1e3 as DOUBLE)`, `0x1F`, `null`), with bound values inlined (`select ? from t` → `1`). | routes, join sides, unions, unsharded keyspaces, bypass / shard-targeted queries (`bypass.go:64`), lock functions | every pass-through column |
| R2 | **vtgate names columns from the wrong source.** Dual projections use `As` or `sqlparser.String(expr)` of the *normalised* AST (`planbuilder/select.go:319-322`), which gives `:vtg1 /* INT64 */`. Plain literals have no `InputExpression`, so after normalisation `ColumnName()` also returns `:vtgN`. `?` becomes `:v1`. | dual projections, aggregates, projections | columns vtgate computes itself |
| R3 | **Names are baked into cached plans, but the plan key is the normalised text.** Aggregate aliases, `Projection.Cols` and route aliases all come from the first spelling that was planned (`executor.go:1434-1441`, `engine/plan.go:263`). | every column vtgate names | `COUNT(*)` vs `count(*)`; `sum(id)+1` vs `sum(id)+2` |
| R4 | **Internal naming is inconsistent.** Semantic analysis names derived columns with `String(expr)` (`semantics/derived_table.go:90`), while the SQL builders use `ColumnName()` (`SQL_builder.go:206`, `route.go:566`, `projection_pushing.go`, `cte_table.go`, …). Names are compared with a mix of `==` (`SQL_builder.go:220-237`, `cte_table.go:135`, `query_planning.go:1052`) and case-insensitive checks. CTE bodies are parameterised but derived tables are not (`normalizer.go:863`). | derived tables, CTEs, unions | invalid SQL, "column not found", VT13001, panics |
| R5 | **The naming function is incomplete.** It is missing all of the following: stripping versioned-comment markers; `NULL` → `NULL`; the first-fragment rule for string literals; stripping leading whitespace; cutting at NUL; conversion to utf8mb3 with `?` replacement; the per-token truncation rules; telling `AS ''` apart from no alias; the merged-vs-materialized rule. The lexer also treats `0X41`, `0B101`, `1a`, `N 'abc'` and `b''` differently from MySQL, which changes what kind of item these become. | `ColumnName()`, the tokenizer | literals, long names, charsets |
| R6 | **Prepared-statement metadata is wrong.** Prepare-time metadata comes from the route's field query (`where 1 != 1`, re-printed). It includes helper columns such as `weight_string(...)`, and routed `?` columns are named after the bound value. | `executor.go:1785` `handlePrepare` | `COM_STMT_PREPARE` / `EXECUTE` |
| R7 | **References to implicit names are not resolved.** ``order by `id+1` ``, ``group by `1-id` ``, ``having `…` `` and ``select `1+1` from (select 1+1) x`` fail with 1054. They only work today when the normaliser happened to inject an alias (``select database() from t order by `database()` ``). | semantic analysis, the normaliser | query errors |

The normaliser's alias injection (`normalizer.go:206-224`) causes two more problems:
- It leaks literals into `RedactSQLQuery` output. For example, `select concat('secret', last_insert_id())` and `select concat('secret', @@autocommit)` are redacted with `` as `concat('secret', …)` ``.
- It splits the plan cache per literal value and per spelling.

`InputExpression` is a substring of the query, so it keeps the whole raw query alive in cached plans. `CachedSize` counts only the substring.


## 4. Design

### 4.1 Naming select items (parser)

`go/vt/sqlparser/column_name.go`.

- For each unaliased select item, the grammar records the byte span of the item in the query, the `/*!…*/` edits the tokenizer made inside it, and what kind of item it is: column reference, text literal, `NULL`, `?`, integer, decimal, float, unsigned or raw text. This is kept in the unexported `AliasedExpr._name`, which `Equals` and `Clone` ignore.
- `AliasedExpr.MySQLColumnName(env)` applies §2 to it, including the per-token truncation and the utf8mb3 conversion. An item that the parser did not create, such as one the planner builds, falls back to `ColumnName()`.
- The tokenizer understands 6-digit version comments from MySQL 8.1 on, and the cpp edits in §2.2.

### 4.2 Aliasing (after normalising)

`sqlparser.AliasColumnNames(stmt, env, bindVars)` runs right after `Normalize` and before the plan-cache key is taken. It visits the first query block of the statement, and of every derived table, CTE and view without a column list, because their select items name their columns.

For each item:

| item | action |
|---|---|
| explicit alias | normalised the way MySQL normalises it (`AS ' a'` → `a`, emoji → `?`, 256-byte limit) |
| column reference | left alone, except for a reference into a derived table or CTE that MySQL materializes, when the reference is spelled differently from the column (`select ID from (select id from t limit 5) x` → `ID`). For merged tables and base tables, the name vtgate and MySQL give it already agree |
| result column whose name contains a bind variable (a literal the normaliser parameterised, or a client bind variable), or whose name is empty | not aliased; renamed in each result (§4.3) |
| MySQL's name equals the printed expression | left alone: MySQL names the SQL vtgate sends the same way |
| anything else | aliased with MySQL's name |

Items are aliased even when their name matches the printed expression in these cases:

- **The item contains a subquery, a `NOT` before a comparison, or a comparison of tuples.** Planning rewrites these, so the SQL vtgate sends is spelled differently.
- **`ORDER BY`, `GROUP BY` or `HAVING` refers to the item by name** (``order by `count(*)` ``), so that vtgate resolves the reference.
- **A referenced name can only be an expression's name** because it is not a plain identifier (`` `id + 1` ``). Then literal-bearing items are aliased too. This makes vtgate reject ``select id+1 from t order by `id + 1` `` as MySQL does.

Columns of derived tables and CTEs with an empty name get the alias `vt_unnamed_<position>`, which a star over the table turns back into the empty name. View columns that MySQL would name `Name_exp_<position>` (an empty name, over 64 characters, or ending with a space) get that alias, because MySQL rejects such names as explicit aliases.

The normaliser no longer adds its own aliases to the items the parser created. `RedactSQLQuery` normalises without aliasing, so redacted queries no longer reveal literals through aliases. Literals in derived tables are now parameterised, because their aliases keep their names.

### 4.3 Renaming columns in each result

`AliasColumnNames` returns the columns it did not alias, with their names, as a `*ColumnRenames`. The executor keeps them on the vcursor and applies them to the result fields: for buffered results before they are returned, and for streamed results on the packet that carries the fields.

- Columns are found by their position in the select list. A star in the list has a width only the result knows: columns before the first star are counted from the start, and columns after the last star from the end.
- Renaming never changes fields in place, because field slices can be shared by cached plans and consolidated results. It copies the slice, and clones a field, only when a name changes.
- Prepared statements are cached by their exact text, and a cache hit uses the plan without parsing the text again. Their plans therefore keep their aliases, including the literal-bearing ones, and carry the renames (`engine.Plan.ColumnRenames`).

### 4.4 The plan cache

- The aliases are part of the normalised statement, so statements whose columns have different names get different plans. Statements that differ only in literals get the same plan.
- The plan key includes the session's client and connection charsets, which names depend on. `PlanKey.Hash` prefixes every string with its length, so keys whose strings split the same bytes differently no longer collide.

### 4.5 Charsets

- The session (`vtgatepb.Session`) keeps the client, connection and results charsets. The MySQL handshake sets them, and so do `SET NAMES`, `SET CHARACTER SET`, `character_set_client`, `character_set_connection`, `character_set_results` and `collation_connection`. The tablets still use their own charset.
- `MySQLColumnName` reads identifiers and raw text in the client charset and text literals in the connection charset, as MySQL does.
- The MySQL protocol server converts the name, original name, table, original table and database of each field to `character_set_results`, unless it is utf8, utf8mb4, binary or NULL. It does so for query results and for prepared-statement metadata. gRPC clients get UTF-8.
- `SET character_set_results = binary` parses.

## 5. Alternatives considered

**Keep names out of plans and rename every column at the executor root.** An earlier version stored, in each plan, which select item names each result column, including through derived tables, and computed every name from the executed statement. It shared plans between spellings and returned the same names as this design, but it took about four times as much code in the executor, planner and engine. It had to follow columns through the planner's rewritten copies of the statement, and it silently fell back to vtgate's own names whenever that mapping failed. This design keeps that approach only for the one case where aliases cost too much: names that contain literals.

**Alias every item, including literal-bearing ones.** This is the simplest rule, but each literal value then gets its own plan. On the executor benchmark, a select with a different literal each time ran 2.5 to 3 times slower, and pushed other plans out of the cache. It also puts literal values into the SQL sent to the tablets, where they show up in query logs and statistics and cannot be redacted.

## 6. Rollout and compatibility

There is no flag. MySQL compatibility is the contract, so a column name that differs from MySQL is a bug.

**User-visible changes.**
- Some names that were wrong but deterministic change, for example `count(*)` → `COUNT(*)` in an unsharded keyspace, or `1 + 1` → `1+1`. Applications that look columns up by the old names will notice. This should ship in a major release, with a release note that lists the classes of change.
- The SQL that vtgate sends to the tablets now carries aliases for items spelled differently from how Vitess prints them (`sum(k) as ``SUM(k)```). This shows in tablet query logs, query statistics, `vexplain` and plan output, and in the text that tablet query rules match.
- Errors that print a select expression now include its alias. The planner's error for a second `DISTINCT` aggregate prints the expression without it.

**Mixed versions.**
- vttablet does not change. The aliases vtgate adds are ordinary identifiers.
- The new session fields are additive: vtgates one major version older or newer ignore them.
- During a rolling vtgate upgrade, vtgates of different versions behind one load balancer can return different names for the same query.

## 7. Performance

Measured with `BenchmarkSelectColumnNames` (`go/vt/vtgate/executor_column_names_test.go`), which runs the executor end to end against sandbox tablets, against the base commit. The two were run alternately, 6 times each, on a 4-core machine:

| query | time | allocations |
|---|---|---|
| `select id, name from user where id = 1` | within noise | -2 (199 → 197) |
| `select id, count(*), 1+1, 'abc' from user where id = 1` | within noise (+3 %) | +3 (229 → 232) |
| `select 1, 1+1, now()` (evaluated by vtgate) | +9 % | +8 (100 → 108) |
| `select id, <a different literal each time> from user where id = 1` | +8 % | +1 (206 → 207) |

- The per-execution cost is computing names for the select items, printing the ones that may need an alias, and renaming the fields of literal-bearing columns.
- Selects whose literals differ share one plan, as before.

## 8. Tests

- **Corpus.** `go/vt/sqlparser/testdata/column_names.json` holds 1,583 queries with the names MySQL 8.0.43 and 8.4.6 return, generated by `go/tools/mysqlcolnames`. `TestMySQLColumnNameCorpus` checks `MySQLColumnName` against every case whose names follow from the query text, including the charset cases.
- **End to end.** `go/test/endtoend/vtgate/queries/columnnames` runs the corpus against vtgate in a sharded and an unsharded keyspace, over OLTP, OLAP and prepared statements, and compares the names with MySQL. Mismatches are listed in `known_divergences.txt`, which must only shrink. `-update-known` rewrites it, and `-show-known` logs what each side returns.
- **Unit tests** cover aliasing (`TestAliasColumnNames`), renaming (`TestColumnRenames`), plan sharing between literal values (`TestColumnNamesDoNotSplitThePlanCache`), streaming, prepared statements, charsets and the plan key.

## 9. Remaining divergences

`known_divergences.txt` lists 731 case and target combinations (140 cases), from 4,804 before. Almost all are SQL that vtgate does not support, rather than naming:

| cases | what | why |
|---|---|---|
| 47 | `sql_mode` `ANSI_QUOTES`, `ANSI`, `PIPES_AS_CONCAT`, `IGNORE_SPACE`, `HIGH_NOT_PRECEDENCE`, `NO_BACKSLASH_ESCAPES` | vtgate refuses to set them, because its parser cannot honour them |
| 22 | `TABLE t`, `VALUES ROW(…)` | not supported |
| 19 | `NATURAL JOIN` on sharded keyspaces, `LATERAL`, `JSON_TABLE`, `INTERSECT`, `EXCEPT` | not supported |
| 16 | window functions on scatter queries | not supported |
| 17 | `{d '…'}`, `cast(… as year)`, `sounds like`, `@a := 2`, `@@session . x`, nested comments inside `/*!…*/`, `b''`, ``AS `` ``, a `WITH` inside a derived table | parser gaps |
| 4 | `json_arrayagg`, `std`, `avg(distinct)` and similar on scatter queries | not supported |
| 7 | `0X41`, `count (*)`, schema names in other case, `PARTITION` on an unpartitioned table, a latin1 identifier lookup | vtgate accepts SQL that MySQL rejects |
| 1 | `select 1 into @x from t` | vtgate drops the connection |
| 4 | views created through the schema tools, not through vtgate | the tools print the view without the aliases |
| 3 | `N'x'` in a derived table; `select ID` from a `UNION` derived table; `RIGHT JOIN … USING` column order | planner and normaliser bugs |

Once vtgate supports a construct, the parser already names it correctly.

**Out of scope.**
- `SHOW …` headers, and the column counts that `DESCRIBE` and `SHOW CREATE` report at prepare time.
- The `org_name`, `table`, `org_table` and `db` metadata.
- vitessio/vitess#20480: scatter queries use the first shard's field metadata.
- Converting row data, not only names, to `character_set_results`.

## 10. Risks and open questions

- **Planning can rewrite an expression the alias rule did not expect.** Its column is then named after the rewritten SQL. The rule covers the rewrites we know of (subqueries, `NOT`, tuple comparisons), and the corpus catches the others.
- **The merge decision can depend on server state vtgate does not see**, such as a global `optimizer_switch` with `derived_merge=off`. It only affects references into derived tables that are spelled differently from the column.
- **Handling of `/*!` depends on `--mysql-server-version` matching the backend.** The parser already requires this.
- **The legacy `TRUE`/`FALSE` naming of MySQL 8.0.11–8.0.16 is not supported.** Those versions are outside Vitess's support window.
## Appendix — How the measurements were made

The numbers in §1 and §3 come from four investigations, all against commit `3ce81c4`:
- **MySQL source.** A reading of the `mysql/mysql-server` branches `8.0`, `8.4` and `trunk`: `sql/sql_yacc.yy`, `parse_tree_items.cc`, `sql_lex.cc`, `item.cc`, `sql_base.cc`, `table.cc`, `sql_view.cc`, `sql_resolver.cc` and `protocol_classic.cc`.
- **Empirical corpus.** 1,571 cases run on MySQL 8.0.43 and 8.4.6 through five paths: text protocol, raw prepare, raw execute, go-sql-driver and SQL `PREPARE`.
- **Diff harness.** `vttestserver` with a sharded keyspace (`-80`/`80-`) and an unsharded one. It ran the corpus plus 343 seed cases against vtgate and against a reference MySQL. It uses a raw `COM_STMT_PREPARE` client, and gives each case its own plan-cache entry so that results are deterministic.
- **Code audit** of every place Vitess names a column, followed by an adversarial review of this plan against the corpus and live servers.

The corpus and the harness are checked in as `go/tools/mysqlcolnames` and `go/test/endtoend/vtgate/queries/columnnames`.
