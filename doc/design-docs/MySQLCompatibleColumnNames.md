# MySQL-compatible result-set column names

Status: proposal / implementation plan
Scope: vtgate (parser, normaliser, planner, executor, MySQL protocol layer). vttablet needs no changes.

## 1. Summary

When a `SELECT` item has no alias, MySQL names the result column after the item's *original query text*. A few item kinds are exceptions and are named by their own value.
Vitess re-parses, normalises, re-prints, splits and partly evaluates queries. At each of those steps it can end up with a different name.

We ran 1,914 queries against MySQL 8.4.6 and against a local Vitess cluster with a sharded and an unsharded keyspace. Each query went through the text protocol, `COM_STMT_PREPARE` metadata and `COM_STMT_EXECUTE` metadata:

- **921 queries (48 %) return at least one different column name.**
- **244 more fail on only one side.** Most of these fail because the name vtgate uses internally does not match the name MySQL uses.

Some examples, all from this checkout:

| query | MySQL | Vitess today |
|---|---|---|
| `select 1` | `1` | `:vtg1 /* INT64 */` |
| `select 1+1` | `1+1` | `:vtg1 /* INT64 */ + :vtg1 /* INT64 */` |
| `select COUNT(*) from unsharded_t` | `COUNT(*)` | `count(*)` |
| `select sUm(id) from sharded_t` (run after `select SUM( id ) …`) | `sUm(id)` | `SUM( id )` (plan-cache poisoning) |
| `select j->'$.a' from t` | `j->'$.a'` | `json_extract(j, '$.a')` |
| prepared `select ? from t` | `?` | `NULL` at prepare time, then the bound value (`1`) at execute time |
| ``select `1+1` from (select 1+1) x`` | `1+1` | error: column not found |
| ``select id+1 from t order by `id+1` `` | rows | error 1054 |
| `with c as (select 1+1) select * from c` | `1+1` | error: unknown column `:vtg1 /* INT64 */ + …` |

This document:

1. specifies MySQL's naming algorithm exactly (§2);
2. proposes an architecture that makes Vitess return byte-identical names in every case we found, without growing any plan cache and with negligible per-query cost (§4 to §9).

The core idea is that **a column name depends on the current statement's text, never on the cached plan**.
- A cached plan stores a *name template*. The template says, for each output column, which select item names it, found by its position in the statement's structure.
- On each execution, vtgate computes the names from the statement it has just parsed and applies them once, at the root of the result.

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
  - The Vitess tokenizer reads only 5 digits (`token.go:735`).
- **5-digit versions** such as `/*!80400 x*/` are expanded on 8.4 and removed on 8.0, so the name differs between the two.
- **`ANSI_QUOTES`** makes `"x"` an identifier.
- **`NO_BACKSLASH_ESCAPES`** changes the values of string literals.
- **`PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE` and `IGNORE_SPACE`** do not change names; the raw text is kept (`count (*)`).

## 3. Why Vitess diverges today

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

### 4.1 Principles

1. **One naming function.** Every client-visible name comes from a single implementation of MySQL's rules, `sqlparser.MySQLColumnName`.
2. **Names are never stored as strings in a plan whose cache key is not the raw text.**
   - Such a plan holds a *name template*: for each output column, a **structural reference**.
     - The reference is the item's ordinal in the first query block's select list.
     - For a column of a merged derived table, it is the path to the derived table plus the ordinal inside it.
     - The template never uses a global walk index. The normaliser rewrites parts of the AST (`EXISTS`, subquery unnesting, view inlining), so a walk index would not be stable across statements that share a key.
   - References are resolved against the **normalised** AST of the current execution. Its structure is exactly what the plan key encodes, so a reference always addresses the corresponding node. The name inputs live on nodes that survive normalisation.
   - Each template entry also records a fingerprint (the item's `NameKind` and node type), which is checked on every execution.
   - Together these remove plan-cache poisoning (R3) by construction.
3. **Rename once, at the root; only where vtgate can know better than MySQL.**
   - The executor overwrites `Field.Name` of the final result, or of the first packet when streaming, using the template.
   - The operator tree marks, for each output column, whether the column reaches the root **unchanged from a single route**.
     - A **column reference** on such a column is left to MySQL (`PassThrough`). MySQL prints it exactly as typed and knows the merge decisions, views and `information_schema` case.
     - Every other column is renamed.
4. **Internal names are separate from client names.**
   - Inside a plan, vtgate only has to agree with itself and with the SQL it sends.
   - Internal names depend only on the normalised AST, so they are stable across statements that share a key and never contain literal text. When generated SQL has to refer to a derived-table or CTE column by name, vtgate emits an explicit *internal* alias (§5, Phase 5).
   - User references to implicit names (R7) are resolved by `MySQLColumnName` during semantic analysis.
5. **Original text stays out of the SQL and the cache keys.** Nothing about client-visible names goes into the normalised query. This fixes both cache fragmentation and the redaction leak.

**Alternatives considered and rejected**

- **Alias every select item with `AS <mysql name>` in the normaliser.**
  - This is the simplest option, and it is correct for pass-through queries.
  - But it splits the vtgate plan cache, the vttablet plan cache, query stats and consolidator keys per literal and per spelling.
  - It also makes every query longer, leaks literals into redacted logs, and cannot express names containing NUL or longer than 256 bytes.
- **Put `InputExpression` into the plan key.** This fragments the cache per spelling and still does not fix pass-through names.
- **Make the printer preserve the original formatting.** This is not feasible: bind variables, AST rewrites and vttablet's re-print all change the text.
- **Index name sources in walk order.** This breaks because the normaliser changes the shape of subqueries and FROM clauses (Principle 2).

### 4.2 Data flow after the change

```
client text
  → Parse2         tokenizer records, per select item: raw span, NameKind, versioned-comment edits
  → Normalize      unchanged, except: no alias injection for client names; top-level select items
                   and derived-table items keep their name inputs
  → plan key       the normalised text, unchanged
  → cached plan    Plan.Names = name template (structural refs + fingerprints + pass-through bits, no strings)
  → execute
  → root           names := template.Resolve(normalised AST of *this* execution, len(fields), NameEnv)
                   fields := renameFields(fields, names)   // copy-on-write: only changed entries are cloned
  → client
```

## 5. Implementation plan

The phases can each be merged on their own. Every phase ships tests that fail on `main` without it (see §8).

### Phase 0 — Test corpus and comparison tooling (prerequisite)

1. **Record MySQL's names.** Add `go/tools/mysqlcolnames/`, which runs a case file against a real MySQL 8.0 and 8.4 and records the names from:
   - the text protocol;
   - raw `COM_STMT_PREPARE` metadata (go-sql-driver throws this away);
   - `COM_STMT_EXECUTE` metadata.

   Commit its output as `go/vt/sqlparser/testdata/mysql_column_names.json`: about 1,500 cases, grouped by category, with names for 8.0 and 8.4. It must cover everything in §2, including the §2.1 references to implicit names (`order by` / `group by` / `having` / derived tables).
2. **Compare Vitess with MySQL end to end.** Add `go/test/endtoend/vtgate/queries/columnnames/`, which runs the corpus against vtgate and MySQL with `CompareColumnNames: true` across these combinations:
   - sharded and unsharded keyspaces, plus bypass / shard-targeted (`USE ks/-80`, `ks:-80`) queries;
   - OLTP and OLAP (streaming);
   - text and prepared statements, checking both the raw prepare metadata and the execute metadata.

   It starts with a list of known divergences. Each phase shortens the list; the goal is an empty list.
3. **Fix the plan e2e test.** Change `go/test/endtoend/vtgate/plan_tests/plan_e2e_test.go` to send MySQL the **original** query text and to compare names.

### Phase 1 — Parser and naming function (fixes R5; no behaviour change on its own)

Files: `go/vt/sqlparser/sql.y`, `token.go`, `ast.go`, and a new `column_names.go`.

1. **Name inputs, kept apart from existing behaviour.** In `select_expression` (`sql.y:5673`), record a new non-printed `AliasedExpr.Name *NameInput` whenever the item has no alias. It holds:
   - the raw span;
   - `NameKind` — one of `Raw`, `Column`, `Text`, `Null`, `Number{Int|Uint|Decimal|Float}`, `Param` or `NameConst`. It is computed from the parsed expression before any rewrite, and the self-naming rules and the `(…)` / unary-`+` unwrapping of §2.1 are decided here. A unary minus folded into a literal (`-1`, `- 1`) counts as `Raw`;
   - the versioned-comment edits inside the span (normally nil);
   - for text literals, the charset of the introducer.

   Two things keep this from changing behaviour:
   - `InputExpression` and `ColumnName()` stay exactly as they are, so internal names do not change. Otherwise, for example, `select * from (select 01) d` would name the inner column `01` inside vtgate while MySQL, which sees the re-printed `1`, calls it `1`.
   - `Name` is excluded from `Equals`/`Cachesize` hashing. Use a pointer to an opaque side struct that `asthelpergen` skips (add a skip option if it has none). Note that the existing `InputExpression` is already compared at `ast_equals.go:1866`; drop it from `Equals` as well.
2. **Record an explicit empty alias.** `as_ci_opt` (`sql.y:5704`) currently returns an empty `IdentifierCI` both for `AS ''` and for no alias. Add `AliasedExpr.HasAlias`, and accept ``AS `` ``. `Format` must never print an empty alias to vttablet (see Phase 5).
3. **Versioned-comment edits.** In `scanMySQLSpecificComment` / `Scan` (`token.go:160-169`, `727-775`):
   - record the byte ranges of the `/*!` / `/*!NNNNN` / `*/` markers and of future-version comments that are skipped entirely;
   - do this only when the query contains `/*!`;
   - support 6-digit versions, which require trailing whitespace;
   - compare versions against the configured `--mysql-server-version`, so that 8.0 and 8.4 backends each get their own behaviour.
4. **First fragment of a text literal.** `text_literal STRING` (`sql.y:2371`) concatenates the fragments. For `NameKind=Text` the naming function instead re-reads the **first** fragment from the span when it is needed, and applies the introducer charset and `NO_BACKSLASH_ESCAPES`.
5. **Lexer parity.** Match MySQL for `0X41`, `0B101`, `1a`, `N 'abc'` and `b''`, because they decide `NameKind`.
6. **`MySQLColumnName(ae, env NameEnv) (string, error)`** implements §2.1–§2.3.
   - It returns error 3854 for a string-quoted alias containing a 4-byte character.
   - `NameEnv` holds `character_set_client`, `collation_connection` and `character_set_results`, the server version and `NO_BACKSLASH_ESCAPES`. vtgate fills it from the session; gRPC clients get utf8mb4.
   - The result is always **valid UTF-8**, because `querypb.Field.Name` is a proto `string`. A raw byte cut that splits a character is done only when writing to the wire (Phase 6).
   - **Fast path:** a raw ASCII span with no edits, ≤255 bytes and no NUL is returned as is, with no allocation.
7. **Unit tests.** Table-driven over the Phase 0 corpus: parse, call `MySQLColumnName`, and compare with the 8.0 and 8.4 names. Also fuzz spans containing comments and whitespace.

### Phase 2 — Name templates and root renaming (fixes R2, R3, R6)

Files: `go/vt/vtgate/engine/plan.go`, `executor.go`, `plan_execute.go`, `planbuilder/*`, `semantics/*`.

1. **Template type** (`engine.Plan.Names`):
   ```go
   type ColumnNameRef struct {
       Kind        NameRefKind // Item, DerivedItem, Fixed, PassThrough, Star
       Path        []int       // derived-table path from the top-level FROM (DerivedItem)
       Ordinal     int         // select-list ordinal in the first query block of the target
       Fingerprint uint16      // NameKind + node type, checked at Resolve
       Fixed       string      // "column_0", "nextval", vindex-function columns, view columns
       FromRoute   bool        // column reaches the root unchanged from a single route (decides PassThrough for column refs)
   }
   ```
2. **Build the template in one place, for every plan type.** Do it in `getCachedOrBuildPlan`, from the normalised AST plus what planning reports: which stars were expanded, and which columns come unchanged from a route. That covers all of these:
   - planner plans;
   - dual projections (`handleDualSelects` runs before semantic analysis, `select.go:45`);
   - lock functions (`buildLockingPrimitive`);
   - bypass / shard-targeted plans (`bypass.go:64`);
   - `SQL_CALC_FOUND_ROWS`;
   - `next value` (`Fixed "nextval"`);
   - vindex functions (`Fixed` names);
   - `select … into` (no result set, so no template).

   The template is built from the first query block, or the leftmost one for nested or parenthesised unions. A star that Vitess could not expand (no schema) becomes one `Star` entry of unknown width.
3. **Resolve at the root.**
   - `template.Resolve(stmt, nFields, env)` computes the width of an unknown star as `nFields - known`.
   - If the widths do not add up, or a fingerprint does not match, it never fails the query. It skips renaming, increments `vtgate_column_name_template_mismatch`, and logs once per plan.
4. **`renameFields` is copy-on-write.**
   - It compares each field's name with the resolved one, and allocates a new slice and `CloneVT`s a field only when a name differs.
   - Several field slices are shared and must never be mutated:
     - `Rows.fields`, which lives in the cached plan;
     - vttablet consolidator results, which are shared between waiters;
     - `sequenceFields`, which is a global.
   - Remove the in-place renames in `SimpleProjection.renameFields` (`engine/simple_projection.go:129-137`) and the dead `RenameFields` primitive.
5. **Hook points.** Apply the same template in each of these places. Because renaming happens in the executor, the gRPC API behaves the same as the MySQL protocol.
   - **`Executor.Execute` / `newExecute`**, after `executePlan`.
   - **`StreamExecute`**, on the first callback that carries `Fields`.
   - **`handlePrepare`**, after `GetFields`. Also drop the hidden helper columns there. Use the plan's hidden-column count (as in `Route.TruncateColumnCount`) rather than the template length, which is unknown when there is a `Star`.
   - **`COM_STMT_EXECUTE`**.
   - **SQL `EXECUTE`**: `ExecStmt` must carry the inner plan's template and the inner statement's name inputs (`PlanPrepareStmt`, `executor.go:1989`).
   - **The optimised `PlanSwitcher` plan** that replaces a cached prepared plan (`executor.go:~1289`) must keep the template.
   - **Multi-statement `ComQuery`.**
6. **Prepared plans are looked up by raw text, with no parse on a hit** (`executor.go:1268-1272`).
   - Store the template **and** a detached copy of the top-level and derived-table name inputs, cloned with `strings.Clone`.
   - Resolve them on each execution against the current `NameEnv`. Do not cache the resolved strings: the prepared-plan key has the collation, but not `character_set_client` or `optimizer_switch`.
7. **Dual projections** (`planbuilder/select.go:319-322`) and `buildNullFieldTypes` (`executor.go:1826-1843`) stop computing client names.

### Phase 3 — Column references, stars and statement kinds

1. **Top-level column references.**
   - **The column reaches the root unchanged from a single route** (single-shard, unsharded, bypass, one side of a join, a scatter route without vtgate projection): `PassThrough` (`FromRoute=true`).
     - MySQL gets the identifier exactly as typed, so its name is exactly what it would return for the original query.
     - This keeps MySQL's own merge decisions for MySQL-side views. For example, `select ID from v6` → `id`, where `v6` is registered in the vschema as a table.
     - It also keeps `derived_merge=off`, `information_schema` upper case and `performance_schema`.
     - These cases are correct today and must stay correct.
   - **vtgate projects the column itself** (a derived table, CTE or Vitess view evaluated or merged at vtgate):
     - evaluate `mysqlWouldMerge(derived, session)` with the §2.4 conditions;
     - if it merges, the name is a `DerivedItem` reference to the inner item (followed recursively), or `Fixed` for a column list or a Vitess view column;
     - if it materializes, the name is the typed name;
     - `derived_merge` comes from the session `optimizer_switch` when it has been set, and is assumed `on` otherwise;
     - `MERGE` / `NO_MERGE` hints come from the parsed comments.
   - **An unknown table under vtgate projection** (rare, for example cross-keyspace `information_schema`): use the typed name, and record it as a known limitation.
2. **Stars.**
   - When Vitess expands a star (`early_rewriter.go:879-903, 1113-1172`), the columns are named from the **schema tracker's** names, not from the case written in a hand-made vschema.
   - A star that is not expanded is a `Star` entry.
   - Fix USING/NATURAL coalescing so that coalesced columns come first, named from the left table (the right table for RIGHT JOIN). Today the column count on sharded keyspaces is wrong (vitessio/vitess#20759).
3. **Set operations.** The template comes from the leftmost first query block.
4. **`VALUES ROW(...)`, `TABLE t`, `values … union …`.** Use `Fixed` `column_N` and `Star` entries.
   - Fix the panic in `GetColumns()` (`ast_funcs.go:3224`) and the `no columns available` error on the prepare path.
   - Most of these statements are rejected by the planner today. Their tests are enabled once planner support lands.
5. **More than one unknown-width star.** When two stars have non-star items between them (`select t.*, 1+1, u.*`), the width of each star cannot be derived. Decide this syntactically in the normaliser, which has no schema:
   - add an explicit `AS <MySQLColumnName>` to the items between the stars, and mark those entries `PassThrough`;
   - this is the only place where client-name text goes into the SQL;
   - mark the alias as *injected*, so that `RedactSQLQuery` prints it redacted (Phase 4.3).

### Phase 4 — Rename every column (fixes R1)

1. The root renames **every** column that is not `PassThrough` or part of a star, whether or not vtgate produced it. This includes pass-through expressions: `COUNT(*)` on unsharded keyspaces, `1+1` on routes, `j->'$.a'`, `NULL`, bypass queries and lock functions.
2. The normaliser **stops** injecting top-level `AS <original text>` aliases (`noteAliasedExprName`, `normalizer.go:206-224`), because the root now provides the name.
   - Plan-cache entries that differ only in spelling or literals then share one entry (for example `DATABASE()` and `database()`).
   - It also removes the formatting of every `ColName` and literal item into a new buffer on each execution.
   - Phase 5.1 must be in place first, because today some `ORDER BY`/`GROUP BY` references to implicit names work only thanks to the injected alias (R7).
3. `RedactSQLQuery` never injects aliases, and prints any alias marked *injected* as redacted. This closes the literal leak.

### Phase 5 — Internal naming consistency (fixes R4, R7)

1. **Resolve references by the MySQL name.** Semantic analysis resolves user references to implicit names with `MySQLColumnName` and a case-insensitive comparison. This covers:
   - outer references into derived tables and CTEs (`derived_table.go:72-92`, `cte_table.go:103,135`, `real_table.go:152-165`, `semantic_table.go:530-544`);
   - `ORDER BY`, `GROUP BY` and `HAVING` references to select items.

   Replace `==` with `strings.EqualFold` at `SQL_builder.go:220-237`, `cte_table.go:135` and `query_planning.go:1052`.

   Known limitation: statements that share a key may differ only in the spelling of an item the user refers to by name. For example ``select `1+1` from (select 1+1) x`` and ``select `1+1` from (select 1 + 1) x``. MySQL rejects the second one, but Vitess would accept it if the first one was planned first. Every such pair resolves to semantically identical expressions, so only an error turns into a result, and results are never wrong.
2. **Internal names.** Internal names for derived-table and CTE columns are computed from the **normalised** AST. Whenever SQL that vtgate generates refers to such a column by name, the inner item gets an explicit internal alias. This fixes `Unknown column ''a''` and ``Unknown column `:vtg1 …` ``.
   - The alias is the printed normalised expression when that text is non-empty, at most 256 bytes long, and free of bind variables.
   - Otherwise it is a synthetic name, `vt_c<ordinal>`.
   - The alias never contains literal text. It depends only on the key, so it neither fragments the cache nor leaks anything.
   - An empty alias is never emitted, so vttablet N-1, which re-prints the SQL, is unaffected.
   - The root renames these columns for the client, which is correct because they reach the root through vtgate or through a star that vtgate expanded.
   - CTE bodies follow the same parameterisation rule as derived tables (`normalizer.go:863`).
3. **Column lists and duplicates.**
   - `(select 1, 2) x(a)` and `with c(a) as (select 1, 2)` must return MySQL's column-count error instead of panicking.
   - Duplicate derived-table column names must return error 1060. The comparison ignores case but not trailing spaces.
   - Fix the VT13001 error for a derived table that comes from a `WITH` (corpus `cte_015`).
4. **Views managed by vtgate** (`CREATE VIEW` through vtgate, vschema views): column names follow `make_valid_column_names` (`Name_exp_N`, `Name_exp_<k>_<name>`). They are computed when the view is loaded and used as `Fixed` names.
5. **DDL with a `SELECT`** (`CREATE VIEW`, `CREATE TABLE … SELECT` through vtgate) has no result set to rename.
   - Inject explicit aliases equal to the MySQL names, so that the table or view columns match. This is a one-off DDL, not a cached query.
   - Keep MySQL's validation errors (1166, 1060).

### Phase 6 — Charsets and wire encoding

1. `NameEnv` follows `SET NAMES`, `character_set_client` and `collation_connection` (§2.3).
2. The MySQL protocol layer converts all six metadata strings from utf8 to `character_set_results`. For utf8mb3 results it applies the raw 256-byte cut to aliases and to `ULONGLONG`/`FLOAT` tokens, and it skips the conversion for NULL or binary.
3. Accept `SET character_set_results = binary`, which vtgate currently rejects as a syntax error.
4. Cut names at the first NUL at the protocol boundary. This also applies to pass-through names.

### Out of scope, tracked separately

- `SHOW …` headers (`Tables_in_x (t%)`), and the column counts that `DESCRIBE` and `SHOW CREATE` report at prepare time.
- `org_name` / `table` / `org_table` / `db` metadata: `engine.Projection` drops it, and merged derived tables report the base table.
- vitessio/vitess#20480: scatter queries use the first shard's field metadata.
- Features that currently fail to parse or plan: `ANSI_QUOTES`, `PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE`, `IGNORE_SPACE`, `NO_BACKSLASH_ESCAPES`, `INTERSECT` / `EXCEPT` / `NATURAL JOIN` on sharded keyspaces, `LATERAL`, `JSON_TABLE`, and scatter window functions. Once they are supported, Phase 1 names them correctly.
- Panics that are not naming bugs: `select 1 into @x`, and `select 0X41` under `COM_STMT_PREPARE`.

## 6. Rollout and compatibility

There is no flag. MySQL compatibility is the contract, so a column name that differs from MySQL is a bug. Every phase fixes it unconditionally.

**User-visible change.** Some names that are wrong but deterministic today will change, for example unsharded `count(*)` → `COUNT(*)`, or `1 + 1` → `1+1`. Applications that depend on the old names (`row["count(*)"]`) will notice.
- The release notes of the version that ships Phase 4 carry a prominent "column names now match MySQL" entry, with the classes of change and examples from Appendix A.
- Phase 4 should land in a **major** release and not be backported, so that a patch upgrade never changes names.
- Phases 1–3 and 5 can be backported. They only fix names that are placeholder- or value-dependent, or non-deterministic (`:vtgN`, plan-cache poisoning, `?` → bound value, helper columns in prepare metadata), and turn errors into results.
- The redaction fix and the removal of in-place field mutation can also be backported on their own.

**Order within the release.** Phase 5.1 must merge before Phase 4, because removing the normaliser's injected aliases would otherwise break `ORDER BY` / `GROUP BY` references that only work thanks to those aliases today (R7).

**Mixed versions.**
- vttablet does not change. vtgate sends fewer aliases, and the only new aliases are internal ones: non-empty identifiers with no new syntax. vttablet N-1 and N+1 therefore work.
- During a rolling vtgate upgrade, vtgates of different versions behind one load balancer can return different names for the same query. The release notes say so.

## 7. Performance

| cost | where | size |
|---|---|---|
| `NameInput` per unaliased item | parse | one small struct per select item; the span is a substring, so nothing is copied |
| template resolution | root, per execution | O(output columns); structural references are followed on the normalised AST that is already in memory |
| `MySQLColumnName` | root, per execution | fast path returns the substring with no allocation; the slow path runs only for text literals, `/*!`, and long or multibyte names |
| `renameFields` | root, per result (only the first packet when streaming) | N string comparisons; copies the slice and `CloneVT`s fields only when a name changes, which happens only for columns whose names are wrong today |
| detached name inputs for prepared plans | plan build only (cache miss) | a few cloned strings per prepared plan |
| **removed:** the `TrackedBuffer` formatting in `noteAliasedExprName` on every execution | normaliser | one allocation and one format call per unaliased `ColName` or literal item |
| **removed:** cache fragmentation caused by alias injection | vtgate and vttablet plan caches, query stats, consolidator | fewer distinct keys |
| `Detach` of name inputs and `InputExpression` before an AST is cached | plan build (cache miss) | cached plans no longer keep the raw query alive. It covers `Route.QueryStatement`, `AggregateParams.Original` (`engine/aggregations.go:52`) and every other primitive that keeps AST nodes. `CachedSize` becomes accurate |

- We expect the overhead on a cache hit to be well below 1 % of vtgate CPU for typical OLTP statements.
- Run these benchmarks before and after each phase and post the results on the PRs:
  - `BenchmarkNormalize*`;
  - `BenchmarkWithNormalizer` / `BenchmarkWithoutNormalizer` (`go/vt/vtgate/bench_test.go`);
  - the executor select benchmarks in `executor_select_test.go`;
  - a new `BenchmarkMySQLColumnName` that covers both the fast and slow paths;
  - a new `BenchmarkRootRename`.
- Budget: no extra allocations on the fast path, and at most 1 % more ns/op on the executor benchmarks.

## 8. Test plan

Each phase adds tests that fail on `main` without it. Follow `CLAUDE.md`: testify, `t.Context()`, `t.Cleanup()`, `assert.Eventually`.

1. **Unit tests (sqlparser).**
   - `MySQLColumnName` over the Phase 0 corpus, for 8.0 and 8.4. Include sweeps across the per-token truncation boundaries and charset cases.
   - Tokenizer span and edit tests for combinations of `/*!` comments, including 6-digit versions.
   - `AS ''` round-trips.
   - `Equals` ignores the name inputs.
2. **Executor tests** (sandbox tablets, `executor_select_test.go`).
   - Poisoning: run spelling A, then spelling B, and check B's names.
   - The key collisions found in review: `exists(select a, b …)` vs `exists(select 1 …)`; `exists(select count(*) …)` vs `true`; a vschema view vs the same query written as a derived table; nested `(select (select …))`.
   - `select 1` names with normalisation on and off.
   - Prepared `?` metadata at prepare and at execute, for both vtgate-projected and routed columns.
   - A template or fingerprint mismatch skips renaming and increments the metric.
   - Copy-on-write: the cached `Rows.fields` must not change.
3. **Planner tests** (`planbuilder/testdata/*.json`). Show the `Names` templates in the plan output, and run the naming cases with the normaliser on.
4. **End-to-end** (`queries/columnnames`): the corpus, over the matrix in Phase 0.2, compared with MySQL by name. Also enable name comparison in `plan_e2e_test.go` with the original text.
5. **Random testing.** Extend the random-query tests (`go/test/endtoend/vtgate/queries/random`) to insert random whitespace, comments and case into select items, and add random `ORDER BY` / `GROUP BY` references to implicit names. Compare the names with MySQL.

## 9. Risks and open questions

- **The merge decision can depend on server state vtgate does not see**, such as a global `optimizer_switch` with `derived_merge=off`.
  - This only matters for columns that vtgate projects itself. Pass-through columns keep MySQL's decision (Phase 3.1).
  - Mitigation: use the session value when it has been set, and document the rest.
- **Structural references need the normalised AST to keep the same shape for a given key.**
  - Printing is structure-preserving for the nodes the references use: select lists, FROM, derived tables.
  - The fingerprint check guards against the rest.
  - The key collisions found in review are part of the test plan.
- **Spelling-only collisions on name references** (Phase 5.1) may make Vitess accept a query that MySQL rejects. They never change results.
- **Handling of `/*!` depends on `--mysql-server-version` matching the backend.** The parser already requires this.
- **The legacy `TRUE`/`FALSE` naming of 8.0.11–8.0.16 is not supported.** Those versions are outside Vitess's support window.

## Appendix A — Edge-case checklist

The *mechanism* column says which part of this plan produces MySQL's name. "Rename" means the Phase 2/4 root rename.

| case | MySQL | mechanism |
|---|---|---|
| `select 1`, `01`, `1.50`, `.5`, a 401-digit integer or decimal | the token text, not truncated | `Number{Int,Decimal}` |
| `1E3`, `0…018446744073709551615` (ULONGLONG) | the token text, cut at 256 | `Number{Float,Uint}` |
| `+1`, `(1)`, `((t.id))`, `+'a'` | `1`, `1`, `id`, `a` | self-naming items unwrapped at parse time |
| `-1`, `- 1`, `--1`, `+-1`, `(-1)`, `+(1+2)` | raw text | `Raw` |
| `'  abc'`, `'\ta'`, `''`, `'a''b'`, `'x' 'y'`, `'a\0b'` | `abc`, `a`, empty, `a'b`, `x`, `a` | `Text`: first fragment, leading strip, cut at NUL |
| `_utf8mb4'z'`, `N'n'`, `_latin1'é'`, `_binary'\xffabc'` | `z`, `n`, `Ã©`, `abc` | introducer charset + conversion |
| `'😀'`, or raw text containing an emoji | `?` | utf8mb3 conversion |
| `NuLl`, `(null)` | `NULL` | `Null` |
| `true`, `TRUE`, `x'41'`, `0x1f`, `b'1'`, `DATE '2020-01-01'`, `{d '…'}` | raw text | `Raw` + lexer parity |
| `1 /* c */ + 1`, `1 -- c\n + 1`, `1 # c\n + 1` | verbatim | raw span |
| `1 /*!+ 2*/`, `1 /*!80000 +1 */`, `1 /*!99999 + 2*/`, `1/*!+2*/+3` | `1 + 2`, `1  +1`, `1`, `1+2 +3` | cpp edits + server version |
| `COUNT(*)`, `Count( * )`, `CURRENT_TIMESTAMP`, `j->'$.a'`, `a <> 1`, `mod`, `over (order by id)` | raw text | rename (Phase 2 when vtgate produces the column, Phase 4 when it passes through) |
| an expression over 255 bytes, a multibyte character at the boundary | cut at ≤255 bytes on a character boundary | truncation |
| `AS ' a'`, `AS ''`, ``AS `` ``, `AS 'x  '`, a 300-character alias, `AS 'x\0y'` | `a`, empty, empty, `x  `, ≤256 bytes, `x` | alias rules + wire |
| `` AS `😀` `` / `AS '😀'` | `?` / error 3854 | alias rules |
| `?`, `? + 1`, `select ? from t` (prepared) | `?`, `? + 1`, `?` | `Param`; prepare and execute share the template |
| `@a`, `@@Session.AutoCommit`, `database()`, `last_insert_id()` | raw text | `Raw`; no normaliser alias needed |
| `(select 1)`, `exists(select 1)`, `(select Col from t where id=1)` | raw text | `Raw`; prepare metadata renamed |
| `select *, 1+1 from t` | table columns, `1+1` | `Star` + `Item` |
| `select t.*, 1+1, u.*` without a schema | table columns, `1+1`, table columns | Phase 3.5 fallback |
| `select ID from t` (column defined as `id`) | `ID` | `PassThrough` on a route; typed name at vtgate |
| `select ID from v6` (a MySQL view registered in the vschema as a table) | `id` | `PassThrough` |
| `select ID from (select id from t) d` / `… limit 1) d` evaluated at vtgate | `id` / `ID` | merge predicate |
| `select table_name from information_schema.tables` | `TABLE_NAME` | `PassThrough` |
| ``select `1+1` from (select 1+1) x`` | `1+1` | Phase 5.1 + internal alias |
| ``select id+1 from t order by `id+1` ``, ``group by `1-id` ``, ``having `1-id` < 0`` | rows | Phase 5.1 |
| `select * from (select 'a', 1, null) x` | `a`, `1`, `NULL` | Phase 5.2 + rename |
| `with c as (select id+1 from t) select * from c` | `id+1` | Phase 5.2 + rename |
| `(select 1 as a) union select 2 as b` | `a` | first query block |
| `values row(1,2)`, `table t` | `column_0`, `column_1` / table columns | `Fixed` / `Star` |
| `t join u using (id)`, `natural join`, `right join … using` | joined columns first | Phase 3.2 |
| `select * from (select 1, 1) d`, `(select id, ID from t) d`, `(select 'x', 'x ') d` | 1060, 1060, valid | Phase 5.3 |
| `create view v as select 1, 1, 1` | `1`, `Name_exp_1`, `Name_exp_1_1` | Phase 5.4 |
| `create table x as select <expression whose name is over 64 characters>` | error 1166 | Phase 5.5 |
| `SET NAMES latin1` / `character_set_results = latin1` | the name, converted | Phase 6 |
| `/*!80400 x*/` on 8.0 vs 8.4 | differs | version-aware edits |
| `USE ks/-80; select 1+1, 'a' 'b' from t` | `1+1`, `a` | bypass template + rename |
| `select IS_FREE_LOCK('a')` | `IS_FREE_LOCK('a')` | lock-function template + rename |

## Appendix B — How the measurements were made

The numbers in §1 and §3 come from four investigations, all against commit `3ce81c4`:
- **MySQL source.** A reading of the `mysql/mysql-server` branches `8.0`, `8.4` and `trunk`: `sql/sql_yacc.yy`, `parse_tree_items.cc`, `sql_lex.cc`, `item.cc`, `sql_base.cc`, `table.cc`, `sql_view.cc`, `sql_resolver.cc` and `protocol_classic.cc`.
- **Empirical corpus.** 1,571 cases run on MySQL 8.0.43 and 8.4.6 through five paths: text protocol, raw prepare, raw execute, go-sql-driver and SQL `PREPARE`.
- **Diff harness.** `vttestserver` with a sharded keyspace (`-80`/`80-`) and an unsharded one. It ran the corpus plus 343 seed cases against vtgate and against a reference MySQL. It uses a raw `COM_STMT_PREPARE` client, and gives each case its own plan-cache entry so that results are deterministic.
- **Code audit** of every place Vitess names a column, followed by an adversarial review of this plan against the corpus and live servers.

Phase 0 turns the corpus and the harness into checked-in tooling: `go/tools/mysqlcolnames` and `go/test/endtoend/vtgate/queries/columnnames`.
