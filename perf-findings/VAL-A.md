# VAL-A: VReplication rule bugs (#1, #3, #22) and one new finding

Base: the worktree HEAD e5e0091d44 (aa9ccf9 + 3 unrelated commits). `replicator_plan.go` and `table_plan_builder.go` are identical to aa9ccf9. No fixes were made.

Tests (in `VAL-A.patch`):
- `go/vt/vttablet/tabletmanager/vreplication/replicator_plan_rules_test.go`: pure tests. They build the plan through the real `buildReplicatorPlan` + `buildExecutionPlan`. The package TestMain still needs mysqld.
- `go/vt/vttablet/tabletmanager/vreplication/vcopier_rules_test.go`: tests backed by MySQL 8.0.46 through the package framework.
- `perf-findings/VAL-A-repro/`: the cluster repro scripts (BASE 30000, unsharded), plus the onlineddl filter probe (scratch; it passes on base and is kept for reference only).

---

## BUGS #1: an explicit column list with a target-generated column name. Verdict: **REPRODUCED** (end to end, Online DDL). The mechanism differs from what BUGS says. **Re-rank: P0 → P1** (no wrong data; the workflow wedges or vttablet crashes).

### Mechanism (corrected)

- In the explicit-list path, `colExprs` never gets `isGenerated`, but `FieldsToSkip` holds the **target's** generated columns.
- `appendFromRow`, which runs only in the copy phase (vcopier), matches `FieldsToSkip` against the **source field names**. The field names are the source column names, not the aliases.
- Any source field whose name matches a target generated column is skipped. The remaining values shift left, and the last bind location indexes past `Fields`, which panics (`index out of range [3] with length 3`).

### Why no wrong data is written

- In the copy phase, `len(Fields) == len(bindLocations)`, because `lastpk` is nil: vcopier builds its plan with `copyState=nil`, so no extra PK columns are appended.
- So every skip ends in a panic before any SQL executes. Shifted values are built into the buffer but never sent.
- The running phase (vplayer `applyChange`) binds by name and is correct.
- The one place "extra PK columns" exist is vplayer with a lastpk, and it does not use `appendFromRow`.

### Realistic trigger (verified)

- The Online DDL `ALTER TABLE t CHANGE COLUMN g h ..., ADD COLUMN g ... AS (...)` renames a column and keeps the old name as a generated alias.
- `NewVRepl` generates `select `id` as `id`, `g` as `h`, `x` as `x` from `t``. I checked this with a probe test.
- The same failure hits Materialize whose `source_expression` names a column that is generated on the target.

### Impact on base (end to end, `repro1*.sh`)

- **Default settings (1 insert worker):**
  - The panic unwinds through `copyTable`'s deferred `copyWorkQueue.close()`. `ResourcePool.Close` then waits forever for the worker that never returned, so `runBlp`'s recover is never reached.
  - The migration stays `running`: rows_copied 0, stream `Copying`, **empty message**, and nothing is logged.
  - `ALTER VITESS_MIGRATION ... CANCEL` fails with DeadlineExceeded after 30 s. `vtctldclient Workflow stop` also fails after 30 s, and `GetWorkflows sbtest` hangs.
  - The cause of those hangs: the goroutine is stuck in `controller.Stop` under `Engine.exec`, holding `vre.mu`, so the tablet's whole VReplication engine is locked.
  - A vttablet restart clears the hang only because the timed-out cancel had already persisted `state=Stopped`.
- **`--vreplication-parallel-insert-workers>1`:** the panic happens in the goroutine started by `vcopierCopyWorkQueue.enqueue` (vcopier.go:811), which has no recover. It kills the process: `TestVALAScratchGenRenameParallelWorkers` died with the panic trace. Restarting then crash-loops, because the copy resumes.

### Tests (fail on base)

- **`TestExplicitColumnListWithTargetGeneratedColumnName`** (pure):
  - The Online DDL subtest shows the running phase giving the correct `insert into dst(id,h,x) values (1,'renamed-value','x-value')` while the copy phase panics.
  - The Materialize subtest also panics.
- **`TestPlayerCopyExplicitListWithTargetGeneratedColumnName`** (MySQL):
  - It fails with `row counts don't match: [] ... stream: [[Copying ""]]`.
  - Its cleanup then panics on purpose: `stream 1 did not stop within 30s; its controller is stuck and the vreplication engine is locked`. Without that, the package would hang.

## BUGS #3: `select *` drops ConvertCharset / ConvertIntToEnum. Verdict: **TEST FAILS ON BASE** (silent corruption shown with MySQL), but **DOWNGRADE P0 → P3** (unreachable from supported operations)

### Reach

- The only producer of `ConvertCharset` / `ConvertIntToEnum` is `onlineddl/vrepl.go:381/384`.
- Its filter always comes from `generateFilterQuery`, which always writes an explicit `select ... as ...` list, including for REVERT: both `NewVRepl` call sites go through it.
- The materializer (MoveTables, Reshard, Materialize, Migrate, LookupVindex) never sets these fields, and `TableMaterializeSettings` can't express them.
- So the bug needs a hand-inserted `_vt.vreplication` row.

### Tests

- **`TestSelectStarRuleKeepsConversions`** (pure):
  - `star.ConvertCharset` is nil, and so is `ConvertIntToEnum`.
  - The copy phase gives `'é'` instead of `'\xe9'`.
  - The running phase gives `2` instead of `'2'`.
- **`TestPlayerSelectStarRuleConvertsCharset`** (MySQL, source utf8mb4 → target latin1):
  - The explicit-list subtest passes.
  - The `select *` subtest stores `cafÃ©` (copy phase) and `naÃ¯ve` (running phase). The stream stays `Running` with no message.

### Side note

`appendFromRow` never applies `ConvertIntToEnum`, even for explicit lists. Online DDL avoids this because it sends `CONCAT(col)`.

## BUGS #22: a nil `ConvertCharset` entry panics. Verdict: **TEST FAILS ON BASE** (nil deref in both phases), but no real rule path produces nil, so **P2 → P3**

- The controller parses `source` with prototext.
- A nil entry marshals as `value:{}`. `convert_charset:{key:"g"}`, prototext, proto and vtproto all read back as a non-nil empty `CharsetConversion`.
- protojson rejects `null`.
- The empty conversion that real parsing produces gets a clean error (`character set  not supported for column val`), and the test asserts this.

### Test

`TestNilCharsetConversionEntry`. As a bonus, applying only the `replicator_plan.go` part of F16 turns it green. #1 and #3 still fail with F16.

## NEW: Online DDL `RENAME COLUMN` silently NULLs the column. **REPRODUCED end to end**. Suggest **P0**, needs triage.

- `schemadiff.OnlineDDLAlterTableAnalysis` puts only `CHANGE COLUMN` in `ColumnRenameMap`, not `RENAME COLUMN`.
- The column is therefore treated as dropped and left out of the filter (`select `id` as `id` from `t``).
- On the cluster, `alter table vala_r rename column a to b` (vitess strategy) → `complete`, and `b` is NULL for every row. The `CHANGE COLUMN a b` control keeps the data.
- Script: `repro_rename_column.sh`. I found no matching upstream issue.

Cleanup: the cluster is down, test mysqlds are killed, and the data dirs are removed.
