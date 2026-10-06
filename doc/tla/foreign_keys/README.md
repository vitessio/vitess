# TLA+ models of foreign key handling

These models check how vtgate maintains foreign keys when a keyspace uses
`foreign_key_mode: managed`. Each model covers one area:

| Model | Question | Result |
|---|---|---|
| [`FkLocking`](#fklocking-row-locking) | Do the locks keep FKs intact under concurrent transactions? | **No, under READ COMMITTED** |
| [`FkNonLiteral`](#fknonliteral-non-literal-updates-and-set-null) | Does a non-literal `UPDATE` cascade the way MySQL does? | **No, for `ON UPDATE CASCADE`**, until the child updates run in dependency order. Yes for `SET NULL` |
| [`FkCrossShard`](#fkcrossshard-cross-shard-constraints) | Do cross-shard constraints survive a failed commit? | **No, with the default MULTI commit.** Yes with TWOPC |
| [`FkPartialExec`](#fkpartialexec-partial-failure-in-a-transaction) | Does a failed statement leave writes in the client's transaction? | No |

The three bugs were also reproduced on MySQL 8.0.46:

- **FkLocking:** the race was replayed directly on MySQL, and also run
  through vtgate (`vttestserver`).
- **FkNonLiteral:** the bug was run through vtgate.
- **FkCrossShard:** the result follows from `TxConn.commitNormal`, which
  commits the shards one at a time.

## Running

You need Java and `tla2tools.jar` from the
[TLA+ releases](https://github.com/tlaplus/tlaplus/releases). Version 1.8.0
was used here. Each model has an `MC<Model>.tla` module, and one `.cfg` file
per configuration:

```sh
cd doc/tla/foreign_keys
java -cp tla2tools.jar tlc2.TLC -workers auto -config MCFkLocking_RC_share.cfg MCFkLocking.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config MCFkPartialExec.cfg FkPartialExec.tla
```

All the models share the same simplifications, and each spec's header
explains why they are sound:

- Each SQL statement is one atomic step.
- A gap lock is modelled as a lock on one key value.
- Any transaction can roll back at any statement boundary. This covers lock
  wait timeouts, `NOWAIT` failures and client rollbacks.

## FkLocking: row locking

Schema: `gp(k) ← p(k) ON UPDATE/DELETE CASCADE ← c(p) ON UPDATE/DELETE RESTRICT`,
all on one shard.

The model runs four transactions concurrently:

- Vitess's `UPDATE gp` cascade plan.
- Vitess's `DELETE gp` cascade plan.
- `INSERT INTO c`, which Vitess passes to MySQL with foreign key checks on.
- `UPDATE c`, which Vitess also passes to MySQL with foreign key checks on.

The steps are the queries of the planner's golden plans (the `u_tbl7` cases
in `go/vt/vtgate/planbuilder/testdata/foreignkey_cases.json`):

| Plan step | Query | Lock |
|---|---|---|
| Selection | `SELECT k FROM gp WHERE k = 'k1'` | `FOR UPDATE` |
| VerifyChild | `SELECT 1 FROM p parent, c child WHERE parent.k = child.p AND parent.k IN ::fkc_vals ...` | `FOR SHARE` (`getVerifyLock`) |
| PostVerify | `UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ p SET k = 'k2' WHERE k IN ::fkc_vals` | |
| Parent | `UPDATE gp SET k = 'k2' WHERE k = 'k1'` | |

| Config | Isolation | VerifyChild lock | Result |
|---|---|---|---|
| `MCFkLocking_RR_share.cfg` | REPEATABLE READ | `FOR SHARE` (current) | No violation (260 states) |
| `MCFkLocking_RC_share.cfg` | READ COMMITTED | `FOR SHARE` (current) | **Orphan row in `c`** |
| `MCFkLocking_RC_update.cfg` | READ COMMITTED | `FOR UPDATE` | No violation (260 states) |
| `MCFkLocking_RR_update.cfg` | REPEATABLE READ | `FOR UPDATE` | No violation (260 states) |

Here is the counterexample under READ COMMITTED:

| Step | T1: Vitess, `UPDATE gp SET k = 'k2' WHERE k = 'k1'` | T2: `INSERT INTO c (p) VALUES ('k1')` |
|---|---|---|
| 1 | Selection takes X on `gp('k1')` | |
| 2 | VerifyChild `FOR SHARE` takes S on `p('k1')` and finds no child row. READ COMMITTED takes no gap lock on `c`. | |
| 3 | | The InnoDB FK check takes S on `p('k1')`, which is compatible with T1's S. The insert commits. |
| 4 | `UPDATE p ...` runs with `foreign_key_checks=OFF`, so InnoDB does not check `c` | |
| 5 | `UPDATE gp ...` runs, then T1 commits. `c.p = 'k1'` now has no parent. | |

Here is why each other configuration is safe:

- **REPEATABLE READ:** step 2 also takes a gap lock on `c` at `'k1'`, and
  that lock blocks T2's insert.
- **`FOR UPDATE`:** step 2 takes X on `p('k1')`, which blocks T2's foreign
  key check.

The DELETE plan is safe at both isolation levels. Its child `DELETE` keeps
`foreign_key_checks` on, so InnoDB enforces RESTRICT with its own locks.

The same sequence of a `FOR SHARE` check followed by an update with checks
off also runs for a direct non-literal update of a referenced column:
`SemTable.HasNonLiteralForeignKeyUpdate` sets `VerifyAllFKs`. So
`UPDATE p SET k = concat('k', '2') WHERE k = 'k1'` has the same race, even
though no cascade is involved.

Both the cascade and the direct non-literal update leave an orphan row when
run through vtgate under READ COMMITTED. To pause vtgate's plan inside the
race window, the test gave `p` a second RESTRICT child `d`. A third
transaction held a lock on a matching row in `d`, so vtgate's check of `d`
waited after its check of `c` had already passed.

## FkNonLiteral: non-literal updates and SET NULL

Schema: `parent(id PRIMARY KEY, k UNIQUE) ← child(p) ON UPDATE CASCADE | SET NULL`.
The statement is `UPDATE parent SET k = <expr>`, where each row can get any
new key, such as `k + 1` or another column's value.

For a non-literal update, the plan cascades one Selection row at a time
(`FkCascade.executeNonLiteralExprFkChild`). For each row it runs
`UPDATE child SET p = :new WHERE p IN ((:old))`, then a separate
`UPDATE parent` with `foreign_key_checks=OFF`.

MySQL itself cascades each row as it updates it, and checks the unique key
after each row. The Selection is a different statement from the UPDATE, so
MySQL can return its rows in a different order. For example, the Selection
reads only `k` and its expressions, so the index on `k` covers it. The UPDATE
of `k` scans the primary key instead.

The `CascadeOrder` constant picks the order of the per-row child updates:

- **`selection`:** the order the Selection returns the rows. This is
  Vitess's behaviour before the fix.
- **`dependency`:** the fix, in `nonLiteralUpdateOrder`. A row whose new key
  is another changed row's old key runs after that row. If the keys move in
  a cycle, no such order exists. The statement then fails with a duplicate
  key error before any write, as MySQL fails the parent UPDATE.

Vitess uses the dependency order only when the parent columns contain a
primary or unique key, as in this model. MySQL 8.0 also allows a foreign key
to reference a non-unique index. In that case MySQL's own cascade matches
children again in the order it updates the rows. Vitess then keeps the
Selection order, which matches MySQL when both statements return rows in the
same order, for example with `ORDER BY`. The `fk_multicol_t15` cases in
`TestFkScenarios` cover this.

The model enumerates every initial state (including NULL keys), every new
key, every child update order and every UPDATE order. It then compares
Vitess's result with MySQL's:

| Config | Child action | Order | Result |
|---|---|---|---|
| `MCFkNonLiteral_cascade_selection.cfg` | CASCADE | Selection | **Differs from MySQL** |
| `MCFkNonLiteral_cascade_dependency.cfg` | CASCADE | dependency | Matches (11,226,348 states) |
| `MCFkNonLiteral_setnull_selection.cfg` | SET NULL | Selection | Matches (17,032,500 states) |
| `MCFkNonLiteral_setnull_dependency.cfg` | SET NULL | dependency | Matches (11,226,348 states) |

Before the fix, a later row's `WHERE p IN ((:old))` with CASCADE also
matches the children that an earlier row just moved. `FkIntegrity` still
holds in every state: each child references an existing parent, but
sometimes the wrong one.

The example below was run through vtgate before the fix:

```sql
-- parent(id, k): (1, 2), (2, 1)    child(id, p): (10, 1), (20, 2)
UPDATE parent SET k = k + 1;
-- Selection order (index on k): k=1 -> 2, then k=2 -> 3
-- Vitess: child 10 -> 2, then the k=2 row moves children 10 and 20 -> 3
-- Result: child 10 points at parent id 1 (k=3).
-- MySQL's own cascade keeps it on parent id 2 (k=2).
```

With the fix, the k=2 row's child update runs first. `TestFkQueries` in
`go/test/endtoend/vtgate/foreignkey` covers this case.

## FkCrossShard: cross-shard constraints

Schema: `parent(k)` on shard B, and `child(p)` on shard A with
`ON UPDATE/DELETE CASCADE`. The model runs three Vitess transactions:

- A cascading `DELETE` on parent.
- A cascading `UPDATE` on parent.
- `UPDATE child SET p = ...`. Its cross-shard parent check is a vtgate
  `LEFT JOIN`, with `FOR SHARE` on both shards.

An `INSERT` with a cross-shard parent, and a cross-shard RESTRICT, are
rejected with `VT12002`, so they are not modelled.

| Config | Commit | Result |
|---|---|---|
| `MCFkCrossShard_multi_fail.cfg` | MULTI; a shard commit can fail | **Orphan persists** |
| `MCFkCrossShard_multi.cfg` | MULTI; commits never fail | No lasting violation |
| `MCFkCrossShard_multi_strict.cfg` | MULTI; checked in every state | Violated briefly between shard commits |
| `MCFkCrossShard_twopc.cfg` | TWOPC; checked in every state | No violation |

`TxConn.commitNormal`, the default MULTI mode, commits the shards in the
order they joined. In a cascade, the Selection makes the parent's shard join
first, so the parent's shard commits first. If the child's shard then fails
to commit, the parent change is permanent and the cascaded child change is
rolled back. vtgate reports this only as warning `ERNonAtomicCommit`.

Locking readers are protected while the commit is in progress. A reader that
does not lock can see one shard's half of the change.

## FkPartialExec: partial failure in a transaction

This model covers the per-statement state machine that vtgate uses inside a
client `BEGIN ... COMMIT`:

- `SafeSession.SetSavepointState`
- `VCursorImpl.markSavepoint`
- `setRollbackOnPartialExecIfRequired`
- `Executor.rollbackPartialExec`

It runs every plan of up to 5 queries, each on 1 to 3 shards, with every
outcome:

- Reads and writes.
- `ExecuteMultiShard` and `VCursorImpl.Execute`.
- Full, partial and no success on the shards.
- A failing `SAVEPOINT`, a failing `ROLLBACK TO`, and errors raised by vtgate
  itself, such as an `FkVerify` hit.

It checks two properties:

- **`NoPartialWrites`:** a failed statement leaves none of its writes in a
  transaction that stays open.
- **`SavepointBeforeWrites`:** a savepoint is only taken before the
  statement's first write.

Result: both hold (399 distinct states). The design works because any
successful write arms a rollback, so a savepoint is never taken after a
write. Once armed, the rollback is either a `ROLLBACK TO` that undoes
everything after the savepoint, or a full transaction rollback.

A mutation test confirms the model can fail. Arming the rollback only when a
query partially succeeds breaks `SavepointBeforeWrites`.

## Not modelled

- **Typing.** The new value sent to a cascaded child is computed by a
  `SELECT ... cast(expr AS type)`. The parent's own `UPDATE` stores `expr`
  with MySQL's assignment rules. Where those differ (truncation, rounding,
  `sql_mode`), the child and parent values can differ. Testing this needs a
  model of the evalengine, so it belongs in a fuzz test rather than in TLA+.
- **Non-unique parent keys.** MySQL 8.0 still allows a foreign key to
  reference a non-unique index. Vitess matches MySQL there only when its
  Selection returns rows in the order MySQL updates them.
