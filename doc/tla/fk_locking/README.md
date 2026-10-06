# TLA+ model of foreign key locking

`FkLocking.tla` models the row and gap locks that keep foreign keys intact
when Vitess manages them (`foreign_key_mode: managed`). It checks one
property, `FkIntegrity`: the committed data never contains a child row whose
parent does not exist. It checks it across every interleaving of a few
concurrent transactions.

## What is modelled

The schema is one shard with a chain of three tables:

```sql
CREATE TABLE gp (k ... UNIQUE);
CREATE TABLE p  (k ... UNIQUE, FOREIGN KEY (k) REFERENCES gp (k) ON UPDATE CASCADE ON DELETE CASCADE);
CREATE TABLE c  (p ..., FOREIGN KEY (p) REFERENCES p (k) ON UPDATE RESTRICT ON DELETE RESTRICT);
```

There are four kinds of transaction:

| Kind | Statement | How it runs |
|---|---|---|
| `CascadeUpdGP` | `UPDATE gp SET k = 'k2' WHERE k = 'k1'` | Vitess `FkCascade` plan |
| `CascadeDelGP` | `DELETE FROM gp WHERE k = 'k1'` | Vitess `FkCascade` plan |
| `InsC` | `INSERT INTO c (p) VALUES ('k1')` | Passed to MySQL; InnoDB checks the FK |
| `UpdC` | `UPDATE c SET p = 'k1' WHERE id = ...` | Passed to MySQL; InnoDB checks the FK |

Each Vitess statement in the model corresponds to a query in the plan that
the planner generates. The `u_tbl7` cases in
`go/vt/vtgate/planbuilder/testdata/foreignkey_cases.json` show the same plan
shape.

| Plan step | Query | Model action |
|---|---|---|
| Selection | `SELECT k FROM gp WHERE k = 'k1' FOR UPDATE` | `Selection` |
| CascadeChild / VerifyChild | `SELECT 1 FROM p parent, c child WHERE parent.k = child.p AND parent.k IN ::fkc_vals AND child.p NOT IN ('k2') LIMIT 1 FOR SHARE` | `VerifyChild` |
| CascadeChild / PostVerify | `UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ p SET k = 'k2' WHERE k IN ::fkc_vals` | `CascadeUpdateP` |
| CascadeChild (DELETE plan) | `DELETE FROM p WHERE k IN ::fkc_vals` | `CascadeDeleteP` |
| Parent | `UPDATE gp SET k = 'k2' WHERE k = 'k1'` / `DELETE FROM gp WHERE k = 'k1'` | `ParentStmt` |

The locks come from `getUpdateLock` and `getVerifyLock` in
`go/vt/vtgate/planbuilder/operators/update.go`. The child statements come
from `buildChildUpdOpForCascade` and `createFkChildForDelete`.

The model makes these simplifications. The header of `FkLocking.tla` explains
why each one is sound:

- Each SQL statement is one atomic step that takes all of its locks or waits.
- A gap lock is modelled as a lock on one key value. Real InnoDB gap locks
  cover ranges and so block more.
- Locking reads keep the locks they take, even under READ COMMITTED.
- Any transaction can roll back at any statement boundary. This covers InnoDB
  deadlock victims, `NOWAIT` failures and client rollbacks.
- Gap locks follow InnoDB's rules:
  - Locking reads, `UPDATE` and `DELETE` take gap locks only under
    REPEATABLE READ.
  - InnoDB's own foreign key checks take them at both isolation levels.

## Running

You need Java and `tla2tools.jar` from the
[TLA+ releases](https://github.com/tlaplus/tlaplus/releases); version 1.8.0
was used here.

```sh
cd doc/tla/fk_locking
java -cp tla2tools.jar tlc2.TLC -workers auto -config MC_RC_share.cfg MC.tla
```

`MC.tla` runs all four transaction kinds at the same time. Each `.cfg` file
picks the isolation level and the lock clause used by `VerifyChild`.

## Results

TLC checks `TypeOK`, `LocksConsistent` and `FkIntegrity`, and also checks for
deadlocks.

| Config | Isolation | VerifyChild lock | Result |
|---|---|---|---|
| `MC_RR_share.cfg` | REPEATABLE READ | `FOR SHARE` (current) | No violation (260 distinct states) |
| `MC_RC_share.cfg` | READ COMMITTED | `FOR SHARE` (current) | **`FkIntegrity` violated** |
| `MC_RC_update.cfg` | READ COMMITTED | `FOR UPDATE` | No violation (260 distinct states) |
| `MC_RR_update.cfg` | REPEATABLE READ | `FOR UPDATE` | No violation (260 distinct states) |

### The READ COMMITTED counterexample

Under READ COMMITTED, the cascading `UPDATE` races with a concurrent `INSERT`
into the RESTRICT grandchild. The result is a committed orphan row:

| Step | T1: Vitess, `UPDATE gp SET k = 'k2' WHERE k = 'k1'` | T2: `INSERT INTO c (p) VALUES ('k1')` |
|---|---|---|
| 1 | `SELECT ... FROM gp ... FOR UPDATE`: takes X on `gp('k1')` | |
| 2 | VerifyChild `... FOR SHARE`: takes S on `p('k1')`, finds no child row, takes no gap lock on `c` | |
| 3 | | The FK check takes S on `p('k1')`, which is compatible with T1's S. Nothing blocks the insert into `c`. Commits. |
| 4 | `UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ p SET k = 'k2' ...`: InnoDB does not check `c` | |
| 5 | `UPDATE gp SET k = 'k2' ...`, then commit | |
|   | Committed state: `c.p = 'k1'`, but no row in `p` has `k = 'k1'` | |

Here is why each other configuration is safe:

- **REPEATABLE READ:** step 2 also takes a gap lock on `c` at `'k1'`. That
  lock blocks T2's insert.
- **`FOR UPDATE`:** step 2 takes X on `p('k1')`. That lock blocks T2's
  foreign key check.

The `DELETE` plan is safe at both isolation levels. Its child `DELETE` keeps
`foreign_key_checks` on, so InnoDB enforces RESTRICT with its own locks.

These two interleavings were replayed against MySQL 8.0.46 by running the
statements above directly on MySQL. They did not go through vtgate.

- **READ COMMITTED with `FOR SHARE`:** T2's insert committed and left the
  orphan row `c(10, 'k1')`.
- **Each of the other three configurations:** T2's insert waited on T1's
  locks.

Vitess does not set the isolation level of its MySQL connections. Clients can
set it with `SET transaction_isolation`.

## Not modelled yet

- **Cross-shard and cross-keyspace constraints.** In these, `VerifyParent`
  joins run on more than one MySQL, so the locks are split across servers.
- **Non-literal updates and `SET NULL`.** Both appear in the cascade paths.
- **Partial failure inside an explicit transaction.** The model assumes a
  failed statement rolls back the whole transaction.
