# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
    - **[Breaking Changes](#breaking-changes)**
        - [VTOrc: `--cells-to-watch` removed in favor of `--cells-no-recovery`](#vtorc-cells-no-recovery)
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

### <a id="breaking-changes"/>Breaking Changes</a>

#### <a id="vtorc-cells-no-recovery"/>VTOrc: `--cells-to-watch` removed in favor of `--cells-no-recovery`</a>

The `--cells-to-watch` flag has been removed. It restricted vtorc's tablet discovery to a fixed set of cells, which created a serious failure mode for any keyspace that spanned cells: if the primary lived in a cell *not* in `--cells-to-watch`, vtorc filtered the primary out of discovery, concluded the keyspace had no primary, and triggered an `EmergencyReparentShard` against a replica in a watched cell. The other cell's vtorc then saw its primary demoted and ran its own ERS — the two vtorcs ping-ponged ERS operations until the keyspace was destroyed. The flag only "worked" under true cell isolation (each cell hosting an independent primary), a configuration with no practical purpose.

The replacement, `--cells-no-recovery`, is a deny-list for *recovery actions only*; vtorc's discovery still spans all cells, so it always sees the real topology. Detection still happens for tablets in listed cells (so operators retain visibility), but actionable recovery functions are skipped with a `CellNoRecovery` reason recorded under the existing `SkippedRecoveries` stat. Non-actionable recoveries (pure detection paths) are unaffected.

**Migration:** drop `--cells-to-watch` from your vtorc invocation. If you previously used it for true cell-isolated deployments, the new flag is not a like-for-like replacement (vtorc will now discover and watch all cells); discuss your scenario in the linked issue if the new flag does not cover your needs.

See [#20021](https://github.com/vitessio/vitess/issues/20021) for details.

## <a id="minor-changes"/>Minor Changes</a>

#### <a id="vreplication-reverse-workflow-data-protection"/>Default data protection for `_reverse` workflow cancel/complete</a>

When calling `cancel` or `complete` on an auto-generated `_reverse` workflow without explicitly providing `--keep-data=false`, the system now defaults to keeping data and returns a warning. This prevents accidental deletion of production tables on the original source side, where the `_reverse` workflow's target is actually your production keyspace.

**Behavior change:**

| Workflow type | `--keep-data` flag | Effective `keep_data` | Warning emitted |
|--------------|-------------------|----------------------|-----------------|
| Normal       | omitted           | `false`              | No              |
| `_reverse`   | omitted           | `true`               | **Yes** |
| `_reverse`   | `--keep-data=false` | `false`            | No              |

The `--keep-data` flag help text has been updated to note this default explicitly. This change applies to MoveTables, Reshard, and other VReplication workflow types that use the shared cancel/complete paths.

See [#19906](https://github.com/vitessio/vitess/pull/19906) for details.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.
