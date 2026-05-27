# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
    - **[Breaking Changes](#breaking-changes)**
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)
    - **[VTGate](#minor-changes-vtgate)**
        - [New controls for cross-keyspace reads](#vtgate-cross-keyspace-reads)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Consolidator Reject on Waiter Cap](#vttablet-consolidator-reject-on-cap)
    - **[VTTablet](#minor-changes-vttablet)**
        - [Schema engine table-count limit is now configurable](#vttablet-schema-max-table-count)
    - **[VTCtld](#minor-changes-vtctld)**
        - [EmergencyReparentShard tolerates partial relay-log-apply failures](#vtctld-ers-partial-relay-log)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

### <a id="breaking-changes"/>Breaking Changes</a>

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

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-cross-keyspace-reads"/>New controls for cross-keyspace reads</a>

VTGate now supports preventing cross-keyspace reads (joins and UNIONs), preventing queries that would combine data from different keyspaces. This can be configured at two levels:

**VTGate flag** (applies to all queries):

```
--prevent-cross-keyspace-reads
```

**Per-keyspace VSchema setting** (applies to specific keyspaces):

```bash
vtctldclient ApplyVSchema --vschema='{"prevent_cross_keyspace_reads": true}' my_keyspace
```

When enabled, the planner will reject queries that require joining or combining (via UNION) tables from different keyspaces. This can be overridden on a per-query basis using the `ALLOW_CROSS_KEYSPACE_READS` comment directive:

```sql
/*vt+ ALLOW_CROSS_KEYSPACE_READS */ SELECT * FROM ks1.t1 JOIN ks2.t2 ON t1.id = t2.id;
```

The VTGate flag prevents cross-keyspace reads globally, regardless of per-keyspace VSchema settings.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-consolidator-reject-on-cap"/>Consolidator Reject on Waiter Cap</a>

A new `--consolidator-reject-on-cap` flag (default `false`) has been added to VTTablet. When enabled alongside a non-zero `--consolidator-query-waiter-cap`, queries that would join a consolidated result but exceed the **global** consolidator waiter cap are rejected with a `RESOURCE_EXHAUSTED` error instead of silently falling back to independent MySQL execution.

**Important:** The cap is enforced against the consolidator's global `totalWaiterCount` across all queries, not a per-query waiter count. This means a duplicate for query B can be rejected because query A has already consumed most of the global waiter budget. This provides backpressure when the consolidator as a whole is saturated, rather than when any single query has too many waiters.

See [#19836](https://github.com/vitessio/vitess/pull/19836) for details.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="vttablet-schema-max-table-count"/>Schema engine table-count limit is now configurable</a>

Previously the schema engine had a hardcoded cap of 10,000 tables: a vttablet whose underlying MySQL had more than 10,000 tables would fail to load its schema and could not serve queries. This made recovery from `EmergencyReparentShard` impossible without dropping tables directly on MySQL.

Two changes:

1. The schema engine no longer enforces a row cap on its reload queries. A vttablet with any number of tables will load successfully.
2. A new flag, `--queryserver-config-schema-max-table-count` (default `10000`), governs new schema object creation for tables and views. `CREATE TABLE` and `CREATE VIEW` statements that would push the engine's tracked schema-object count above this limit are rejected at vttablet with a clear error before they reach MySQL. The flag is dynamic: changes are observed without restart.

Tablets that already have more tracked schema objects than the configured limit will reload fine — only new creations are gated. Operators who need to support more tables and views should increase the flag and ensure both vttablet and mysqld have enough memory to comfortably hold the larger schema.

See [#19978](https://github.com/vitessio/vitess/issues/19978) for details.

### <a id="minor-changes-vtctld"/>VTCtld</a>

#### <a id="vtctld-ers-partial-relay-log"/>EmergencyReparentShard tolerates partial relay-log-apply failures</a>

`EmergencyReparentShard` (ERS) on GTID-based shards no longer fails when only some replicas can apply their relay logs. As long as at least one tablet at the leading `Combined` GTID position applies successfully, ERS proceeds; lagging or stuck-SQL-thread replicas are no longer blockers. Pre-existing pre-PR behavior is preserved for non-GTID flavors (FilePos, MariaDB), where ERS still requires every candidate to apply.

When the leading GTID-based candidates have incomparable `Combined` positions (suspected split-brain), ERS now aborts upfront with a clear `FAILED_PRECONDITION` error naming the diverged tablets, rather than silently picking one side. Pre-PR ERS would pick blindly and let the losing side's unique GTIDs become errant on those tablets — a silent data-integrity incident that surfaced later via lag alerts or downstream consistency checks. See [#20199](https://github.com/vitessio/vitess/issues/20199) for the bug this addresses.

A new `--allow-split-brain-promotion` flag is added to `vtctldclient EmergencyReparentShard` (and `--allow_split_brain_promotion` on the legacy `vtctl`). It is **off by default**. Operators who deliberately need to force ERS through a detected split-brain — typically because they already know which side to keep and plan to re-clone the losing side — can set it to convert the abort into a `WARN` log and proceed. The losing side's unique GTIDs will become errant after promotion, so this is an explicit operator override, not a default-on safety knob.

Three new stats are exported for observability:

- `EmergencyReparentFilteredCandidates` — counts replicas excluded from the relay-log wait because their `Combined` position is strictly behind the leading group.
- `EmergencyReparentRelayLogFailedCandidates` — counts replicas that genuinely failed to apply relay logs (cancellations after a peer succeeded are not counted).
- `EmergencyReparentSplitBrainOverrides` — counts ERS runs that proceeded despite detected split-brain because `--allow-split-brain-promotion` was set. Stays at zero unless an operator has deliberately invoked the escape hatch.

See [#18707](https://github.com/vitessio/vitess/pull/18707) for details.
