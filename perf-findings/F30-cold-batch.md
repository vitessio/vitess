# F30 cold-path batch triage

Patch: F30-cold-batch.patch. It covers items 6, 12, 2 (histogram only), 7, 11, 13 and 14. All new tests are in `zz_f30_*_test.go` files.
Tests: stats, key, sqlparser, topoproto, srvtopo and the targeted go/mysql tests pass. In the full go/mysql run, only
TestServer, TestServerStats, TestClearTextServer and TestDialogServer fail, because they need a `mysql` client binary (VT_MYSQL_ROOT) that this environment does not have.

## Measured micro A/B (noisy shared 4 vCPU)
| item | old | new |
|---|---|---|
| 12 writePacket (100B payload, buffered) | ~91 ns, 4 B, 1 alloc | ~80 ns, 0 B, 0 alloc (`-gcflags=-m`: "moved to heap: header" at conn.go:827 is gone) |
| 6 Preview (5 typical queries) | 848 ns, 64 B, 5 allocs | 348 ns, 0 B, 0 allocs (-59%, p=0.002) |
| 2 Histogram.MarshalJSON (11 buckets) | ~5.9 us, 4392 B, 20 allocs | ~0.4 us, 416 B, 1 alloc (~14x) |
| 11 TabletAliasString | ~110 ns, 32 B, 2 allocs | ~39 ns, 16 B, 1 alloc |

## Per item
1. logz.Wrappable: real, but only /queryz and /querylogz call it, on input that TruncateForUI has already shortened. Skip. Gotcha: the current `range`+WriteRune turns invalid UTF-8 into U+FFFD; a bulk WriteString would keep the raw bytes.
2. Histogram.MarshalJSON: prototyped (strconv.AppendInt, buffer sized to the bucket count; output is byte-identical, checked by an equivalence test).
   Timings.String json.Marshal re-validation and counters.String `%q` are left alone because F19 touches timings.go and counters.go. Separately, `%q` is not JSON-safe for control characters.
3. BIT_COUNT(binary): real, but vtgate rarely evaluates it. Skip.
4. hex: EncodeBytes cannot use encoding/hex directly because MySQL needs uppercase. DecodeUint makes about 10 allocations through repeated prepend (fill a [10]byte from the end instead), and the input has at most 10 bytes. Only UNHEX/HEX in vtgate use them. Skip or nice-to-have.
5. sqlescape.WriteEscapeID: admin and DDL paths only. Skip.
6. Preview: prototyped. ASCII keywords are lowercased into a stack buffer (hack.String). Non-ASCII input falls back to strings.ToLower so the exact semantics stay (the Kelvin sign lowercases to 'k').
   The fuzz run old against new (180k execs) found no mismatch. It runs about 2x per query in vtgate and about 3x in vttablet, and most of those calls are the eager `trace.AnnotateSQL(span, Preview(sql))`, which is computed even with the noop tracer. Guarding those calls would be a further win.
7. Bitmap.BitCount: prototyped with OnesCount8 per byte, masking the unused bits of the last byte as the comment requires. An equivalence test covers counts 0-70. It runs once per rows event, so the gain is tiny.
8. binlog JSON: the map to switch change is about 10 ns per number, and `make(nodes, 0, elementCount)` saves regrowth. Gotcha: elementCount comes from the binlog, so a corrupt value could cause a huge preallocation (cap it by len(data)). Maybe.
9. parseStmtArgs `%06d`: real (one Sprintf per temporal bind var in COM_STMT_EXECUTE). Replace with zero-padded strconv.AppendInt. Not done because F11 touches query.go.
10. 8bit padding, uca ToLower/ToUpper: real, cold (CHAR padding and LOWER() in vtgate). F14 touches 8bit.go. Skip.
11. TabletAliasString: prototyped with an equivalence test (uid 0 .. MaxUint32). The reserved-connection and healthcheck paths call it, and tabletgateway calls it only on retries. Low value, zero risk.
12. writePacket header escape: prototyped. The rare exact-MaxPacketSize branch now writes its own `[]byte{0,0,0,seq}`, which allocates only on that branch. That saves one heap allocation per packet written (each row sent to a client).
    TestPackets already covers the MaxPacketSize path, and TestF30WritePacketNoAlloc was added. WritePacketHeader (conn.go:437) still escapes (one allocation per streamed packet in plugin_mysql_server:1708). A `[4]byte` scratch field on Conn would fix it; not done.
13. Timings.Reset: confirmed. It swaps the map under RLock. Its only callers are tests and the servenv wrappers used by tests. Fixed with Lock. The -race test fails before the fix (DATA RACE) and passes after.
14. processExactKeyRange: confirmed. sort.SliceStable runs in place on srvtopo's cached `partition.ShardReferences`. That is a race only when the list is unsorted (rebuilt SrvKeyspaces are sorted), and it mutates shared cache state either way.
    Fixed: clone and sort only when `!slices.IsSortedFunc`. The unit test fails on main (the order changes).
15. FetchNext: confirmed. readLenEncStringAsBytes returns `data[pos:pos+s]` with capacity running to the end of the packet. Appending to a Value.Raw() would overwrite the next column (no such caller found).
    Value.CachedSize counts cap(val), so the stream consolidator over-counts each row by roughly cols x packet / 2 and falls back to no consolidation sooner. Fix: `data[pos:pos+s:pos+s]` (one line in encoding.go, which F11 also touches).
    After the fix, the sum of per-value caps equals the packet size, so the accounting becomes accurate. Neither retention nor allocations change. A unit test is easy.
16. PAD SPACE: confirmed divergence. Collate('a','a ') returns non-zero, and Hash differs, for utf8mb4_general_ci, latin1_swedish_ci, utf8mb4_unicode_ci and utf8mb4_bin. MySQL treats these strings as equal. 0900 (NO PAD) is correct.
    It affects evaluation inside vtgate: =/IN/CASE on vtgate-side filters, GROUP BY, DISTINCT and UNION keys (both compare and hash), hash joins and aggregation over sharded results. It does not affect MySQL 8's default collation.
    Fix difficulty M: Collate must treat a trailing all-space tail as equal (also in the prefix mode), and Hash must trim trailing spaces for PAD SPACE collations, while WeightString(0) stays as it is to match MySQL's WEIGHT_STRING. Coordinate with F14's Collate fast path.
17. VReplication: (a) confirmed. For a `select *` rule, buildFromFields -> generate() returns a new TablePlan without ConvertCharset/ConvertIntToEnum (the prelim plan's rules are dropped), so charset and enum conversion is silently skipped. Severity medium (silent data corruption), but only for select * plus convert rules; OnlineDDL uses explicit column lists.
    (b) confirmed by reading the code. In the explicit-list path, colExprs never get isGenerated, but FieldsToSkip does contain the target's generated columns. appendFromRow's skip loop then advances fieldsIndex with no bounds check, which panics with index out of range (or binds shifted values when extra PK columns exist).
    The trigger is a user filter that selects a column that is generated on the target. Severity low-medium: a config mistake crashes the copy phase. Both are unit-testable in replicator_plan_test.
