# Round-1 survey: what the four initial search agents checked

Four agents searched the codebase for byte-level and allocation-level tuning opportunities, split into go/mysql, parser/sqltypes/evalengine, vtgate/vttablet, and everything else. Every candidate they ranked became one of the F01–F30 investigations (see `SUMMARY.md`). This file records what they **checked and rejected**, plus the ideas that weren't pursued, so nobody redoes them.

## Checked and rejected (already optimal, cold, or not worth it)

**go/mysql:**
- `encoding.go`: already bulk `copy`, `bytes.IndexByte` for NUL-terminated strings, and unrolled lenenc handling.
- `readEphemeralPacket`: the multi-packet `append` only happens for rows over 16 MB.
- `writeRow`: the pooled-buffer → bufio copy is a cheap memmove.
- Binlog CRC32 is stripped, not verified. zstd is a library.
- `decimal` scan/format already work in 9/19-digit word chunks.
- `fastparse`: inputs are short and need overflow checks, so SWAR 8-digit parsing gains little.
- JSON `Parser`: `parseRawString` and `unescapeStringBestEffort` already use `IndexByte`, and `skipWS` has a fast path. `parseRawKey` is a byte loop, but keys are short.
- uca0900 `Hash`/`Collate`/`WeightString` already use the 64-bit block iterator. `collationBinary` already uses `bytes.Compare`, and `_bin` 8-bit hashing already writes in bulk.
- utf8mb3/mb4 `Length` (`utf8.RuneCount`) looked fast enough here; F29 later found it allocates, see `BUGS.md`.
- Binlog `Rows()` value extraction is zero-copy. `isZeroDateTime` is trivial. datetime parse and strftime use local append helpers.
- sqlerror, auth, handshake, flavor and schema are cold.

**Parser / sqltypes / evalengine:**
- `SplitMarginComments` and `StripLeadingComments` only walk the margins.
- `TruncateQuery` doesn't allocate when there are no comments.
- `NewIdentifierCI` → `strings.ToLower`: the stdlib skips allocation for lowercase ASCII.
- `compare`/`NullsafeCompare` have a same-type fast path, and tiny weights cover numerics.
- `NullsafeHashcode128`, `evalWeightString` and `TinyWeighter` are fine.
- LIKE: constant patterns are precompiled, and exact/prefix patterns use `fastMatcher`. A byte-level skip would only be safe for `_bin`, and vtgate rarely evaluates LIKE.
- `fn_string` trim/replace/repeat already use stdlib `bytes`. evalengine JSON functions have no byte loops.
- `Directives`, `GenerateQuery`/`Append`, `BufDecodeStringSQL`, `go/bytes2`, `go/hack`, `go/textutil`, `CopyRow`, `ForEachValue`: reasonable already, or cold.
- The goyacc `Parse` loop (22% flat) is generated code, so it was out of scope here. P6 later found its 23 KB stack frame.

**vtgate / vttablet / key / binlog:**
- `key.Normalize`/`Compare`: loops run about once.
- `PlanKey.Hash` already uses highway hash (AVX2); theine `StringKey` uses `maphash`.
- Vindexes: xxhash 18 ns; binary_md5, numeric and reverse_bits are trivial; the unicode_loose_* costs are in collations.
- Hash join, DISTINCT, memory sort and ordered-aggregate comparisons are in evalengine/sqltypes.
- Query rules `FilterByPlan` runs at plan-cache time. Plan-key concatenation, `logstats.SizeOfResponse`, schema, twopc and messager loops are per query or cold.
- Lookup vindex `ToString` keys: network round trips dominate. `binlog/keyrange_filter` is the legacy update stream.
- `RowToProto3Inplace` already bulk-copies. `joinRows` does one allocation per output row (minor). Buffer and heartbeat are cold.
- The vstreamer IN filter compares linearly per row; a hash set would only matter for large IN lists.

**Everything else under go/:**
- vtproto `Row.UnmarshalVT` packed lengths: a SWAR count gave no gain, because allocation dominates.
- Hash algorithm choices: tmutils `crc64.ISO` (cold), FNV in vtctl/wrangler (admin), statsd crc32 per flush, discovery checksums (small).
- `Histogram.Add` is a linear scan over ~10 cutoffs. It's fine, although its shared atomics can contend.
- Cold: `RingInt64.Values`, prometheus collectors, opentsdb `sanitize`, `fileutil.HasWildcard`, `topo/validator`, `flagutil`, `tableacl.Authorized` (cached).
- No byte loops found in smartconnpool, the srvtopo resolver, `streamlog.ShouldEmitLog` (`strings.Contains`) or logutil.
- Backup I/O buffers are adequately sized. lz4's `ReadFrom` is deliberately hidden.
- `topoproto.TabletAliasString` `Sprintf` is minor (F30 #11).

## Candidates not turned into investigations

- **DES in the `hash` vindex:** 87 of 120 ns per id; bitsliced SIMD batching is speculative (see `SIMD.md`).
- **`ResolveDestinations` closure allocation per destination:** folded into F04.
- **Hash join `joinRows`:** a slab allocation would help, but the gain is minor.
- **`hex.EncodeBytes` 256-entry uint16 table, `DecodeUint` prepend allocation:** cold (F30 #4).
- **`Histogram` shared atomics:** a scalability concern under heavy concurrency, not measured.
