# F05: binlog CellValue temporal/DECIMAL formatting

## Verdict
**Do it.** The finding is real. The prototype gives byte-identical output (checked by exhaustive, randomized and fuzz equivalence tests against a verbatim copy of the old code), runs **6-11x faster** per temporal/DECIMAL cell, and makes **1 allocation per cell** where the old code made up to 10. The change is local to one file.

## Is it real?
`go/mysql/binlog/rbr.go` `CellValue` (lines 178-811 at aa9ccf9) formats cells as follows:
- DATE, TIME and DATETIME go through `fmt.Appendf`.
- TIMESTAMP and TIMESTAMP2 go through `printTimestamp`. That function builds a `*bytes.Buffer` from `time.Unix(v,0).UTC().AppendFormat(nil, "2006-01-02 15:04:05")`, which parses the layout on every call. TIMESTAMP2 then adds the fraction with `fmt.Fprintf`.
- DATETIME2 uses a `bytes.Buffer` plus `fmt.Fprintf`.
- TIME2 uses `fmt.Sprintf` for the fraction, then `fmt.Appendf("%v%02d:%02d:%02d%v")`, which boxes 5 interfaces.
- DECIMAL runs `make`+`copy` of the input, then a `bytes.Buffer`, then `fmt.Fprintf("%09d")` per 9-digit group, then an extra allocation in `trimPrecedingZeroes`.

Callers, all of which run per row event and per non-NULL cell:
- `vstreamer.getValues` (VStream / VReplication source)
- `go/vt/binlog/binlog_streamer.go` (the legacy update stream)
- `go/mysql/binlog_event_rbr.go` `StringValuesForTests`/`StringIdentifiesForTests`
- `binlog_json.go`, for DECIMAL values inside JSON

Timezone: TIMESTAMP and TIMESTAMP2 are always formatted in UTC (`time.Unix(...).UTC()`). The new code keeps that.

## Prototype (uncommitted, in worktree)
- `go/mysql/binlog/rbr.go`
  - New helpers:
    - `appendUintPadded(b, v, width)`: a 2-digit-table itoa with zero padding, the same output as `%0Nd`/`%.Nd`. A value with more digits than `width` is not truncated, the same as fmt, so garbage inputs match.
    - `appendIntPadded`: the same as `%0Nd` for negative numbers (the sign counts toward the width).
    - `appendDate`, `appendClock`.
    - `appendTimestamp`: uses `t.Date()`/`t.Clock()` rather than `AppendFormat`.
    - `readFractionalSeconds`/`appendFractionalSeconds`: one shared FSP 1..6 reader. An odd FSP gets `/10`, and FSP outside 1..6 prints no fraction, as before.
  - Every temporal case allocates a single slice of the exact size (for TIME2, the size accounts for the sign and 3-4 digit hours).
  - DECIMAL moved to `decimalCellValue()`:
    - It copies the input into a 32-byte stack array. The largest DECIMAL that MySQL supports, (65,28/30), uses 30 bytes; anything larger falls back to the heap.
    - It formats into an 80-byte stack buffer.
    - `trimDecimalLeadingZeroes` does the only heap allocation, at the exact size. Its logic is unchanged from the old closure.
  - Removed the `bytes` and `fmt` imports. `printTimestamp` is gone, replaced by `appendTimestamp`, and its test was updated.
- `rbr_reference_test.go`: a verbatim copy of the old `CellValue`/`printTimestamp`, named `cellValueReference`.
- `rbr_format_test.go`: equivalence tests, a fuzz target and benchmarks (details under Tests).

Size: about 330 changed lines in `rbr.go`, a net +13 lines. The DECIMAL body mostly moved into its own function. No generated code is involved.

## Micro A/B (measured, `-count=10 -benchtime=300ms`, loaded shared 4-vCPU box)
The absolute numbers are about 2x inflated by load. The ratios are the meaningful part.
```
                                old sec/op    new sec/op     delta
CellValueFormat/DATE           351.2n ±23%   58.8n ±68%   -83%   2→1 allocs, 20→16 B
CellValueFormat/TIME2_0        316.0n ±95%   34.6n ±64%   -89%   1→1 allocs, 8→8 B
CellValueFormat/DATETIME2_0    661.1n ±14%   67.1n ±21%   -90%   3→1 allocs, 120→24 B
CellValueFormat/DATETIME2_3    793.4n ±32%   78.2n ±63%   -90%   3→1, 120→24 B
CellValueFormat/DATETIME2_6    750.3n ±13%   84.3n ±12%   -89%   4→1, 128→32 B
CellValueFormat/TIMESTAMP2_6   618.0n ±19%  108.6n ±18%   -82%   6→1, 176→32 B
CellValueFormat/DECIMAL_19_4   491.2n ±14%   77.4n ±18%   -84%   5→1, 133→16 B
CellValueRow (11 cols)         3454n  ±15%   818n  ±17%   -76%   26→9 allocs, 736→160 B
```
An earlier full run (`-count=8`) gave these additional results:
- TIME2_6: -87% (5→1 allocs)
- TIMESTAMP2_0: -78% (5→1)
- DECIMAL_10_2: -70% (4→1)
- DECIMAL_65_30: -86% (10→1 allocs, 204→64 B)
- INT, BIGINT and VARCHAR: unchanged (~), as expected.

The row benchmark decodes 11 columns: BIGINT, INT, 2x VARCHAR, DECIMAL(10,2), DECIMAL(19,4), DATE, DATETIME2(0), DATETIME2(6), TIMESTAMP2(0) and INT.

Rejected alternative (measured): a hand-written Hinnant civil-from-days conversion instead of `time.Unix().UTC().Date()/Clock()` made no measurable difference (51.8 ns vs 55.0 ns, p=0.94). I kept the `time` package because it is simpler and obviously correct.

## End-to-end relevance (estimated)
There is no existing benchmark of the vstreamer, binlog parsing or `CellValue` (I searched `go/mysql`, `vstreamer`, `vreplication` and `go/vt/binlog`), and there is no mysqld in the container for vstreamer tests.
- Per row on the source vstreamer, for a typical OLTP table with 2-5 temporal/DECIMAL columns, this removes roughly 1.3-2.6 µs of CPU (measured on the loaded box) and 3-5 allocations per temporal/DECIMAL cell.
- The rest of the vstreamer per-row path is `getValues` (allocating the values/charsets slices), the filter/`RowToProto3`, and VEvent proto marshal plus gRPC send. I estimate that at roughly 1-3 µs per row for a row of this width, which puts the old `CellValue` at an estimated **30-50% of vstreamer per-row CPU for tables with many temporal/DECIMAL columns**. GC pressure also drops about 4.6x in bytes allocated per row.
- On the vplayer (target) side, apply time is dominated by MySQL writes, so the benefit there is small. The benefit shows up as source-tablet CPU during VReplication copy catch-up, MoveTables, Reshard and VStream consumers (CDC).

## Gotchas
- **Byte-identical, including quirks that are now pinned by tests:**
  - Old `TYPE_TIME` (pre-5.6.4) loses the sign when the hour is 0, so -00:00:30 prints as "00:00:30". This is preserved.
  - Invalid FSP metadata (7 or higher) returns different lengths from `CellLength` (TIMESTAMP2 returns 4, DATETIME2 returns 5, TIME2 returns 3+(m+1)/2). This is preserved.
  - Fraction values larger than the FSP width (garbage input) print all their digits, as fmt did.
  - A negative zero DECIMAL prints "-0.00", as before.
- **Aliasing:** a zero TIMESTAMP (or TIMESTAMP2 with FSP 0) used to return a slice aliasing the global `ZeroTimestamp` through `bytes.NewBuffer`. It now returns a fresh copy, which is strictly safer. Callers that use `bytes.HasPrefix(v, ZeroTimestamp)` are unaffected.
- **DECIMAL with precision 0** (l == 0) still panics on `d[0]`, as before. Invalid metadata (precision > 65) still works through the heap fallback, and the tests cover precision 90, 200 and 255.
- **Portability:** the code is pure Go, uses no unsafe, and has no architecture-specific paths. The stack buffers are small, 32 + 80 + 20 bytes.
- **Release compatibility:** the output bytes are unchanged, so there is no wire or behaviour change.
- `rbr_reference_test.go` is a 690-line verbatim copy of the old code, kept for the equivalence test. A reviewer may prefer to drop it after the change lands and keep only golden cases, or keep it as a regression oracle.
- **Unused helper:** `datetime.DateTime.Format` is not reused. It goes through the generic Strftime path with nanosecond rounding, has different semantics for out-of-range values, and is slower.

## Tests
- Existing tests pass: `go/mysql/binlog` (`TestCellLengthAndData`, the JSON tests with decimals, `TestPrintTimestamp` updated), `go/vt/binlog`, and `go/mysql -run 'Binlog|Row|JSON|Decimal'`.
- Added `TestCellValueTemporalFormatEquivalence`, which compares type, raw bytes, length, error and panic against the old code:
  - every TIMESTAMP day, as LE TIMESTAMP and as TIMESTAMP2 with FSP 0-6
  - FSP 0-8 for TIMESTAMP2, DATETIME2 and TIME2
  - fixed patterns 0x00/0xff/0x80/0x7f and 20k random byte strings per type
  - valid encoders for TIME, DATETIME, DATETIME2 and TIME2, including negative TIME2 with fractions
- Added `TestCellValueDecimalFormatEquivalence`: every (precision 1..65, scale 0..min(p,30)), using valid encodings (leading-zero-heavy, both signs) and random bytes, plus invalid large metadata.
- Added `FuzzCellValueFormat`: 45 s, about 690k execs, no diffs.
- A mutation check confirmed the tests fail when the TIME2 negative-fraction complement is off by one.
- All pass. The equivalence tests take about 4.5 s, which a reviewer may want to trim.
