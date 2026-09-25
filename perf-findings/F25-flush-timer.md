# F25: flush timer Reset on every buffered packet (go/mysql/conn.go)

## Verdict
Do it. The change is small, measurably faster (about 40% on the buffered writeRow path), and the new
semantics (flush at most `flushDelay` after the first unflushed write) match the documented meaning of
`--mysql-server-flush-delay` ("Delay after which buffered response will be flushed to the client")
better than the old debounce did.

## Is the finding real?
Yes. `startFlushTimer()` runs under `bufMu` on every buffered write: `writePacket` (every row, column
definition, EOF/OK packet), `WritePacketHeader`, `WritePacketRaw`, and `WritePacketDirect` (in replication.go).
It called `c.flushTimer.Reset(c.flushDelay)` every time. Buffering is enabled for every command that
returns data (ComQuery, ComStmtExecute, ComBinlogDump*, and similar; see the `startWriterBuffering` callers in conn.go),
and the buffer is flushed explicitly by `endWriterBuffering` at the end of the command, or by `FlushWriteBuffer`
(vtgate's binlog stream, after each gRPC response). The buffer is a 16 KiB bufio.Writer, which also flushes
by itself when it is full.

So the timer only matters when a single command has a gap in writing: a streaming query whose source is
slow (for example OLAP streaming through vtgate, where rows arrive in gRPC chunks), or the time between writing
the fields and the first rows. For ordinary non-streaming results, all rows are written in one burst and
`endWriterBuffering` stops the timer. The timer never fires, but the old code still paid one `Reset` per packet.

## Semantics: old compared with new
- Old (debounce): the timer fires `flushDelay` after the LAST write. If the writer writes more often than
  every `flushDelay` (default 100 ms), the timer never fires. Data leaves only when 16 KiB fills up or the
  command ends. Pathology: a trickle of one 100-byte row every 90 ms is not flushed for about 164 rows, which
  is about 15 s of latency.
- New (bounded latency): the timer is armed on the first write after a flush and is not pushed back. Buffered
  data leaves at most about `flushDelay` after it was written.
- Cost of the change: a connection that streams continuously gets at most `1/flushDelay` extra partial
  flushes per second, which is 10 per second at the default. A fast stream already flushes once per 16 KiB
  (for example, 3,000 flushes per second at 50 MB/s), so the extra flushes add about 0.3% more syscalls. A slow
  stream gets the extra flushes as intended lower latency. This is not a flush storm. The new test asserts
  `writes <= elapsed/delay + 1`.
- Each callback takes `bufMu` briefly. At most 10 callbacks per second per streaming connection, which is
  negligible.

## Alternatives considered
- Lazy debounce with a `lastWrite` timestamp: `time.Now()` costs about 50 ns on this VM and `time.Since` about 27 ns,
  measured. `Timer.Reset` costs about 46 to 52 ns, also measured. That saves little to nothing, so it is rejected.
- Lazy debounce with a write counter (`c.writeSeq++` per packet; the callback re-arms for a full `flushDelay`
  if the counter changed): costs about the same as the chosen approach and keeps the debounce semantics, but the flush then lands
  between 1x and 2x `flushDelay` after the last write, and the callback runs every `flushDelay` for the
  whole stream. It is a fallback if reviewers want to keep the debounce. The chosen approach has better latency, so I prefer it.

## Implementation (prototype in the worktree, uncommitted)
- `Conn.flushPending bool`, protected by `bufMu`.
- `startFlushTimer`: returns at once if `flushPending` is set. Otherwise it sets the flag and calls `AfterFunc`/`Reset`.
- `onFlushTimer` (callback): takes `bufMu`, clears the flag, and flushes if `bufferedWriter != nil`.
- `stopFlushTimer` (used by `endWriterBuffering` and `FlushWriteBuffer`): clears the flag and calls `Stop`.
- Size: S. conn.go is +39/-17. The new test file conn_flush_test.go has about 170 lines, including 2 tests and 2 benchmarks.

### Race and invariant analysis
Invariant: if the buffer holds unflushed data, then `flushPending` is true and a timer fire is scheduled or
in flight. The flag is set only together with arming the timer, all under `bufMu`. It is cleared in only three places:
- by the callback, after it flushes under `bufMu`;
- by `endWriterBuffering`, after it flushes;
- by `FlushWriteBuffer`, after it flushes.

A stale in-flight callback whose `Stop` failed can clear the flag
after a new arm. It flushes everything under the lock when it does, and the re-armed timer still fires
later. The worst case is one extra, early flush, which the old code could also do. `go test -race` passes on the conn,
streaming and flush tests.

## Measurements (measured; noisy shared 4-vCPU machine; old and new binaries interleaved, n=10)
BenchmarkBufferedWriteRows writes 1000 two-column rows through writeRow/writePacket, then calls endWriterBuffering. The
sink is a discarding net.Conn.

```
                            │   old3.txt    │               new3.txt               │
                            │    sec/op     │    sec/op     vs base                │
BufferedWriteRows-4           131.50µ ± 68%   77.61µ ± 29%  -40.98% (p=0.000 n=10)
BufferedWriteRowsParallel-4   132.68µ ± 45%   77.66µ ± 25%  -41.47% (p=0.001 n=10)
```
Two earlier independent runs gave -38.7% (p=0.007) and -44.5% (p=0.000). That is about 55 to 65 ns per row saved,
on a path that costs about 80 to 150 ns per row. Allocations do not change (1 alloc per row, see the unrelated finding below).

End-to-end (estimate, not measured): vtgate's per-row cost when it sends large results to a MySQL client is
dominated by gRPC decoding and sqltypes handling upstream, roughly 0.5 to 2 µs per row. 60 ns per row is therefore about 3 to 10% of the
MySQL-protocol write side and a low single-digit percent of vtgate CPU for row-heavy result sets. It also removes one runtime timer-heap
operation per packet, which touches per-P timer locks under many concurrent connections.

## Tests
- New: `TestFlushTimerFlushesIdleBuffer` checks that 2 rows are flushed by exactly one timer flush when the writer is idle, and that the timer
  re-arms after it fires. It passes on both old and new code and guards against regressions.
- New: `TestFlushTimerBoundsLatencyWhileStreaming` writes a row every 1 ms with `flushDelay` = 50 ms. It asserts that the
  first flush is a timer flush (smaller than 16 KiB) and not a full-buffer flush, and that the flush count is at most
  elapsed/delay + 1. It FAILS on main: "first flush came from a full buffer after 1639 packets". It passes with the fix.
- `go test ./go/mysql`: the only failures are TestServer, TestServerStats, TestClearTextServer and
  TestDialogServer. They fail identically on main because this environment has no `mysql` client binary (VT_MYSQL_ROOT).
- The race subset passes (conn, stream, flush, packets and binlog tests).
- `go test ./go/vt/vtgate -run 'Binlog|Flush|MysqlServer|Stream'` passes.
- `go vet` and `scripts/fmt` are clean.

## Gotchas
- This is a visible behaviour change: flushes happen during continuous streaming, about 10 per second per connection, where before
  there were none until 16 KiB filled up. Clients see partial results sooner. There is no protocol or compatibility impact,
  because packet boundaries and content are unchanged and only TCP write timing changes. It is compatible across versions.
- For a slow trickle, clients may now receive more small TCP segments. At 100 ms granularity this is bounded.
- Stale callback after `Stop` fails: the same as before (it may flush the next command's buffer early). It is harmless.
- The benchmarks in conn_flush_test.go can be dropped before a PR. The two tests should stay.

## Unrelated finding spotted along the way (not in the patch)
`writePacket` allocates 4 B on the heap for every packet: `var header [PacketHeaderSize]byte` escapes
(`go build -gcflags=-m` reports "moved to heap: header" at conn.go:849). It escapes only because the rare
MaxPacketSize-boundary path passes `header[:]` to `w.Write` through an interface. `WritePacketHeader` (conn.go:436) has
the same problem. Fix: use a separate small array for the zero-length-packet write inside that rare branch, or
use the existing `c.header` field. That removes 1 alloc per row sent (1000 allocs/op in the benchmark above).
