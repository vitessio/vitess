# F01-sketch-reset: CountMinSketch.reset leaks bits between packed 4-bit counters

## Verdict
Do it, and fix `indexOf` in the same change. The reset bug is real, but on its own it is almost
invisible, because a second bug in `indexOf` hides it. Both were fixed upstream in theine-go in
October 2024 (#53 "fix sketch reset" and #54 "improve sketch index select"). Vitess copied the
code in 2023 and never took these fixes.

## 1. The bug is real
`go/cache/theine/sketch.go` packs sixteen 4-bit counters into each uint64. `reset()` did
`Table[i] >> 1`, which moves bit 0 of counter p+1 into bit 3 of counter p.

**The second bug that hides it.** `indexOf` takes both of these from the same bit of the counter hash:
- the word: `index = block + counterHash&1 + 2*offset`
- the counter within the word: `(counterHash & 0xF) << 2`

As a result, even words only use even counters and odd words only use odd counters. So:
- Half of the sketch is dead. Its real width is half its size.
- A bit that leaks on reset lands first in a counter that is never read.
- The leaked bit keeps shifting down through that unused counter. At the 5th reset it lands in bit 3
  of the next used counter below.

The result is that a frequency is still made up, but only four resets after the fact, and with some
probability. That explains why the obvious single-reset Estimate test passes on main.
`TestSketchResetEstimates` fails at "round 4" (the 5th reset) with estimate 8 where 0 was expected.
This matches the analysis.

## 2. Upstream (github.com/Yiling-J/theine-go, cloned)
- **14e5761 (#53, 2024-10-20), "fix sketch reset".** `(v>>1) & 0x7777777777777777`. It also changes
  `Additions = (Additions - popcount(v & 0x1111..)>>2) >> 1`, which is the Caffeine-style correction
  for truncation.
- **f8e7c8a (#54, 2024-10-21), "improve sketch index select".** Row r uses byte r of the counter hash:
  bit 0 picks the word and bits 1-4 pick the counter. It also sets `SampleSize = 10*newSize`
  (was `10*size`). A later commit changed `rehash` to `h*0x94d049bb133111eb; h^=h>>31`.
- Vitess's copy matches upstream from before the #51 refactor (2023). The license header is dual
  "Vitess Authors / Yiling-J", Apache-2.0. Vitess has diverged a lot since then: no window LRU, and
  hill-climbing `lruFactor` instead of the adaptive window from upstream #55. So a wholesale re-sync
  is out of scope.

## 3. Hit-ratio impact (measured)
**Setup.**
- Real `theine.Store` (the Get/Set path the plan cache uses), cost 1, no doorkeeper, single
  goroutine.
- Key space 2^20. 3M ops (4M for the shifting workload), with the first 500k not counted.
- 3 seeds per cell. The spread between seeds was 0.1pp or less, except the shifting workload at
  about 0.3pp.
- The simulator is in the scratchpad at `F01_zz_sim_test.go.txt` and is not part of the patch.

**Workloads.**
- zipf 1.01 and zipf 1.2.
- zipf 1.05 mixed with 30% one-hit scans.
- zipf 1.05 whose popular keys shift every 500k ops. This is the aging-sensitive one.

**Columns.**
- buggy: the code on main.
- mask: only the proposed fix.
- mask+idx: the patch.
- full: the patch plus the Additions correction and `SampleSize = 10*newSize`.

```
  size workload              buggy    mask mask+idx   full   d(mask) d(mask+idx)
  1000 zipf-shifting         57.15   57.21    57.62  57.68   +0.06pp  +0.47pp
  1000 zipf1.05+30%scan      41.06   41.07    41.07  41.10   +0.01pp  +0.01pp
  1000 zipf1.01              51.50   51.55    51.54  51.58   +0.05pp  +0.04pp
  1000 zipf1.2               80.74   80.79    80.76  80.79   +0.05pp  +0.02pp
 10000 zipf-shifting         66.72   67.23    68.49  68.50   +0.51pp  +1.77pp
 10000 zipf1.05+30%scan      50.48   50.51    50.71  50.76   +0.03pp  +0.23pp
 10000 zipf1.01              67.08   67.08    67.22  67.22   -0.01pp  +0.13pp
 10000 zipf1.2               89.88   89.89    89.92  89.89   +0.01pp  +0.04pp
```

**Takeaways.**
- The mask alone gains 0 to 0.5pp (the most is on the shifting workload).
- Mask plus the decorrelated index gains up to +1.8pp on a changing workload at 10k entries, and
  0.0 to 0.2pp on steady zipf.
- The full upstream sync adds nothing measurable beyond that.
- CPU cost: reset adds one AND per word, and indexOf now does a shift instead of a multiply-add.
  Both are negligible; no benchmark was run.

## 4. Other packed-counter or bit mistakes in go/cache/theine
- **`bf/bf.go` (doorkeeper).** A plain bitvector with `Reset` zeroing it. No packed counters and no
  problems found. `k = 0.7*m/n` is about ln2·m/n, which is fine.
- **`inc`, `count`, `Estimate`.** The masks and shifts are correct. `uint64(0xF << offset)` is typed
  uint64 because the constant takes the conversion's type.
- **`EnsureCapacity`.** Sets `SampleSize = 10*size` rather than `10*newSize`. The effect is minor:
  it resets more often when the size is not a power of two. Not changed.
- **`Additions` after reset.** Is not corrected for truncation (upstream now is). No measurable
  effect. Not changed, so the existing `TestSketch` assertion still holds.

## 5. Patch (`F01-sketch-reset.patch`)
- **`sketch.go`.**
  - `reset`: `(v>>1) & resetMask`.
  - `indexOf`: `ch := h >> (offset<<3)`, word = `ch&1`, counter = `((ch>>1)&0xF)<<2`.
  - `rehash`: the upstream 64-bit constant.
- **`sketch_test.go`.** Adds three tests. All three fail on main and pass with the patch:
  - `TestSketchResetHalvesEachCounter`: table-level, each nibble is halved on its own.
  - `TestSketchResetEstimates`: 6 resets in a row; each Estimate is exactly the previous one >> 1.
  - `TestSketchUsesAllCounters`: after 100k adds, all 16 counters of every word are non-zero.
    On main, half of them are always zero.
- **Tests run.** `go test ./go/cache/theine/...` and the vtgate cache/plan tests pass. `go vet` is
  clean, and `scripts/fmt` was run.

## Gotchas
- **Must ship together.** The mask and the index fix belong in one change. The index fix without
  the mask fix would make the reset bug show up right away on every reset.
- **Compatibility.** The sketch is in-memory only: no persistence and nothing on the wire. Changing
  the hash layout does not affect compatibility across versions.
- **Effect on callers.** Plan caches in vtgate and vttablet will see slightly different
  admission/eviction decisions. Tests that assume exact eviction order could change, but none in
  theine did.
- **Bigger upstream improvements.** The adaptive window LRU (#55) was not investigated here and
  would be a much larger port.
- **Doorkeeper.** Hit ratio with the doorkeeper enabled was not simulated. The doorkeeper only
  gates Set and does not change the sketch.
