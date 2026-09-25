# Common brief for finding investigators

You are investigating ONE performance (or correctness) finding in the Vitess repo. You run inside your own
git worktree of vitessio/vitess (your current working directory). Go toolchain: 1.27.1 (from go.mod).
Machine: 4 vCPU Xeon (AVX-512), SHARED with several other agents benchmarking concurrently, so absolute
numbers are noisy; always compare A/B in an interleaved way and report ratios.

## Rules
- Work ONLY inside your worktree (and the scratchpad paths below). Never touch /home/user/vitess itself.
- Do NOT commit, push, or open PRs. Leave your prototype as uncommitted changes in your worktree.
- Before any go build/test: `export GOFLAGS=-trimpath` (lets all worktrees share the Go build cache).
- benchstat is at /tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/bin/benchstat
- A/B method: prefer keeping the old implementation as a copy in a _test.go file (or `git stash` A/B) and run
  `go test -run '^$' -bench X -count=8 -benchmem`, then benchstat old vs new. Keep benchmark runs short
  (-benchtime=200ms..1s); do not hog the CPU for more than a few minutes total.
- Repo conventions (CLAUDE.md): testify assert/require; t.Context(); vterrors for user-facing errors;
  changes must be compatible with one major release before and after; run `scripts/fmt <files>` on changed Go files.
- Correctness first: run the existing unit tests of every package you touch and add an equivalence/fuzz-style
  test where the change is a rewrite of an encoder/decoder/formatter.

## What to determine
1. Is the finding real? Re-verify the code and callers at the cited lines (they may be slightly off).
2. Actual benefit: (a) microbenchmark A/B of the function itself; (b) end-to-end relevance: what share of a
   realistic path does it represent (use an existing package benchmark or a CPU profile of one if available;
   otherwise reason from call frequency). Say clearly which numbers are measured vs estimated.
3. Implementation difficulty: S/M/L, approx LOC, files touched, whether generated code / generators are involved.
4. A prototype implementation (sketch is fine if full implementation is too large; make it compile if you can).
5. Gotchas: correctness edge cases, behaviour changes, memory retention/aliasing, release compatibility,
   portability (arm64), concurrency, test coverage gaps, anything a reviewer would flag.

## Deliverables
- Save the diff: `git diff > /tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/findings/<ID>.patch`
  (include new files: `git add -N .` first).
- Write a detailed report to .../scratchpad/findings/<ID>.md (benchstat output, profile excerpts, sketch, gotchas).
- Your final reply (to the orchestrator) must be <= 400 words, in this shape:
  VERDICT: do it / maybe / skip — one line why
  BENEFIT: measured micro A/B (ratio + ns/allocs), end-to-end estimate
  DIFFICULTY: S/M/L, ~LOC, files
  SKETCH: 2-5 lines describing the approach
  GOTCHAS: bullet list
  TESTS: what exists / what you added, pass/fail
