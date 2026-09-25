# Performance investigation notes (working notes, not for merge)

These files record the results of a codebase-wide search for performance improvements.

- `SUMMARY.md`: the overview. It covers every finding (verdict, measured benefit, size, gotchas), the correctness bugs found, and which patches overlap.
- `<ID>.md`: the full report for each finding, with benchmark output.
- `<ID>.patch`: the prototype for each finding, against commit aa9ccf9. Apply it with `git apply perf-findings/<ID>.patch`.
- `BRIEF.md`: the instructions given to each investigator.

Remove this directory before opening any PR from this branch; upstream changes should be cut as separate, focused PRs.
