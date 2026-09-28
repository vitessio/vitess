## Changelog

**By default, a PR does not add an entry to the release summary (`changelog/<major>/<version>/summary.md`).**

Add an entry only for a change that users or operators need to know about, or act on, when they upgrade:

- New features, flags, configuration options, and APIs
- Changed defaults
- Deprecations, removals, and other breaking changes
- Behavior changes that are not bug fixes, such as stricter validation that rejects input Vitess used to accept

Everything else stays out of the release summary:

- Bug fixes, unless users or operators have to change something when they upgrade
- Refactors, and performance improvements that add no new setting
- Tests, CI, build tooling, and dependency bumps

Every merged PR appears in the full release changelog, which is generated when the release is cut in GitHub.
