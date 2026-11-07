## Testing

- Use `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require` for assertions.
- Use `require` when the test cannot continue after a failure.
- Use `assert.Eventually` rather than manual `time.Sleep()` calls and timeouts.
- Use `t.Context()` rather than `context.Background()`.
- Use `t.Cleanup()` for test cleanup.
- Use CI timeouts of at least 30 seconds.
- A test must exercise the condition that its name and documentation claim.
- A test must fail on `main` without the fix that it protects.
- A test must not duplicate a condition that a unit test already covers precisely.

## Errors

- Use `vterrors` for user-facing errors.
- Use the applicable `vtrpcpb.Code`.
- Use `vterrors.Wrapf` to add context to an error.

## Release compatibility

- Changes must remain compatible with Vitess versions one major release before and one major release after the current version.
- Deprecation requires advance warning. Use as many release phases as provide real compatibility value:
  - Command-line flags require three releases because removing a still-configured flag prevents the binary from starting: release N warns, release N+1 keeps accepting the flag as a no-op, and release N+2 removes it.
  - Behavior or default changes that can preserve the old behavior behind a compatibility flag also use three releases: release N announces and warns with the default unchanged, release N+1 may flip the default while preserving the old behavior behind the deprecated flag, and release N+2 removes the flag and old behavior.
  - Other removals may use two releases when a third phase has no practical compatibility benefit: release N warns, and release N+1 removes or fails fast as unsupported. This requires explicit maintainer agreement.
  - Never remove or change a default in the same release that introduces the deprecation warning.

## Formatting

- Run `scripts/fmt <changed-go-files>` before each commit.

## General

- Use `.github/pull_request_template.md` for pull request bodies.
