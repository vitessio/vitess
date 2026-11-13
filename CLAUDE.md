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
- Deprecation requires advance warning. Use as many release phases as provide real compatibility value, where N, N+1 and N+2 are consecutive major releases:
  - Command-line flags use three releases by default because removing a still-configured flag prevents the binary from starting: release N deprecates the flag and warns when it is set, release N+1 still accepts it, and release N+2 removes it. A flag that still controls supported behavior in N+1, such as a renamed flag or a compatibility flag, keeps working there, and becomes a no-op only once that behavior is gone.
  - Behavior or default changes that can preserve the old behavior behind a compatibility flag also use three releases: release N adds the flag and warns with the default unchanged, so operators can try the new behavior or pin the old one, release N+1 may flip the default while the deprecated flag preserves the old behavior, and release N+2 removes the flag and the old behavior.
  - Other removals may use two releases when a third phase has no practical compatibility benefit, and so may a command-line flag removal when the PR justifies skipping the middle release: release N warns, and release N+1 removes or fails fast as unsupported. This requires explicit maintainer agreement.
  - Never remove or change a default in the same release that introduces the deprecation warning. Experimental features are exempt, and so are security fixes with explicit maintainer agreement.

## Formatting

- Run `scripts/fmt <changed-go-files>` before each commit.

## General

- Use `.github/pull_request_template.md` for pull request bodies.
