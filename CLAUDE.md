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
- Stage breaking changes across releases: deprecate and warn first, change the default in the next major release, and remove the old behavior in the following major release.

## Formatting

- Run `scripts/fmt <changed-go-files>` before each commit.

## General

- Use `.github/pull_request_template.md` for pull request bodies.
