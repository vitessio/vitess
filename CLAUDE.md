## Style

- Follow [`doc/CodingStyle.md`](doc/CodingStyle.md). It explains the reasoning behind the rules in this file.
- Explain why in comments, not what.
- Link a GitHub issue from every new `TODO`.

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
- Test the error paths and the invalid inputs of the code that you change, not only the success path.

## Errors

- Use `vterrors` for user-facing errors.
- Use the applicable `vtrpcpb.Code`.
- Use `vterrors.Wrapf` to add context to an error.
- Return `vterrors.VT13001` when a request-scoped invariant is violated. Do not call `log.Fatal` or crash the process for it.
- Fail closed when an invariant that protects data integrity is violated: stop the stream or refuse the write.
- Do not discard an error with `_ =` unless a comment explains why that is safe.
- In a `switch` over an enum, handle every value explicitly, and return an error from the `default` case.

## Limits

- Give every network call and every blocking operation a context with a deadline.
- Limit every retry loop, queue, channel, buffer, cache, and per-request fan-out.
- Do not use unbounded or user-controlled values, such as query text, as metric labels.
- Document how every `for {}` loop exits.

## Concurrency and state

- Derive contexts from the caller's context. Do not create a `context.Background()` in a request path.
- Give every goroutine an owner that can stop it and wait for it to exit.
- Do not hold a mutex across an RPC, a MySQL query, a topology call, or another blocking operation.
- Use topology versions or topology locks rather than reading a record and then writing it back.
- Do not mutate a proto returned from a cache or from the topology server. Clone it first.
- Do not release a pooled buffer while a slice still refers to it.

## Release compatibility

- Changes must remain compatible with Vitess versions one major release before and one major release after the current version.
- Stage breaking changes across releases: deprecate and warn first, change the default in the next major release, and remove the old behavior in the following major release.
- Do not change the meaning of an existing protobuf field or topology record field. Add a new field.

## Formatting

- Run `scripts/fmt <changed-go-files>` before each commit.

## General

- Use `.github/pull_request_template.md` for pull request bodies.
