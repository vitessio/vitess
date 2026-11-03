# Vitess Coding Style

This guide describes how we write Go code for Vitess, and why. It is adapted from
TigerBeetle's [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md),
and keeps its goals while changing the rules that assume a single process that owns its own disk,
memory, and clock.

The short, enforceable version of these rules lives in [`AGENTS.md`](../AGENTS.md). This document
explains the reasoning, so that you can apply it to cases that the rules do not cover.

## Goals

Our design goals are **safety, performance, and developer experience, in that order**.

Within safety, **data integrity comes before availability**. A Vitess cluster that stops serving is a
severity 1 incident. A Vitess cluster that returns wrong rows, loses writes, or routes a write to the
wrong shard is also a severity 1 incident, and it is harder to detect and to recover from (see
[`SEVERITY.md`](../SEVERITY.md)). When the two conflict, fail closed.

Style matters because it serves these goals. Readability is the minimum. The question to ask about a
change is whether it makes Vitess more or less safe, fast, and easy to work on.

### How Vitess differs from TigerBeetle

TigerStyle was written for a single-binary database with a deterministic simulator. Vitess is:

- **A long-running, multi-tenant server.** One vtgate serves many applications. Crashing it to stop
  one bad query is an outage for all of them.
- **A distributed system.** State lives in MySQL, in the topology server, and in caches on every
  vtgate and vttablet. Every cached copy can be stale, and every read-then-write can race.
- **Run in mixed versions.** During an upgrade, version N and N+1 run side by side. Protobuf
  messages, topology records, and flags are a persistent format.
- **Large and community-maintained.** Hundreds of contributors work on it, so rules must be
  checkable in review and, where possible, by linters.
- **Written in Go.** Go has a garbage collector, goroutines, `context.Context`, and `gofmt`, and it
  has no `assert`.

## Safety

### Distinguish programmer errors from operating errors

An **operating error** is expected: a tablet goes away, a query times out, a user sends invalid SQL,
a lock is held by someone else. Handle it, and return a `vterrors` error with the applicable
`vtrpcpb.Code`.

A **programmer error** is a violated invariant: a code path that "cannot happen" did happen. It
means that our understanding of the code is wrong, and that any further work built on that
understanding may be wrong too. Never continue silently past a programmer error.

### Contain the blast radius of a violated invariant

TigerStyle crashes the process on any assertion failure. Vitess scopes the reaction to the smallest
unit that is known to be affected:

| What is broken | What to do |
| --- | --- |
| One query, plan, or expression | Fail the request with `vterrors.VT13001`. It carries `Code_INTERNAL` and the `[BUG]` prefix. |
| One stream or workflow (VReplication, Online DDL, VDiff) | Stop that stream in an error state that an operator can see. Do not skip ahead, and do not "repair" the data silently. |
| Process-wide state, or anything that can corrupt data (for example, two serving primaries, a replication position that goes backwards, a key range gap) | Fail closed: stop serving, or refuse the write. |

```go
if route.Keyspace == nil {
	return nil, vterrors.VT13001("route without a keyspace")
}
```

A violated invariant that does not crash the process must still be visible. Make sure that it
increments an error counter and logs enough context to debug it. Otherwise "fail the request" turns
a bug into a silent one.

Use `panic` only inside code that has a recovery boundary that converts it into an error, such as
the planner and its `operators.PanicHandler`. An internal recovery boundary converts only the
`error` values that the code panicked with on purpose. It re-panics on `runtime.Error`, such as a
nil dereference or an out-of-range index, so that the unexpected failure reaches the RPC-level
`HandlePanic`, which logs the stack, counts the panic, and fails the request.

### Check invariants at trust boundaries

Do not aim for a fixed number of checks per function. Put checks where data crosses a boundary and
where a mistake would be expensive:

- RPC and protocol input (MySQL protocol packets, binlog events, gRPC requests).
- Reads from and writes to the topology server.
- State-machine transitions (tablet type changes, reparents, workflow states, Online DDL cut-over).
- The output of complex transformations (the planner's rewrites, VReplication's row filtering).

### Check each property from two sides

For every property that matters, look for two different code paths where it can be checked. The
classic case is persistent data: validate it **before it is written and after it is read**. In
Vitess, topology records are persistent data. Validate a VSchema when it is saved and again when
vtgate builds it. Check that the key ranges of a keyspace's serving shards cover the whole key space
when SrvKeyspace is written and again when it is used for routing. The second check catches the
bugs, the older versions, and the manual edits that the first one did not see.

### Check both the positive and the negative space

Check what you expect, and also check what you do not expect. Interesting bugs live where valid data
becomes invalid. For the same reason, tests must cover invalid input, error paths, and the transition
from valid to invalid, not only the success path.

When you `switch` over an enum, such as a `topodatapb.TabletType` or a workflow state, handle every
value explicitly, and make the `default` case return an error rather than doing nothing. A new enum
value then fails loudly instead of being ignored.

### Keep checks simple

- Split compound checks: two `if` statements report which condition failed, and one combined
  condition does not.
- State conditions positively. `if index < count` is easier to verify than `if index >= count`.
- For a condition with several booleans, prefer nested `if`/`else` over long `else if` chains, so
  that the reader can see that every case is handled.

### Check at compile time when you can

Compile-time checks verify the design before the program runs. Use them for interface
implementations and for relationships between constants:

```go
// This fails to compile if *Route stops implementing Primitive.
var _ Primitive = (*Route)(nil)

// This fails to compile if headerSize ever exceeds bufferSize.
const _ = uint(bufferSize - headerSize)
```

Generated code (for example, `sizegen` and the AST helpers) is also checked by CI, so regenerate
rather than edit it by hand.

### Use build tags for expensive checks

When a check is too expensive for the production hot path, guard it with a constant that a build
tag turns on, as `DebugTwoPc` does in `go/vt/vtgate/production.go`. The compiler then removes the
check from production builds, and tests and fuzzers can still run with it enabled.

### Checks do not replace understanding

Fuzzers and end-to-end tests can show that bugs exist, but not that they are absent. First build a
precise mental model of the code. Then encode that model as checks, explain it in comments for your
reviewer, and use tests and fuzzers to find the places where your model and the reviewer's model are
wrong.

### Put a limit on everything

Everything has a limit in practice, so make the limit explicit and choose what happens when it is
reached:

- Every network call and every blocking operation takes a `context.Context` with a deadline.
- Every retry loop has a maximum number of attempts or a deadline, and it backs off.
- Every queue, channel, buffer, and cache has a maximum size.
- Every per-request fan-out has a concurrency limit.
- Every result has a size limit.
- Recursion is allowed only over input whose depth is limited at the entry point. The planner and
  parser recurse over ASTs, so the depth of the AST must be limited before planning starts.
- Metric labels have bounded cardinality. Never use query text, table names, or other user-controlled
  values as a label without limiting them.
- A `for {}` loop documents how it exits. Usually it exits through `<-ctx.Done()`.

An explicit limit turns a slow failure, such as a memory leak or a stuck goroutine, into a fast one
that has an error message.

### Handle every error

Most catastrophic failures in distributed data systems come from mishandled non-fatal errors
([Yuan et al., OSDI 2014](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-yuan.pdf)).
Therefore:

- Do not discard an error with `_ =` unless a comment explains why that is safe.
- Keep the `vtrpcpb.Code` when you add context: use `vterrors.Wrapf`.
- Handle the errors from deferred `Close`, `Rollback`, and `Unlock` calls when they can lose data
  or leave a lock held.
- Test error paths. Use the existing fakes (`memorytopo`, `fakesqldb`, `faketmclient`, the
  `grpcvtctldserver/testutil` helpers) to inject failures, timeouts, and partial results.

### Be explicit about options and defaults

Pass important options explicitly at the call site instead of relying on a default, because a
default can change and the change is invisible at the call site. Pay particular attention to:

- **Timeouts.** Pass them explicitly.
- **Zero values.** In proto3, an unset field and a field set to zero look the same. If zero means
  something special (for example, "no limit"), give that meaning a name, and do not let it collide
  with "use the default."
- **Flags.** Changing a flag's default is a breaking change. Follow the staged deprecation process in
  `AGENTS.md`.

### Treat compatibility as a safety property

Vitess always runs in mixed versions during an upgrade. Protobuf messages, topology records, and
anything else written by one binary and read by another are a persistent format:

- Never change the meaning of an existing field. Add a new one.
- Code must tolerate the records that the previous major version writes and the records that the
  next major version writes.
- The "check before write and after read" rule applies across versions too.

## Concurrency

### Every goroutine has an owner

Every `go` statement has an owner that can stop the goroutine and wait for it to exit, and a
documented exit condition. A goroutine that nobody waits for is a leak waiting to happen, and it makes
shutdown and tests unreliable.

### Use the caller's context

Derive contexts from the caller's context, so that cancellation and deadlines propagate. Do not
create a `context.Background()` in a request path. Use one only at process startup, or for
background work that is intentionally detached from any request, and say so in a comment.

### Do not hold a lock across blocking work

Do not hold a mutex across an RPC, a MySQL query, a topology call, or a channel operation that can
block. A function that suspends while holding state invalidates the invariants that it checked at
the start. After you re-acquire a lock, check those invariants again.

### Close the gap between check and use

Most concurrency bugs are a gap in time between checking a condition and acting on it. In Vitess the
gap usually spans processes:

- Use topology versions (compare-and-swap) or topology locks, rather than reading a record and then
  writing it back.
- Check a tablet's type or a shard's state as close as possible to the action that depends on it,
  and handle the case where it changed.
- Calculate a variable close to where it is used, and keep its scope small.

### Run at your own pace

Do not do expensive work directly in reaction to each external event. Health-check updates,
topology watches, and VTOrc problem detection can arrive in bursts. Debounce or batch them, give one
loop ownership of the resulting state, and do not start a goroutine per event. This keeps control
flow under our control, bounds the work done per unit of time, and prevents thundering herds when the
topology changes.

## State and caches

### Keep one source of truth

Do not duplicate variables or take aliases to them, because copies get out of sync.

Vitess must cache (for example, srvtopo, the health check, and the schema tracker). For every cache,
document:

- its source of truth,
- how it is invalidated,
- the maximum staleness that callers can see, and what happens when they see it.

### Do not mutate shared data

A proto returned from a cache or from the topology server may be shared with other goroutines. Do
not mutate it: clone it first. Do not copy a struct that contains a lock or a proto message by
value; `go vet` reports many, but not all, of these copies.

### Watch for pooled-buffer aliasing

Returning a buffer to a pool while a slice still points into it is Go's version of a use-after-free.
It shows up as corrupted rows, often under load and far from the cause. In the MySQL protocol and
other code that uses `bucketpool` or `sync.Pool`, make the ownership of every buffer clear, and copy
data out before releasing the buffer.

## Control flow and function shape

- Prefer simple, explicit control flow. Use only a few abstractions, and only ones that fit the
  domain well. Every abstraction has a cost, and every abstraction can leak.
- **Keep functions short.** A function that fits on one screen can be checked by reading it. As a
  guideline, keep new functions under about 100 lines. Table-driven functions, such as the
  `information_schema` definitions, are an exception.
- When you split a long function, **push `if`s up and `for`s down**: keep the branching and the
  state changes in the parent function, and move non-branching logic into helpers that compute
  values without side effects. This matters most in state machines, such as reparents and Online
  DDL cut-over, because a long state machine hides missing branches.
- Good function shape is few parameters, a simple return type, and the logic in between. A simpler
  signature reduces the number of cases that every caller must handle.
- Group a resource's acquisition with its `defer` release, so that leaks are easy to spot.

## Performance

- **Think about performance during design.** The large wins come from design decisions, before
  there is anything to profile.
- **Sketch before you build.** Estimate the cost in terms of network, disk, memory, and CPU, both
  bandwidth and latency. For a query-path change, count the network round trips (vtgate to vttablet
  to MySQL) and the fan-out across shards first. These usually dominate.
- **Optimize the slowest resource first**, after accounting for how often each one is used.
- **Separate the control plane from the data plane.** The control plane (topology, vtctld, VTOrc)
  can afford expensive validation. The data plane (vtgate and vttablet query serving) must not block
  on the control plane: it reads cached state, such as srvtopo, and keeps per-row work small.
- **Batch.** Batching amortizes network, disk, and CPU costs. Vitess already batches multi-shard
  queries, VReplication inserts, and binlog processing. New code on these paths should keep it that
  way.
- **Budget allocations on hot paths.** Go has a garbage collector, so we do not allocate everything
  at startup as TigerBeetle does. On hot paths (the MySQL protocol, the evaluation engine, row
  streaming), reuse buffers, preallocate slices, and measure with benchmarks that call
  `b.ReportAllocs()`.

## Naming

- **Get the nouns and verbs right.** A good name shows that you understand the domain. Prefer nouns
  that can be used in documentation and conversation without rephrasing.
- **Do not overload a name.** Several Vitess terms already have more than one meaning, such as
  "plan" (vtgate and the tablet server), "stream", "workflow", "position", and "transaction" (a
  MySQL transaction or a distributed one). Do not add to the list. When you introduce a concept,
  choose a name that is not already in use.
- **Make units part of the type.** Use `time.Duration` instead of an integer of milliseconds. When a
  unit suffix is unavoidable, use it consistently within a struct, so that related fields group
  together.
- **Use distinct types for arguments that can be swapped.** `func(ctx, keyspace, shard string)` is
  easy to call with the arguments in the wrong order, and the compiler cannot help. Prefer a struct,
  or separate types, when two arguments of the same type can be mixed up.
- Do not abbreviate, except for well-known abbreviations and short-lived loop variables.
- Follow Go conventions for everything else, including initialisms (`GTID`, `ID`, `URL`).

## Off-by-one errors and arithmetic

- An index, a count, and a size are different things, even though they are all integers. An index
  plus one is a count, and a count times the unit is a size. Name them so that the difference is
  visible.
- Know the interval conventions: key ranges include their start and exclude their end, and GTID
  intervals include both ends.
- Use exact-width integer types at wire and storage boundaries (the MySQL protocol, binlog events,
  protobuf). Convert between signed and unsigned types, and between widths, deliberately, and check
  for overflow. Auto-increment values and replication positions are common places for this bug.
- Show your intent when you divide: say whether you need to round down, round up, or require an
  exact result.

## Comments and commit messages

- **Always say why.** Code shows what it does. Comments explain why it does it this way, and what
  the alternatives were. A reviewer who knows the reasoning can check it.
- **Say how a test works.** Start a non-trivial test with a comment that explains its goal and
  method.
- Write comments as sentences.
- **Commit messages are read.** Vitess squash-merges pull requests, so the squashed commit message is
  what `git blame` shows. Make sure that it explains why the change was made, not only what changed.

## Technical debt, dependencies, and tooling

- **No debt for correctness or data integrity.** We do not merge a known correctness bug, a known
  unbounded resource, or a known data-integrity risk with a plan to fix it later.
- **Track all other debt.** Every new `TODO` links to a GitHub issue. Breaking changes are staged
  across releases, as described in `AGENTS.md`.
- **Add dependencies carefully.** Each new module is a security, performance, and maintenance risk
  for every Vitess user. Explain why a new dependency is needed. Respect the package layering that
  `depguard` enforces in `.golangci.yml`: library packages such as `mysql` and `sqlparser` must not
  depend on server infrastructure.
- **Prefer Go for tooling.** Write new internal tools in Go rather than shell, for type safety and
  portability. Scripts under `examples/` stay in shell, because users read them as tutorials.

## What we do not adopt from TigerStyle

- **Crash on every assertion failure.** Replaced by the blast-radius rules above.
- **A minimum number of assertions per function.** Replaced by checks at trust boundaries.
- **No recursion.** Replaced by bounded recursion depth.
- **Static memory allocation.** Replaced by allocation budgets on hot paths and size limits on every
  request.
- **Zero technical debt and zero dependencies.** Replaced by the narrower rules above.
- **Formatting rules (`snake_case`, four-space indentation, a hard line-length limit).** `gofmt`
  and Go conventions decide these. Run `scripts/fmt` before each commit.
