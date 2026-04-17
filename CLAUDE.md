## :handshake: Our Partnership

**We're building this together.** You're not just executing tasks - you're helping design and implement the best possible solution. This means:

- Challenge my suggestions when something feels wrong
- Ask me to explain my reasoning
- Propose alternative approaches
- Take time to think through problems

**Quality is non-negotiable.** We'd rather spend an hour designing than 3 hours fixing a rushed implementation.

## :thought_balloon: Before We Code

Always discuss first:
- What problem are we solving?
- What's the ideal solution?
- What tests would prove it works?
- Are we making the codebase better?

## Strict Task Adherence

**Only do exactly what I ask for - nothing more, nothing less.**

- Do NOT proactively update documentation unless explicitly requested
- Do NOT add explanatory comments unless asked
- Do NOT make "improvements" or "clean up" code beyond the specific task
- Do NOT add features, optimizations, or enhancements I didn't mention
- If there is something you think should be done, suggest it, but don't do it until asked to

**Red flags that indicate you're going beyond the task:**
- "Let me also..."
- "While I'm at it..."
- "I should also update..."
- "Let me improve..."
- "I'll also clean up..."

**If the task is complete, STOP. Don't look for more work to do.**

## :test_tube: Test-Driven Development

TDD isn't optional - it's how we ensure quality:

### The TDD Cycle
1. **Red** - Write a failing test that defines success
2. **Green** - Write minimal code to pass
3. **Refactor** - Make it clean and elegant

### Example TDD Session
```go
// Step 1: Write the test first
func TestConnectionBilateralCleanup(t *testing.T) {
    // Define what success looks like
    client, server := testutils.CreateConnectedTCPPair()
    
    // Test the behavior we want
    client.Close()
    
    // Both sides should be closed
    assert.Eventually(t, func() bool {
        return isConnectionClosed(server)
    })
}

// Step 2: See it fail (confirms we're testing the right thing)
// Step 3: Implement the feature
// Step 4: See it pass
// Step 5: Refactor for clarity
```

To make sure tests are easy to read, we use `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require` for assertions:
- Use `require` (not `assert`) when the test cannot continue after a failure (e.g., `require.NoError` after setup that must succeed)
- Use `assert.Eventually` instead of manual `time.Sleep()` and timeouts
- Use `t.Context()` instead of `context.Background()` — it integrates with test cancellation
- Use `t.Cleanup()` for test teardown
- Use `assert.ErrorContains` / `require.ErrorContains` to check error messages
- Use the `_test.go` suffix for mocks and test helpers that are only used by the current package's tests; if helpers or mocks need to be imported by other packages' tests or fuzz harnesses, put them in a normal reusable package such as `testlib` or `testutil`
- CI timeouts must be generous (30s+) — GitHub Actions runners can be resource-starved with multi-second pauses; sub-second timeouts cause flakiness with no recourse but retry

## :rotating_light: Error Handling Excellence

Error handling is not an afterthought - it's core to reliable software.

### Go Error Patterns
```go
// YES - Clear error context with vterrors
func ProcessUser(id string) (*User, error) {
    if id == "" {
        return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "user ID cannot be empty")
    }

    user, err := db.GetUser(id)
    if err != nil {
        return nil, vterrors.Wrapf(err, "failed to get user %s", id)
    }

    return user, nil
}

// NO - Swallowing errors
func ProcessUser(id string) *User {
    user, _ := db.GetUser(id)  // What if this fails?
    return user
}
```

### Error Handling Principles
1. **Use `vterrors`** - Prefer `vterrors` over `fmt.Errorf` or `errors` package, with an appropriate `vtrpcpb.Code` (e.g., `vtrpcpb.Code_FAILED_PRECONDITION` for unexpected input values, `vtrpcpb.Code_INTERNAL` for internal operation failures)
2. **Wrap errors with context** - Use `vterrors.Wrapf(err, "context")`
3. **Validate early** - Check inputs before doing work
4. **Fail fast** - Don't continue with invalid state
5. **Log appropriately** - Errors at boundaries, debug info internally
6. **Return structured errors** - Use error types for different handling
7. **Never silently swallow errors** - When recovering from an error (e.g., restarting replication), always log the original error before the recovery attempt so operators can trace what happened
8. **Log with context** - Include workflow name, recovery type, tablet alias, and other identifiers in log messages — a keyspace/tablet can have many concurrent workflows

### Failure-Path Safety
Multi-step operations must not leave the system in a half-applied state:
- If step 2 of 3 succeeds but step 3 fails, ensure step 2 is rolled back or the cleanup path handles it
- Deferred cleanup must use bounded contexts — never `context.Background()` for operations that could hang indefinitely
- When holding a mutex, always bound the operation with a reasonable timeout
- Test failure paths, not just the happy path — a test that only proves "step X ran" without covering "what if step X+1 fails" is incomplete

### Testing Error Paths
```go
func TestProcessUser_InvalidID(t *testing.T) {
    _, err := ProcessUser("")
    assert.ErrorContains(t, err, "cannot be empty")
}

func TestProcessUser_DatabaseError(t *testing.T) {
    mockDB.EXPECT().GetUser("123").Return(nil, errors.New("db connection failed"))
    
    _, err := ProcessUser("123")
    assert.ErrorContains(t, err, "failed to get user")
}
```

## :triangular_ruler: Design Principles

### 1. Simple is Better Than Clever
```go
// YES - Clear and obvious
if user.NeedsMigration() {
    return migrate(user)
}

// NO - Clever but unclear
return user.NeedsMigration() && migrate(user) || user
```

### 2. Explicit is Better Than Implicit
- Clear function names
- Obvious parameter types
- No hidden side effects

### 3. Conditions Must Be as Specific as the Intent
- Guard clauses should match *exactly* the intended case, not just currently-known cases
- A check like `len(tables) == 0` when you mean "is virtual dual" will silently fire for future zero-table cases
- A nil return from a helper (e.g., `operatorKeyspace()` returning nil for composite operators) should not be treated as "safe to skip" — handle it explicitly
- When catching MySQL error codes, match the specific codes that apply to your call site, not a broad class

### 4. Zero-Value / Default Behavior Safety
- New struct fields must not change behavior for existing callers who omit them
- Prefer negative-polarity booleans (`PreventCrossKeyspaceReads` not `AllowCrossKeyspaceReads`) so the zero-value preserves existing behavior
- When a flag value of `0` or empty previously meant "disabled," don't change it to mean "unlimited" — preserve the existing semantic or make the change explicit
- Validate mutually exclusive flags in `PreRunE` and add unit tests for invalid combinations

### 5. Performance with Clarity
- Optimize hot paths, but keep code readable
- Preallocate slices and maps when the size is known: `make([]T, 0, len(source))`
- Avoid duplicate work — cache results of expensive calls like `reflect.Value.MapKeys()` instead of calling twice
- Document why, not what

### 6. Fail Fast and Clearly
- Validate inputs early
- Return clear error messages
- Help future debugging

### 7. Interfaces Define What You Need, Not What You Provide
- When you need something from another component, define the interface in your package
- Don't look at what someone else provides - define exactly what you require
- This keeps interfaces small, focused, and prevents unnecessary coupling
- Types and their methods live together. At the top of files, use a single ```type ()``` with all type declarations inside.

### 8. Go-Specific Best Practices
- **Receiver naming** - Use consistent, short receiver names (e.g., `u *User`, not `user *User`)
- **Package naming** - Short, descriptive, lowercase without underscores
- **Interface naming** - Single-method interfaces end in `-er` (Reader, Writer, Handler)
- **Context first** - Always pass `context.Context` as the first parameter
- **Context cancellation** - Prefer `context.WithoutCancel(ctx)` over `context.Background()` when you need a non-cancellable context but still want to preserve context values (tracing, caller ID)
- **Channels for coordination** - Use channels to coordinate goroutines, not shared memory
- **No naked returns in non-trivial functions** - For functions with named return values, avoid bare `return` and explicitly return all result values (very small helpers are the only exception). This does not prohibit plain `return` in `func f() { ... }` when used for early-exit/guard clauses.
- **Reduce nesting** - Prefer early returns and guard clauses over deeply nested `if` conditions
- **Copyright header** - New Go files must include the project copyright header with the current year
- **Always run `gofumpt -w`** on changed Go files before committing - this is mandatory
- **Always run `goimports -local "vitess.io/vitess" -w`** on changed Go files before committing
- **Use format verbs precisely** - Use `%s` for strings and `%d` for integers, not `%v` for everything
- **Structured logging** - New log messages should use structured logging with `slog`-style fields (e.g., `log.Warn("message", slog.Any("error", err))`) rather than printf-style logging with format strings
- **Reuse existing helpers** - Before writing new parsing/validation code, check for existing utilities (e.g., `sqlerror` package for MySQL error codes, `mysqlctl.ParseVersionString()`, `strings.Split()`, `topoproto.TabletAliasString()` for formatting tablet aliases)

## :building_construction: Vitess-Specific Conventions

### Generated Code
- **Never** directly edit files with a `Code generated ... DO NOT EDIT` header - these are generated and will be overwritten
- Run `make codegen` to regenerate after modifying source definitions

#### Protobufs
- **Never** directly edit files under `go/vt/proto/` - they are generated from `proto/*.proto` protobuf definitions
- After modifying `proto/*.proto` files, run `make proto` to regenerate
- Avoid storing timestamps or time durations as integers; use `vttime.Time` for timestamps and `vttime.Duration` (or `google.protobuf.Duration`, as appropriate) for durations instead
- Avoid storing tablet aliases as a string: use `topodata.TabletAlias`

#### SQL Parser
- **Never** directly edit these generated files in `go/vt/sqlparser/`: `sql.go`, `ast_clone.go`, `ast_copy_on_rewrite.go`, `ast_equals.go`, `ast_format_fast.go`, `ast_path.go`, `ast_rewrite.go`, `ast_visit.go`, `cached_size.go`
- After modifying source files (e.g., `sql.y`, AST definitions), run `make codegen` to regenerate
- Field order in AST structs matters — generated walkers visit fields in declaration order, so reordering fields changes semantic-analysis walk order and can break scope setup

### Command-Line Flags
- New flags must **not** use underscores (use hyphens instead)
- When flags are added or modified, update the corresponding `go/flags/endtoend/` files - column/whitespace alignment matters

### TabletAlias Formatting
- Format `*topodatapb.TabletAlias` using `topoproto.TabletAliasString(alias)` in logs and error messages so that tablet aliases are human-readable

### MySQL Flavor Isolation
- MySQL-version-specific behavior belongs in the corresponding flavor implementation (e.g., MariaDB handling in the MariaDB flavor file), not in generic code
- Be aware that MariaDB and older MySQL versions may not support all system variables (e.g., `super_read_only`) — other Vitess call sites already warn-and-continue for `ERUnknownSystemVariable`

### User-Visible Changes
- Any user-visible behavioral change — even a correctness fix — needs explicit callout in release/deployment notes
- Removing or renaming a public API function (e.g., in `sqlparser`) is a breaking change for downstream users — call it out explicitly or keep a thin compatibility wrapper
- Changelog summaries are for key changes all users should know about — internal implementation details don't belong there
- Keep PRs clean of unrelated diffs (e.g., stray `package-lock.json` changes, `go.sum` without `go mod tidy`)

### EmergencyReparentShard (ERS)
- ERS must prioritize **certainty** that we picked the most-advanced candidate
- Changes should prioritize reducing points of failure - avoid new RPCs or work that may delay or make ERS more brittle

## :mag: Debugging & Troubleshooting

When things don't work as expected, we debug systematically:

### Debugging Strategy
1. **Reproduce reliably** - Create a minimal failing case
2. **Isolate the problem** - Binary search through the system
3. **Understand the data flow** - Trace inputs and outputs
4. **Question assumptions** - What did we assume was working?
5. **Fix the root cause** - Not just the symptoms

### Debugging Tools & Techniques
```go
// Use structured logging (slog-style) for new code
log.Info("Starting payment processing",
    slog.String("user_id", userID),
    slog.String("action", "process_payment"),
    slog.Float64("amount", amount),
)

// Add strategic debug points
func processPayment(amount float64) error {
    if amount <= 0 {
        return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid amount: %f", amount)
    }

    // More processing...
    log.Info("Payment validation passed")
    return nil
}
```

### When Stuck
- Write a test that reproduces the issue
- Add logging to understand data flow  
- Use the debugger to step through code
- Rubber duck explain the problem
- Take a break and come back fresh

## :recycle: Refactoring Legacy Code

When improving existing code, we move carefully and systematically:

### Refactoring Strategy
1. **Understand first** - Read and comprehend the existing code
2. **Add tests** - Create safety nets before changing anything  
3. **Small steps** - Make tiny, verifiable improvements
4. **Preserve behavior** - Keep the same external interface
5. **Measure improvement** - Verify it's actually better

### Safe Refactoring Process
```go
// Step 1: Add characterization tests
func TestLegacyProcessor_ExistingBehavior(t *testing.T) {
    processor := &LegacyProcessor{}
    
    // Document current behavior, even if it seems wrong
    result := processor.Process("input")
    assert.Equal(t, "weird_legacy_output", result)
}

// Step 2: Refactor with tests passing
func (p *LegacyProcessor) Process(input string) string {
    // Improved implementation that maintains the same behavior
    return processWithNewLogic(input)
}

// Step 3: Now we can safely change the behavior
func TestProcessor_ImprovedBehavior(t *testing.T) {
    processor := &Processor{}
    
    result := processor.Process("input")
    assert.Equal(t, "expected_output", result)
}
```

## :arrows_counterclockwise: Development Workflow

### Starting a Feature
1. **Discuss** - "I'm thinking about implementing X. Here's my approach..."
2. **Design** - Sketch out the API and key components
3. **Test** - Write tests that define the behavior
4. **Implement** - Make the tests pass
5. **Review** - "Does this make sense? Any concerns?"

### Making Changes
1. **Small PRs** - Easier to review and less risky
2. **Incremental** - Build features piece by piece
3. **Always tested** - No exceptions
4. **Clear commits** - Each commit should have a clear purpose

### Git and PR Workflow

**CRITICAL: Git commands are ONLY for reading state - NEVER for modifying it.**
- **NEVER** use git commands that modify the filesystem unless explicitly told to commit
- You may read git state: `git status`, `git log`, `git diff`, `git branch --show-current`
- You may NOT: `git commit`, `git add`, `git reset`, `git checkout`, `git restore`, `git rebase`, `git push`, etc.
- **ONLY commit when explicitly asked to commit**
- Always sign git commits with the `git commit --signoff` flag
- When asked to commit, do it once and stop
- Only I can modify git state unless you've been given explicit permission to commit

**Once a PR is created, NEVER amend commits or rewrite history.**
- Always create new commits after PR is created
- No `git commit --amend` after pushing to a PR branch
- No `git rebase` that rewrites commits in the PR
- No force pushes to PR branches
- This keeps the PR history clean and reviewable

**When asked to write a PR description:**
1. **Use `gh` CLI** - Always use `gh pr edit <number>` to update PRs
2. **Update both body and title** - Use `--body` and `--title` flags
3. **Be informal, humble, and short** - Keep it conversational and to the point
4. **Credit appropriately** - If Claude Code wrote most of it, mention that
5. **Example format**:
   ```
   ## What's this?
   [Brief explanation of the feature/fix]

   ## How it works
   [Key implementation details]

   ## Usage
   [Code examples if relevant]

   ---
   _Most of this was written by Claude Code - I just provided direction._
   ```

## :memo: Code Review Mindset

When reviewing code (yours or mine), ask:
- Is this the simplest solution?
- Will this make sense in 6 months?
- Are edge cases handled?
- Is it well tested?
- Does it improve the codebase?

## :dart: Common Patterns

### Feature Implementation
```
You: "Let's add feature X"
Me: "Sounds good! What's the API going to look like? What are the main use cases?"
[Discussion of design]
Me: "Let me write some tests to clarify the behavior we want"
[TDD implementation]
Me: "Here's what I've got. What do you think?"
```

### Bug Fixing
```
You: "We have a bug where X happens"
Me: "Let's write a test that reproduces it first"
[Test that fails]
Me: "Great, now we know exactly what we're fixing"
[Fix implementation]
```

### Performance Work
```
You: "This seems slow"
Me: "Let's benchmark it first to get a baseline"
[Benchmark results]
Me: "Now let's optimize without breaking functionality"
[Optimization with tests passing]
```

## :rocket: Shipping Quality

Before considering any work "done":
- [ ] Tests pass and cover the feature
- [ ] Code is clean and readable
    - [ ] Golang code passes the `gofumpt` formatter
    - [ ] Golang code passes the `goimports -local "vitess.io/vitess" -w ...` formatter
- [ ] Edge cases are handled
- [ ] Performance is acceptable
- [ ] Documentation is updated if needed
- [ ] We're both happy with it

Remember: We're crafting software, not just making it work. Every line of code is an opportunity to make the system better.
