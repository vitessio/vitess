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

To make sure tests are easy to read, we use testify assertions. Make sure to use assert.Eventually instead of using manual thread.sleep and timeouts.

## :rotating_light: Error Handling Excellence

Error handling is not an afterthought - it's core to reliable software.

### Go Error Patterns
```go
// YES - Clear error context
func ProcessUser(id string) (*User, error) {
    if id == "" {
        return nil, fmt.Errorf("user ID cannot be empty")
    }
    
    user, err := db.GetUser(id)
    if err != nil {
        return nil, fmt.Errorf("failed to get user %s: %w", id, err)
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
1. **Wrap errors with context** - Use `fmt.Errorf("context: %w", err)`
2. **Validate early** - Check inputs before doing work
3. **Fail fast** - Don't continue with invalid state
4. **Log appropriately** - Errors at boundaries, debug info internally
5. **Return structured errors** - Use error types for different handling

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

### 3. Performance with Clarity
- Optimize hot paths
- But keep code readable
- Document why, not what

### 4. Fail Fast and Clearly
- Validate inputs early
- Return clear error messages
- Help future debugging

### 5. Interfaces Define What You Need, Not What You Provide
- When you need something from another component, define the interface in your package
- Don't look at what someone else provides - define exactly what you require
- This keeps interfaces small, focused, and prevents unnecessary coupling
- Types and their methods live together. At the top of files, use a single ```type ()``` with all type declarations inside.

### 6. Go-Specific Best Practices
- **Receiver naming** - Use consistent, short receiver names (e.g., `u *User`, not `user *User`)
- **Package naming** - Short, descriptive, lowercase without underscores
- **Interface naming** - Single-method interfaces end in `-er` (Reader, Writer, Handler)
- **Context first** - Always pass `context.Context` as the first parameter
- **Channels for coordination** - Use channels to coordinate goroutines, not shared memory

## :mag: Dubugging & Troubleshooting

When things don't work as expected, we debug systematically:

### Debugging Strategy
1. **Reproduce reliably** - Create a minimal failing case
2. **Isolate the problem** - Binary search through the system
3. **Understand the data flow** - Trace inputs and outputs
4. **Question assumptions** - What did we assume was working?
5. **Fix the root cause** - Not just the symptoms

### Debugging Tools & Techniques
```go
// Use structured logging for debugging
log.WithFields(log.Fields{
    "user_id": userID,
    "action": "process_payment", 
    "amount": amount,
}).Debug("Starting payment processing")

// Add strategic debug points
func processPayment(amount float64) error {
    log.Debugf("processPayment called with amount: %f", amount)
    
    if amount <= 0 {
        return fmt.Errorf("invalid amount: %f", amount)
    }
    
    // More processing...
    log.Debug("Payment validation passed")
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
- [ ] Edge cases are handled
- [ ] Performance is acceptable
- [ ] Documentation is updated if needed
- [ ] We're both happy with it

Remember: We're crafting software, not just making it work. Every line of code is an opportunity to make the system better.
