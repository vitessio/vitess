# SQL Parser Memory Pooling Plan

## Executive Summary

This document proposes introducing memory pooling for AST node allocations in the
Vitess SQL parser (`go/vt/sqlparser`). Today, every `Parse()` call allocates dozens
to hundreds of small AST structs on the heap. These allocations stress the Go garbage
collector, especially under high-QPS workloads where the same query shapes are parsed
repeatedly. We propose an **arena-style block allocator** that is threaded through the
parser and reclaimed in bulk after the AST is no longer needed.

---

## 1. Current State Analysis

### 1.1 What Is Already Pooled

Only the **parser state machine** (`yyParserImpl`) is pooled via `sync.Pool`
(`parser.go:37-64`). The `yyParserImpl` struct contains the LALR parse stack and is
reset to a zero value before being returned to the pool. No AST nodes are pooled.

### 1.2 How AST Nodes Are Allocated Today

The yacc grammar (`sql.y`, ~7,900 lines) constructs AST nodes directly in rule
actions using Go composite literals:

```go
// Examples from sql.y:
$$ = &BinaryExpr{Left: $1, Operator: PlusOp, Right: $3}
$$ = &Select{SelectExprs: $2, From: $3, Where: NewWhere(WhereClause, $4), ...}
$$ = &Literal{Type: IntVal, Val: $1}
```

There are **~556 `&Struct{...}` allocations** across the grammar rules. The most
frequently allocated types during typical query parsing are:

| Type | Typical allocs/parse | Size (bytes) |
|------|---------------------|--------------|
| `Literal` | 5-50+ | 40 |
| `ColName` | 5-30+ | 64 |
| `BinaryExpr` | 2-20+ | 40 |
| `ComparisonExpr` | 2-10+ | 56 |
| `AliasedExpr` | 1-20+ | 56 |
| `FuncExpr` | 1-10+ | 80 |
| `AliasedTableExpr` | 1-5+ | 88 |
| `Where` | 1-2 | 24 |
| `Select` | 1-2 | 208 |

A simple `SELECT a, b FROM t WHERE x = 1` produces roughly **20-30 heap
allocations** just for AST nodes. A complex query with subqueries, joins, and
expressions can produce **200+**.

### 1.3 AST Lifetime

The AST lifetime is **short and well-defined**:

1. **Parse**: `Parse2(sql)` → allocates AST nodes
2. **Normalize**: `Normalize(stmt, ...)` → mutates AST in-place (replaces `Literal`
   nodes with `Argument` bind variable placeholders)
3. **Rewrite**: `RewriteAST(stmt, ...)` → mutates AST in-place (adds hints, rewrites
   functions)
4. **Plan**: `BuildPlan(stmt, ...)` → reads AST to produce an execution `Plan`
5. **Discard**: AST becomes garbage; only the `Plan` is cached

The AST is **never stored long-term**. Plans are cached, not ASTs. This makes the
AST an ideal candidate for arena/pool allocation since it has a clear "free all"
point.

### 1.4 Complications

1. **AST cloning**: `ast_clone.go` has 300+ `CloneRefOf*` functions that deep-copy
   AST subtrees. Cloned nodes would need to be allocated outside the arena (or on a
   new arena) since they may outlive the original.

2. **AST rewriting**: The `Rewrite()` / `Cursor.Replace()` mechanism replaces AST
   nodes in-place. Replacement nodes created during rewriting (e.g., `NewArgument()`
   in the normalizer) would need to come from the same arena or from the heap.

3. **`ColName.Metadata`**: This `any`-typed field is populated after parsing by
   semantic analysis. It holds planner-internal data that may outlive the parse arena.
   Since `Metadata` is an interface pointing to heap-allocated planner structs, this
   is fine — the arena would hold the `ColName` shell, while `Metadata` points
   elsewhere.

4. **Generated code**: Several files are auto-generated (`ast_clone.go`,
   `ast_rewrite.go`, `ast_visit.go`, `ast_equals.go`, `ast_format.go`,
   `cached_size.go`). The code generators would need to be updated to support
   arena-aware allocation for clone operations.

5. **Parser pool constraint**: Because `yyParserImpl` is pooled, grammar rules
   already cannot take addresses of stack variables (`&$4` is forbidden). This
   constraint is compatible with arena allocation.

---

## 2. Proposed Design: Arena-Style Block Allocator

### 2.1 Why an Arena (Not `sync.Pool` per Type)

| Approach | Pros | Cons |
|----------|------|------|
| **sync.Pool per type** | Simple, stdlib | 80+ pools needed; Put/Get overhead per node; must reset each node; doesn't help with GC scanning since nodes still reference each other |
| **Arena (block allocator)** | Single bulk free; near-zero per-alloc overhead; GC sees one large block not hundreds of tiny objects; great for short-lived AST | Must thread arena through parser; slightly more complex API |
| **Go 1.23 `arena` package** | Native runtime support | Still experimental; API not stable; not available in all build configurations |

**Recommendation**: A custom arena allocator, implemented as a simple block list with
typed allocation methods. This gives us full control, is compatible with all Go
versions Vitess supports, and can be swapped for the native `arena` package later if
it stabilizes.

### 2.2 Arena Implementation

```go
// File: go/vt/sqlparser/arena.go

package sqlparser

import "unsafe"

const defaultBlockSize = 64 * 1024 // 64KB blocks

// Arena is a bump-pointer allocator for AST nodes. It allocates objects from
// contiguous memory blocks and frees them all at once via Reset().
type Arena struct {
    blocks [][]byte
    cur    []byte
    off    int
}

// NewArena creates a new arena with one pre-allocated block.
func NewArena() *Arena {
    block := make([]byte, defaultBlockSize)
    return &Arena{
        blocks: [][]byte{block},
        cur:    block,
    }
}

// alloc returns a pointer to n bytes of zeroed memory, properly aligned.
func (a *Arena) alloc(size, align uintptr) unsafe.Pointer {
    // Align offset
    off := (uintptr(a.off) + align - 1) &^ (align - 1)
    if off+size > uintptr(len(a.cur)) {
        a.grow(size)
        off = 0
    }
    ptr := unsafe.Pointer(&a.cur[off])
    a.off = int(off + size)
    return ptr
}

func (a *Arena) grow(minSize uintptr) {
    sz := uintptr(defaultBlockSize)
    if minSize > sz {
        sz = minSize
    }
    block := make([]byte, sz)
    a.blocks = append(a.blocks, block)
    a.cur = block
    a.off = 0
}

// Reset releases all allocated memory back to the pool. The arena can be
// reused after Reset, but all pointers previously returned are invalidated.
func (a *Arena) Reset() {
    // Keep the first block, release the rest
    a.blocks = a.blocks[:1]
    a.cur = a.blocks[0]
    a.off = 0
}
```

**Typed allocation helpers** (generated alongside other generated code):

```go
// Generated for each AST node type, e.g.:
func (a *Arena) NewBinaryExpr() *BinaryExpr {
    ptr := a.alloc(unsafe.Sizeof(BinaryExpr{}), unsafe.Alignof(BinaryExpr{}))
    return (*BinaryExpr)(ptr)
}

func (a *Arena) NewLiteral() *Literal {
    ptr := a.alloc(unsafe.Sizeof(Literal{}), unsafe.Alignof(Literal{}))
    return (*Literal)(ptr)
}

func (a *Arena) NewColName() *ColName {
    ptr := a.alloc(unsafe.Sizeof(ColName{}), unsafe.Alignof(ColName{}))
    return (*ColName)(ptr)
}
// ... one per AST struct type
```

### 2.3 Threading the Arena Through the Parser

The arena must be accessible from yacc grammar rule actions. There are two candidates
for where to store it:

#### Option A: On `yyParserImpl` (Recommended)

Grammar rule actions execute inside `(yyrcvr *yyParserImpl).Parse(yylex)`. The
receiver `yyrcvr` is directly in scope — no type assertion needed. Vitess owns its
own goyacc fork (`go/vt/sqlparser/goyacc/goyacc.go`), so we can modify the
`$$ParserImpl` template (line 3374) to add an `Arena` field:

```go
// In goyacc.go template (line ~3374), the generated struct becomes:
type yyParserImpl struct {
    lval  yySymType
    stack [yyInitialStackSize]yySymType
    char  int
    Arena *Arena  // Arena for AST node allocation during this parse
}
```

Since `yyParserImpl` is **already pooled** via `sync.Pool` (`parser.go:37-64`), the
arena lifecycle piggybacks on the existing pool/reset mechanism. When the parser is
returned to the pool, the arena is detached and returned to its own pool (or reset).

In grammar rules, allocation changes from:

```yacc
// Before:
$$ = &BinaryExpr{Left: $1, Operator: PlusOp, Right: $3}

// After (yyrcvr is directly in scope — no type assertion):
{
    node := yyrcvr.Arena.NewBinaryExpr()
    node.Left = $1
    node.Operator = PlusOp
    node.Right = $3
    $$ = node
}
```

**Why this is better than the Tokenizer:**
- **Conceptual correctness**: The parser constructs AST nodes, not the lexer. The
  arena belongs with the component that allocates.
- **No type assertion overhead**: `yyrcvr` is a typed receiver, while accessing the
  tokenizer requires `yylex.(*Tokenizer)` on every allocation.
- **Existing pool integration**: The parser is already pooled; the arena reset
  naturally fits into `yyParsePooled()`'s defer block.
- **Cleaner separation**: The `Tokenizer` remains focused on lexing. It doesn't
  accumulate unrelated responsibilities.

#### Option B: On the Tokenizer (Not Recommended)

Storing the arena on the `Tokenizer` struct and accessing via
`yylex.(*Tokenizer).Arena` works but is conceptually wrong — the tokenizer's job is
lexing, not managing AST memory. It also adds a type assertion to every allocation
site. This was the original proposal but is superseded by Option A.

#### Arena Ownership and `yyParsePooled`

The arena is **not owned by the parser**. It is passed in by the caller and must
outlive the parse call (since the AST references arena memory). The updated pooling
code:

```go
func yyParsePooled(yylex yyLexer, arena *Arena) int {
    parser := parserPool.Get().(*yyParserImpl)
    parser.Arena = arena
    defer func() {
        parser.Arena = nil  // Detach arena before returning parser to pool
        *parser = zeroParser
        parserPool.Put(parser)
    }()
    return parser.Parse(yylex)
}
```

### 2.4 Entry Point Changes

```go
// In parser.go:

// arenaPool pools Arena objects for reuse across parse calls.
var arenaPool = sync.Pool{
    New: func() any {
        return NewArena()
    },
}

func Parse2(sql string) (Statement, BindVars, error) {
    arena := arenaPool.Get().(*Arena)
    tokenizer := NewStringTokenizer(sql)
    if yyParsePooled(tokenizer, arena) != 0 {
        // ... existing error handling ...
    }
    // NOTE: We do NOT return the arena to the pool here.
    // The caller must call Release() when done with the AST.
    return tokenizer.ParseTree, tokenizer.BindVars, nil
}
```

### 2.5 Lifetime Management

This is the critical design question. There are two viable approaches:

#### Option A: Explicit Release (Recommended)

Add a `ParseResult` return type that bundles the AST with its arena:

```go
type ParseResult struct {
    Statement Statement
    BindVars  BindVars
    arena     *Arena
}

// Release returns the arena memory to the pool. The Statement must not be
// accessed after Release is called.
func (r *ParseResult) Release() {
    if r.arena != nil {
        r.arena.Reset()
        arenaPool.Put(r.arena)
        r.arena = nil
    }
}

// New entry point:
func Parse2WithArena(sql string) (*ParseResult, error) { ... }
```

Callers acquire the result with `Parse2WithArena()` and defer `result.Release()`.
The existing `Parse2()` / `Parse()` functions continue to work without arenas (heap
allocation) for backward compatibility during the migration.

**Lifetime in the typical vtgate path:**

```go
func (e *Executor) getPlan(sql string, ...) {
    result, err := sqlparser.Parse2WithArena(sql)
    if err != nil { return err }
    defer result.Release()  // Frees all AST memory at the end

    // Normalize, rewrite, build plan — all use result.Statement
    Normalize(result.Statement, reserved, bindVars)
    stmt, _, err = RewriteAST(result.Statement, ...)
    plan := BuildPlan(stmt, ...)

    // Plan is cached; AST is released when function returns
    cache.Set(key, plan)
}
```

#### Option B: Reference Counting

Attach a reference count to the arena. Increment on clone, decrement on release.
Free when count reaches zero. This is more complex and error-prone. **Not
recommended** unless profiling shows cloned ASTs are a significant use case.

### 2.6 Handling AST Cloning

When an AST subtree is cloned (via `CloneRefOf*` functions), the cloned nodes must
**not** live in the original arena since they may outlive it. Two options:

1. **Clone to heap** (simple, recommended initially): Clone functions continue to use
   `&Struct{}` heap allocation. This is the current behavior and requires no changes
   to `ast_clone.go`.

2. **Clone to a new arena**: Pass a target arena to clone functions. This is an
   optimization for later if profiling shows clone allocations are significant.

### 2.7 Handling Post-Parse Mutations

The normalizer and rewriter create new AST nodes (e.g., `NewArgument()`) as
replacements. These should also be allocated from the arena. Since the arena is
accessible via the `Tokenizer`, but post-parse code doesn't have the tokenizer, we
need to either:

1. **Thread the arena through post-parse APIs**:
   ```go
   func Normalize(stmt Statement, reserved *ReservedVars,
                   bindVars map[string]*querypb.BindVariable, arena *Arena) error
   ```

2. **Store the arena in a context or on the ParseResult**: The normalizer and
   rewriter would accept the `ParseResult` or `Arena` directly.

3. **Allocate post-parse nodes on the heap**: Simplest. The normalizer creates very
   few new nodes (one `Argument` per literal). These heap-allocated nodes mixed into
   an arena-allocated tree is safe as long as the arena outlives them — which it does,
   since both are released at the same function scope.

**Recommendation**: Option 3 for initial implementation. The overhead of a few heap
allocations during normalization is negligible compared to the dozens saved during
parsing.

---

## 3. Implementation Plan

### Phase 1: Arena Infrastructure (Low Risk)

1. **Create `arena.go`** with the `Arena` struct, `alloc()`, `Reset()` methods.
2. **Create `arena_alloc.go`** (generated) with typed `New<Type>()` methods for every
   AST struct type. Update the code generator that produces `ast_clone.go`,
   `ast_visit.go`, etc. to also produce `arena_alloc.go`.
3. **Add `Arena` field to `yyParserImpl`** by modifying the goyacc template in
   `goyacc/goyacc.go` (line ~3374). Update `yyParsePooled()` to accept and attach
   the arena.
4. **Add `ParseResult` struct** with `Release()` method.
5. **Add `Parse2WithArena()` entry point** that creates an arena, parses, and returns
   a `ParseResult`.
6. **Unit tests** for the arena itself (allocation, alignment, reset).
7. **Benchmark** `Parse2WithArena` vs `Parse2` using existing `BenchmarkParseStress`
   and `BenchmarkParseTraces`.

### Phase 2: Grammar Rule Migration (Mechanical, Medium Risk)

This is the largest phase — changing ~556 allocation sites in `sql.y`.

1. **Grammar rules access the arena via `yyrcvr.Arena`** — no helper function
   needed for the common case. For `New*()` constructor functions called from
   `ast_funcs.go` (outside grammar actions), pass the arena explicitly or use
   heap fallback.

2. **Migrate grammar rules** in batches by AST node type, starting with the most
   frequently allocated:
   - Batch 1: `Literal`, `ColName`, `BinaryExpr`, `ComparisonExpr`
   - Batch 2: `AliasedExpr`, `FuncExpr`, `Where`, `Limit`, `Order`
   - Batch 3: `Select`, `Insert`, `Update`, `Delete`
   - Batch 4: All remaining statement types
   - Batch 5: All remaining expression types
   - Batch 6: DDL types, utility types

3. **Each batch**: Modify `sql.y`, regenerate `sql.go`, run full test suite.

4. **Fallback for nil arena**: For backward compatibility (callers that don't use
   arenas), the generated allocation helpers handle nil gracefully:
   ```go
   // Generated for each type:
   func (a *Arena) NewBinaryExpr() *BinaryExpr {
       if a == nil {
           return &BinaryExpr{}  // heap fallback
       }
       ptr := a.alloc(unsafe.Sizeof(BinaryExpr{}), unsafe.Alignof(BinaryExpr{}))
       return (*BinaryExpr)(ptr)
   }
   ```
   In grammar rules: `yyrcvr.Arena.NewBinaryExpr()` — works whether arena is
   nil or not.

### Phase 3: Caller Migration (Medium Risk)

1. **Migrate `vtgate/executor.go` `getPlan()`** to use `Parse2WithArena()` +
   `defer Release()`.
2. **Migrate `vtgate/planbuilder`** entry points.
3. **Migrate `vttablet` query processing** paths.
4. **Audit all `sqlparser.Parse` / `Parse2` call sites** across the codebase
   (~100+ sites) and migrate high-throughput paths.
5. Low-throughput paths (DDL processing, schema management, tests) can remain on
   heap allocation via the existing `Parse2()` API.

### Phase 4: Optimization (Low Risk)

1. **Tune block size**: Profile to find optimal block size (32KB, 64KB, 128KB).
2. **Pre-warm arenas**: Pool arenas with pre-allocated blocks sized for typical
   queries.
3. **Thread arena through normalizer/rewriter** if profiling shows post-parse
   allocations are significant.
4. **Slice pre-allocation**: For slice-typed AST fields (e.g., `SelectExprs`,
   `OrderBy`), allocate backing arrays from the arena.
5. **String interning**: For frequently repeated column/table names, intern strings
   in the arena to avoid duplicate allocations.

---

## 4. Risk Assessment

| Risk | Mitigation |
|------|------------|
| Use-after-free if AST escapes the arena lifetime | `ParseResult.Release()` sets arena to nil; optional debug mode with poisoning |
| Performance regression from arena overhead | Benchmark every phase; fallback to heap allocation is always available |
| Grammar rule migration introduces parse bugs | Each batch is tested against the full 5,600+ test case suite |
| Generated code drift | Arena allocation code is generated by the same tooling as clone/rewrite/visit |
| Memory waste from large blocks for small queries | First block is 64KB (reused via pool); only complex queries allocate more |
| Thread safety | Arenas are not shared across goroutines; each parse call gets its own arena |

---

## 5. Expected Impact

Based on the existing benchmarks (`BenchmarkParseStress`), a typical parse of:
- Simple query: ~20-30 allocations → reduced to ~3-5 (arena block + tokenizer + result)
- Complex query: ~100-200 allocations → reduced to ~5-10

**Estimated improvements:**
- **50-80% reduction in allocations per parse** (allocs/op)
- **20-40% reduction in GC pressure** under sustained load
- **5-15% improvement in parse latency** (from reduced GC pauses and improved cache
  locality — arena objects are contiguous in memory)

These estimates should be validated with the existing benchmark suite before and after
implementation.

---

## 6. Alternatives Considered

### 6.1 Go Native `arena` Package (GOEXPERIMENT=arenas)

Go 1.20 introduced an experimental `arena` package. It provides exactly the semantics
we need (bulk allocation + bulk free). However:
- It is behind a build tag and not enabled by default
- The API is not stable and may change
- Vitess supports Go versions that predate this feature
- We would have no control over block sizing or alignment

**Verdict**: Not viable today. Worth revisiting when/if it becomes stable.

### 6.2 Object Pools (`sync.Pool` per Type)

A pool per AST node type (e.g., `literalPool`, `colNamePool`, etc.):
- Requires `Get()` + field reset for every allocation
- Requires `Put()` for every deallocation (must walk entire AST)
- Does not improve memory locality
- Does not reduce GC scanning (pooled objects still reference each other)
- 80+ pools to manage

**Verdict**: Higher complexity, lower benefit than an arena. Not recommended.

### 6.3 Flatbuffer / Protobuf AST Representation

Encode the AST as a flat binary structure:
- Would require a complete rewrite of the AST layer
- Incompatible with the current interface-heavy design (SQLNode, Expr, Statement)
- Would break all downstream consumers

**Verdict**: Too invasive. Not practical for Vitess.

### 6.4 Do Nothing

The current allocation pattern is functional and correct. The Go GC handles it well
for moderate workloads. However, at high QPS (100K+ queries/sec), the allocation
pressure becomes a measurable portion of CPU time. Pooling is a targeted optimization
for this regime.

**Verdict**: Acceptable for now, but leaves performance on the table for
high-throughput deployments.
