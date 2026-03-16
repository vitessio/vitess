# Arena Memory Pooling - Implementation Status & Next Steps

## What Has Been Done

### 1. Arena Infrastructure (`arena.go`)
- **`nodePool[T]`**: Generic chunk-based allocator using typed slices (`[]T`) for GC safety. Allocates 32 nodes per chunk, handing out pointers to individual elements.
- **`Arena` struct**: Contains 47 typed `nodePool` fields for the most frequently allocated AST node types.
- **Value-copy methods** (`newTypeV`): Nil-receiver safe methods on `*Arena` that accept a struct by value, copy it into the pool, and return a pointer. Falls back to heap allocation when arena is nil. This design minimizes syntax changes in `sql.y`.

### 2. Arena-Aware Helper Functions (`arena_helpers.go`)
- `newStrLiteralA`, `newIntLiteralA`, `newFloatLiteralA`, `newDecimalLiteralA`, `newHexNumLiteralA`, `newHexLiteralA`, `newBitLiteralA`, `newDateLiteralA`, `newTimeLiteralA`, `newTimestampLiteralA` — arena variants of `New*Literal()`.
- `newWhereA` — arena variant of `NewWhere()`.
- `newSelectA` — arena variant of `NewSelect()` (includes option parsing logic).

### 3. goyacc Modification (`goyacc/goyacc.go`)
- Added `Arena *Arena` field to the `$$ParserImpl` struct template (line ~3374). This makes `yyrcvr.Arena` available in all grammar rule actions.

### 4. Parser Entry Points (`parser.go`)
- **`arenaPool`**: `sync.Pool` for reusing `Arena` objects across parse calls.
- **`yyParsePooledArena(yylex, arena)`**: Like `yyParsePooled` but attaches arena to the parser. Arena is detached before parser returns to pool.
- **`yyParsePooled(yylex)`**: Updated to delegate to `yyParsePooledArena` with nil arena (backward compatible).
- **`ParseResult`** struct: Bundles `Statement` + `BindVars` + arena. Has `Release()` method to return arena to pool.
- **`Parse2WithArena(sql)`**: New entry point that allocates an arena, parses, and returns `*ParseResult`. Caller must call `Release()` when done.

### 5. Grammar Rule Conversion (`sql.y`)
- **273 allocation sites converted** using `convert_arena.py` script.
- Pattern: `&Type{fields}` → `yyrcvr.Arena.newTypeV(Type{fields})`
- Helper calls: `NewStrLiteral(x)` → `newStrLiteralA(yyrcvr.Arena, x)`, etc.
- All `&BinaryExpr{`, `&ComparisonExpr{`, `&ColName{`, `&Literal{` (via helpers), `&Select{` (via helper), `&Where{` (via helper), and 40+ other types converted.

### 6. Regenerated Parser (`sql.go`)
- `sql.go` regenerated from modified `sql.y` using the custom goyacc (`go run ./goyacc -fo sql.go sql.y`). 3 pre-existing shift/reduce conflicts (unchanged).

### 7. Conversion Script (`convert_arena.py`)
- Python script that mechanically converts `&TypeName{...}` allocations and `New*()` helper calls in `sql.y`. Handles brace matching for single-line and nested allocations.

---

## What Needs To Be Done Next

### Step 1: Verify Compilation
```bash
cd go/vt/sqlparser
go build vitess.io/vitess/go/vt/sqlparser
```
Fix any compilation errors. Likely issues:
- Type mismatches in converted grammar rules (some complex multi-line allocations may need manual adjustment)
- Missing arena method for a type not in the 47-type pool (would need to add it to `Arena` struct and generate the `newTypeV` method)

### Step 2: Run Existing Tests
```bash
cd go/vt/sqlparser
go test -count=1 -timeout 300s ./...
```
The full test suite has 5,600+ test cases. All must pass to ensure the arena conversion didn't change parse semantics. The nil-arena fallback means `Parse2()` (without arena) should behave identically to before.

### Step 3: Write Arena-Specific Benchmarks
Add to `parse_test.go`:

```go
func BenchmarkParseWithArena(b *testing.B) {
	const (
		sql1 = "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
		sql2 = "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
	)

	for i, sql := range []string{sql1, sql2} {
		b.Run(fmt.Sprintf("sql%d/heap", i), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Parse(sql)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("sql%d/arena", i), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := Parse2WithArena(sql)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}

func BenchmarkParseTracesWithArena(b *testing.B) {
	for _, trace := range []string{"django_queries.txt", "lobsters.sql.gz"} {
		queries := loadQueries(b, trace)
		if len(queries) > 10000 {
			queries = queries[:10000]
		}

		b.Run(trace+"/heap", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, query := range queries {
					_, _ = Parse(query)
				}
			}
		})
		b.Run(trace+"/arena", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, query := range queries {
					result, err := Parse2WithArena(query)
					if err == nil {
						result.Release()
					}
				}
			}
		})
	}
}
```

### Step 4: Run Benchmarks
```bash
cd go/vt/sqlparser
go test -bench=BenchmarkParseWithArena -benchmem -count=5 -run=^$ .
go test -bench=BenchmarkParseStress -benchmem -count=5 -run=^$ .
go test -bench=BenchmarkParseTracesWithArena -benchmem -count=5 -run=^$ .
```

Use `benchstat` to compare heap vs arena results.

### Step 5: Cleanup
- Remove `convert_arena.py` (one-time conversion tool, no longer needed)
- Remove `MEMORY_POOLING_PLAN.md` and this file if the implementation is accepted
- Clean up any genproto stubs from `/root/go/pkg/mod/` if they were created

### Step 6: Optional Further Optimization
- Tune `nodePoolChunkSize` (currently 32) based on profiling
- Add more types to the Arena if profiling shows other types are allocation-heavy
- Thread arena through `Normalize()` and `RewriteAST()` for post-parse allocations
- Consider arena-aware `CloneRefOf*` functions for clone-heavy workloads

---

## File Inventory

| File | Status | Description |
|------|--------|-------------|
| `arena.go` | NEW | nodePool[T] + Arena struct + newTypeV methods |
| `arena_helpers.go` | NEW | Arena-aware NewSelect/NewWhere/NewLiteral helpers |
| `convert_arena.py` | NEW | One-time conversion script (can be deleted after validation) |
| `goyacc/goyacc.go` | MODIFIED | Arena field added to $$ParserImpl template |
| `parser.go` | MODIFIED | Parse2WithArena, ParseResult, arenaPool, yyParsePooledArena |
| `sql.y` | MODIFIED | 273 allocation sites converted to arena |
| `sql.go` | MODIFIED | Regenerated from sql.y |
| `MEMORY_POOLING_PLAN.md` | EXISTING | Original design document |
