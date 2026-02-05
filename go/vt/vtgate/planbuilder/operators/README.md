# Vitess Query Planning - Operators Package

The operators package is the heart of Vitess query planning. It transforms SQL queries into an optimized execution plan for distributed MySQL databases, with the primary goal of **pushing as much work as possible down to MySQL shards** rather than processing data in the Vitess proxy layer.

## Core Philosophy

The fundamental principle is simple: **anything pushed under a Route will be turned into SQL and sent to MySQL**. Operations that remain above Routes must be executed in the proxy layer, which is significantly slower. Therefore, the planner aggressively tries to merge and push operations down under Routes.

## Planning Process Overview

```
SQL AST → Initial Operator Tree → Phases & Rewriting → Offset Planning → Final Plan
```

### 1. Initial Plan Construction
- Build operator tree from parsed SQL
- Create **QueryGraphs** at leaves (inner-joined tables)
- Wrap complex operations in **Horizon** operators
- Establish the foundation for optimization

### 2. Phased Planning & Rewriting
- Run multiple optimization phases
- Use fixed-point rewriting until no more changes occur
- Each phase targets specific query patterns

### 3. Offset Planning
- Transform AST expressions to offsets or evalengine expressions
- Prepare for execution

## Key Operator Types

### Route
**The most important operator** - represents work sent to MySQL shards.
- Contains SQL to execute on one or more shards
- Different routing strategies (single shard, scattered, etc.)
- Goal: Push as many operations under Routes as possible

### Horizon
A container for post-join operations that we hope to push down:
- SELECT expressions
- GROUP BY and aggregation
- ORDER BY
- LIMIT/OFFSET
- If pushable → goes under Route, otherwise expands into individual operators

### QueryGraph (QG)
- Represents tables connected by inner joins
- Allows flexible join reordering
- Gets planned into optimized join sequences
- Eventually becomes Route(s) or ApplyJoin(s)

### Join vs ApplyJoin
- **Join**: Logical operator for tracking outer joins
- **ApplyJoin**: Physical join algorithm (nested loop)
- **HashJoin**: Alternative physical join algorithm

### Core Operation Operators
When Horizons can't be pushed down, they expand into:
- **Aggregator**: GROUP BY and aggregate functions
- **Projection**: SELECT expression evaluation
- **Filter**: WHERE clause processing
- **Ordering**: ORDER BY sorting
- **Limit**: LIMIT/OFFSET processing
- **Distinct**: DISTINCT operation

## Planning Phases

The planner runs through several phases, each targeting specific optimizations:

```go
const (
    physicalTransform       // Basic setup
    initialPlanning         // Initial horizon planning optimization
    pullDistinctFromUnion   // Pull distinct from UNION
    delegateAggregation     // Split aggregation between vtgate and mysql
    recursiveCTEHorizons    // Expand recursive CTE horizons
    addAggrOrdering         // Optimize aggregations with ORDER BY
    cleanOutPerfDistinct    // Optimize Distinct operations
    subquerySettling        // Settle subqueries
    dmlWithInput           // Expand update/delete to dml with input
)
```

Each phase only runs if relevant to the query (e.g., aggregation phases skip if no GROUP BY).

## Routing Strategies

Different routing types handle various distribution patterns:

- **ShardedRouting**: Complex routing based on vindex predicates
- **AnyShardRouting**: Reference tables available on any shard
- **DualRouting**: MySQL's dual table (single row, no real table)
- **InfoSchemaRouting**: Information schema queries
- **NoneRouting**: Known to return empty results, no need to actually run the query.
- **TargetedRouting**: Specific shard targeting

## Push-Down Optimizations

The rewriter system uses a visitor pattern to optimize the tree:

```go
func runRewriters(ctx *plancontext.PlanningContext, root Operator) Operator {
    visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
        switch in := in.(type) {
        case *Horizon:
            return pushOrExpandHorizon(ctx, in)
        case *ApplyJoin:
            return tryMergeApplyJoin(in, ctx)
        case *Projection:
            return tryPushProjection(ctx, in)
        case *Limit:
            return tryPushLimit(ctx, in)
        // ... more optimizations
        }
    }
    return FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
}
```

### Key Optimizations

**Horizon Push/Expand**:
```
If possible:  Horizon → Route (push entire horizon to MySQL)
Otherwise:    Horizon → Aggregator + Ordering + Limit + ... (expand to individual ops)
```

**ApplyJoin Merging**:
```
ApplyJoin(Route1, Route2) → Route(ApplyJoin(table1, table2))
```
Merges two routes into one, pushing the join to MySQL.

**Limit Optimization**:
- Single shard: Push entire LIMIT down
- Multi-shard: Push LIMIT down but keep LIMIT on top for global limiting

**Filter Push-Down**:
```
Filter → Route  (push WHERE conditions to MySQL)
```

## Multi-Shard Challenges

When data spans multiple shards, certain operations require special handling:

### LIMIT Example
```sql
SELECT * FROM users LIMIT 10
```

**Single Shard**: `LIMIT 10` sent directly to MySQL
**Multi-Shard (4 shards)**: 
- Send `LIMIT 10` to each shard (gets ≤40 rows total)
- Apply `LIMIT 10` in proxy layer (final 10 rows)

### Aggregation Example
```sql
SELECT COUNT(*) FROM users GROUP BY country
```

**Pushable**: Single shard or sharded by `country`
**Split aggregation**: 
- MySQL: `SELECT country, COUNT(*) FROM users GROUP BY country`
- Proxy: Sum the counts per country

## Join Merging Logic

The join merging system (`join_merging.go`) determines when two Routes can be merged:

```go
switch {
case a == dual:                    // Dual can merge with single-shard routes
case a == anyShard && sameKeyspace: // Reference tables merge easily
case a == sharded && b == sharded:  // Complex vindex-based merging
default:                           // Cannot merge
}
```

**Dual Routing Special Case**: 
- Before merge: `:lhs_col = rhs.col` (parameter-based predicate)
- After merge: `lhs.col = rhs.col` (regular predicate)
- PredTracker handles this transformation

## Horizon Expansion

When a Horizon can't be pushed under a Route, it expands systematically:

```go
// Original: Horizon(SELECT col1, COUNT(*) FROM t GROUP BY col1 ORDER BY col1 LIMIT 10)
//
// Expands to:
Limit(10,
  Ordering(col1,
    Aggregator(COUNT(*), GROUP BY col1,
      Projection(col1,
        Route(...)))))
```

## Error Handling & Fallbacks

The planner includes sophisticated fallback mechanisms:

- **Failed optimizations**: NoRewrite result, try alternative approaches
- **Unsupported features**: Fall back to proxy-layer processing
- **Complex queries**: Expand Horizons into manageable components

## Debugging Support

Set `DebugOperatorTree = true` to see the operator tree at each phase:

```
PHASE: initial horizon planning optimization
Route(SelectEqual(1) sharded)
  └─ Horizon(SELECT id, name FROM users WHERE id = :id)

PHASE: delegateAggregation  
Route(SelectEqual(1) sharded)
  └─ Aggregator(COUNT(*))
    └─ QueryGraph(users)
```

## Performance Considerations

**Critical Path**: Every operation above Routes adds latency and resource usage in the proxy layer.

**Memory Usage**: Large result sets processed in proxy require significant memory.

**Network Traffic**: Multiple round-trips between proxy and MySQL for complex operations.

**Optimization Priority**:
1. Push entire queries to single Routes
2. Merge multiple Routes where possible
3. Push individual operations as far down as possible
4. Minimize proxy-layer processing

## Usage Example

```go
// Build initial operator tree from AST
op := createOperatorFromAST(ctx, stmt)

// Run planning phases
optimized := runPhases(ctx, op)

// Add offset planning
final := planOffsets(ctx, optimized)

// Convert to execution engine
primitive := convertToPrimitive(final)
```

The result is an optimized execution plan that maximizes MySQL utilization while minimizing proxy-layer overhead, enabling Vitess to efficiently handle complex queries across distributed MySQL clusters.