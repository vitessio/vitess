# Subqueries in VTGate

# Introduction

This document builds on top of [The V3 high level design](https://github.com/vitessio/vitess/blob/master/doc/V3HighLevelDesign.md). It discusses implementation of subquery support in greater detail.



# Recap

Here are the list of possible use cases for subqueries:

1. A simple value: `select * from t where id = (select 1 from dual)`
2. A value tuple: multiple values of the same type: `select * from t1 where id in (select val from t2)`
3. A row tuple: multiple values of different types. A row tuple can also be used as a value: `select * from t1 where (a,b) = (select b,c from t2 where pk=1)`
4. Rows: results of a regular query. Note that rows can be treated as a value tuple of row tuples. For example, this statement is valid: `select * from t1 where (a,b) in (select b,c from t2)`
5. A virtual table: If you don’t strip out the field info from the original query, then a select can act as a table, where the field names act as column names. For example, this statement is valid: `select * from (select a, b from t2) as t where t.a=1`

For now, we'll stay away from row tuples, which is use case 3. We'll eventually support use case 4 for insert statements. Use case 5 is already supported.

## Correlation

Subqueries may or may not be correlated. For the sake of optimization, they must be treated differently. An uncorrelated subquery can be pulled out of the expression and its values can be pre-evaluated. However, a correlated subquery cannot afford that luxury. Values from the returned row have to be substituted in the subquery, and then its result has to be applied to the rest of the expression it's part of.

### Uncorrelated subquery

`select * from a where id in (select id from b)`

Can be executed as:

`select id from b` -> store result in var `::list`

`select * from a where id in ::list`

### Correlated subquery

`select * from a where id in (select id from b where b.val=a.val)`

In the above case, the subquery cannot be pulled out. Its plan would be as follows:

`select *, val from a` -> store val in `::a_val`

For each row:

`select id from b where b.val= :a_val` -> store result in var `::list`

`IN clause primitive`: Verify `id IN ::list`

### Partially correlated subquery

Subqueries can also be partially correlated. Here's an example:

`select * from a where id in (select id from b where val in (select id from c where c.val=a.val))`

In the above case, the innermost query on `c` is not correlated to the middle level query on `b`. But it's correlated with the outermost query on `a`.

In the above case, we can perform a partial pull-out where the innermost query is pulled out of the `b` expression but stays within the `a` expression.

# Vitess handling

Vitess can detect if a correlated subquery can be safely pushed down into a route. If this is the case, then we just do that and don't have to worry about complexity (feature is done). This leaves us to handle the following two use cases:

1. Uncorrelated subqueries.
2. Correlated subqueries that cannot be pushed down.

In both the above cases, we need the ability to execute a subquery and convert the result to a value or list of values. The handling of the uncorrelated case is easy:

* In the original expression, replace the subquery with a bind var.
* Create a primitive: pulloutSubquery. It will execute the subquery, save the result in a bind var, and invoke the rest of the primitive.

In the case of a correlated subquery, we have to delay its execution until the primitive has executed, and then invoke the subquery. However, in these cases, the subquery is usually part of an expression. This means that we need the ability to evaluate expressions in VTGate. So, we'll delay handling of correlated subqueries until this capability is added.

## Complications

Substitution of subquery results with values doesn't work in all cases. If we look at the grammar, subqueries can be used in the following contexts:

* IN or NOT IN clause
* EXISTS clause
* As a value in any expression

One may think that you could always replace an expression like `IN (subquery)` with `IN ::values`. However, this will fail if the subquery yields no values. This is because `IN ()` results in a syntax error.

What if we supplied a `null` value instead? This will change the expression to `IN (null)`, which seems to work. However, the result of that expression is not `false`. It's instead `null`, which interacts in odd ways when combined with other expressions. Additionally, `NOT in (null)` also yields `null`, which is the exact opposite of what is expected.

The proposed solution is to rewrite those expressions as follows:

`expr IN (subquery)` -> `(:list_has_values and (expr IN ::list))`

In the above case the IN clause will be evaluated only if `:list_has_values` is `true`. If it's `false`, it will be skipped. Similarly:

`expr NOT IN (subquery)` -> `(:list_is_empty or (expr NOT IN ::list))`

The `EXISTS` clause and value expressions don't require special handling.

## Substitutions

In order to perform the above rewrites, we need the ability to substitute expressions. This is currently not possible with the existing `Walk` functionality in sqlparser. It is difficult to change this function to support substitutions. This is because the every member type of a struct will require different handling.

Instead, we'll implement a new function that works only for expressions: `ReplaceExpr`. This function will work similar to `Walk`, but will allow you to return an alternate expression that can replace the original expression. Because expressions have limited scope, the function will not be as complex.

## Positioning of the pulloutSubquery primitive

As explained above, subqueries can be:
1. Completely correlated.
2. Partially correlated.
3. Completely uncorrelated.

We can handle cases 2 and 3 with the pulloutSubquery primitive. The completely uncorrelated case is trivial because the subquery can be the top primitive, which will get it executed first.

A partially correlated subquery is similar to the completely uncorrelated subquery because of how the analysis works:

`select * from a where id in (select id from b where val in (select id from c where c.val=a.val))`

1. Start analyzing a, encounter b.
2. Start analyzing b, encounter c.
3. Analyze c.
4. Comparing c against b will declare that c does not depend on b. So, we pull c out to be the first primitive for b.
5. Finish analyzing b.
6. Analyzing b will make it correlated to a (because of c).

The `findOrigin` function can already peform this analysis.

So, in the above case, c cannot be pulled out beyond b. If c was not correlated with a, then we have the opportunity to pull it out further.

### Generalizing pull-outs

The pull-out algorithm can be generalized as follows:

1. Compute the primitive the subquery depends on: subqueryFrom
2. Compute the primitive the full expression depends on: subqueryTo
3. If subqueryFrom==subqueryTo, then it's a fully correlated subquery. It cannot be pulled out.
4. If subqueryFrom is non-nil, then it's a partial correlation. The subquery can be pulled up in the tree up to a node that contains both subqueryFrom and subqueryTo.
5. If subqueryFrom is nil, then it can be pulled all the way out.

### Join Vars

We'll need to generate new bind var names while substituting subqueries. This will require us to plumb the list of existing bind var names through the call stack to ensure that names don't collide when we generate new ones.
