# V3 high level design

# Objectives

The goal of this document is to describe the guiding principles that will be used to implement the full SQL feature set of Vitess’s V3 API. We start off with the high level concepts on SQL and its features, and iterate our way into some concrete design proposals.

### Prerequisites

Before reading this doc you must be familiar with [vindexes](https://github.com/youtube/vitess/blob/master/doc/V3VindexDesign.md), which is used as foundation for the arguments presented here.

# Background

VTGate was initially conceived as a router that encapsulated the logic of deciding where to send the application’s queries. The primary objective was to isolate the application from maintenance events like reparenting, server restarts, and resharding. As of the V2 API, this problem is pretty much solved.

One drawback of the V2 API is that it requires the application to be aware of the sharding key. This key had to be sent with every query. So, unless an application was written from the ground up with this awareness, the task of migrating it to use vitess was rather daunting.

The question was asked: could VTGate be changed to not require the sharding key in its API? Could VTGate make the sharded database look like a single database?

This was how V3 was born. A working proof of concept was developed. This showed that we can automatically route queries that targeted single shards. We could also route queries that targeted multiple shards as long as the results could be combined without any additional processing.

The next question was: Can we support more SQL constructs? How far can we go? Can we go all the way and support everything?

These are the questions we’ll try to answer in the subsequent sections.

# TL;DR

This section gives a preview of what this document tries to achieve.

## Current V3

The current V3 functionality is capable of correctly serving simple SQL statements that reference a single table. For example, if id is the sharding column for a:

`select a.col from a where a.id=:id` will get routed to the correct shard.

`select a.col from a where a.id in ::list` will get broken out into shard-specific in statements and routed to those specific shards

`select a.col from a where a.col=:col` will get sent to all shards and results will be combined and returned

The current V3 also supports simple DMLs. More complex SQLs like joins, aggregations and subqueries are not supported.

## Next phase: support more queries

If the first phase of this document is implemented, VTGate will be able to support the following constructs:

### Joins

`select a.col, b.col from a join b on b.id=a.id where a.id=:id` will get routed to the correct shard. Correspondingly, IN clauses will also will get correctly broken out and routed.

`select a.col, b.col from a join b on b.id=a.id `will get sent to all shards.

If the data is on multiple shard, VTGate will perform the join:

`select a.col, b.col from a join b on b.id2=a.id where a.id=:id`

will get rewritten as two queries, one dependent on the other:

```
select a.col, a.id from a where a.id=:id -> yield "_a_id" from col 1
select b.col from b where b.id2=:_a_id
```

Joins can group tables, as long as the order is correctly specified:

`select a.col, b.col, c.col from a join b on b.id=a.id join c on c.id2=a.id where a.id=:id`

will get rewritten as:

```
select a.col, b.col, a.id from a join b on b.id=a.id where a.id=:id
select c.col from where c.id2=:_a_id
```

Joins can also group other where clauses and wire-up cross-table dependencies:

`select a.col, b.col from a join b on b.id2=a.id where a.id=:id and b.col > a.col`

will get rewritten as:

```
select a.col, a.id from a where a.id=:id
select b.col from b where b.id2=:_a_id and b.col > :_a_col
```

Joins can also be cascased:

`select a.col, b.col, c.col from a join b on b.i2=a.id join c on c.id3=b.id2 where a.id=:id`

will get rewritten as:

```
select a.col, a.id from a where a.id=:id
select b.col, b.id2 from b where b.id2=_a_id
select c.col from c where c.id3=_b_id2
```

LEFT JOIN will also be supported for all the above constructs. However, WHERE clauses that add additional constraints to the nullable parts of a left join will not be supported; They cannot be pushed down.

### Subqueries

Correlated subqueries that can be executed as part of the main query will be supported:

`select a.id from a where exists (select b.id from b where b.id=a.id) and a.id=:id` will get routed to the correct shard.

Correlated subqueries can also be sent as scatter queries, as long as the join constraints are met.

Subqueries in the FROM clause can be joined with other tables:

`select a.id, a.c, b.col from (select id, count(*) c from a) as a join b on b.id=a.id`

will get rewritten as:

```
select a.id, a.c from (select id, count(*) c from a) as a
select b.col from b where b.id=:_a_id
```

Subqueries in the WHERE clause and SELECT list will be supported only if they can be grouped. They will not be broken out.

### Aggregation

Aggregation will be supported for single-shard queries, but they can contain joins and subqueries.

You can also use aggregation for scatter queries if a unique vindex is in the list of result columns, like this one:

`select id, count(*) from a`

### Sorting

Sorting will be supported as long as it can be meaningfully pushed down into the grouped parts. For example,

`select a.col, b.col from a join be where b.id=a.id order by a.id, b.id`

will be rewritten as:

```
select a.col from a order by a.id order by a.id
select b.col from b where b.id=:_a_id order by b.id
```

### All combinations

The above features are designed to be orthogonal to each other. This means that VTGate can support simultaneous combinations of the above features, and with arbitrary levels of nesting and cascading.

## Going beyond

Once the above phase is complete, we can iterate on performance features as well as add support for more constructs. It will depend on customer demand.

# High level concepts

The V3 proof of concept drew parallels between database indexes and vitess’s indexing scheme. A database deals with tables, but VTGate deals with databases. It turns out that a table and a database have a surprising number of similarities. The basic guarantee of an index is that it lets you limit the number for rows you have to inspect in order to compute the result of a query. In the same way, vindexes give you the guarantee that it will be sufficient to send your queries to a subset of all the shards.

As a quick recap, a vindex is a cross-shard index maintained by vitess. Given a column constraint, the vindex is one that’s capable of telling which shards contain the rows needed to satisfy a query. In this document, we’ll use the term ‘unique vindex’ quite often. A unique vindex is one that can produce only one shard as target for a column equality constraint. However, there may still be many rows that match that constraint. But they’ll always be within that one shard.

The following table further illustrates the similarities of the concepts:

<table>
  <tr>
    <td>RDBMS</td>
    <td>Vitess</td>
  </tr>
  <tr>
    <td>Table</td>
    <td>Database</td>
  </tr>
  <tr>
    <td>Primary Key</td>
    <td>Sharding Key</td>
  </tr>
  <tr>
    <td>Index</td>
    <td>Vindex</td>
  </tr>
  <tr>
    <td>Foreign Key</td>
    <td>Shared Vindex</td>
  </tr>
</table>


This coincidence is not accidental. For example, if you used a vindex to route a query to a shard, it’s likely that the database also has an index on that same column, with properties similar to the vindex. If you have a shared vindex between two tables, it’s very likely that there exists a similar foreign key relationship between the tables.

The foundation for these similarities comes from the fact that a table that was once in a single database is now partitioned out. So, the indexes for those tables have to also grow out of the original database.

There is another strong parallel between a table and a database: A SELECT statement and a table are interchangeable. On one hand, all the data in table t can be represented as `‘select * from t’. `On the other hand, you can use a SELECT statement as stand-in for a table. So, many of the approaches used to optimize SQL for scanning tables can be re-used to orchestrate queries that span multiple shards or databases. The main difference is that you can treat bigger SQL chunks as tables, and can outsource that work to the database that hosts them.

But then, there’s only so much you can outsource. As soon as a query exceeds a certain level of complexity, where the required data needs to be pulled from more than one database, then any subsequent work needs to be done by VTGate. If we want to support the full SQL syntax, VTGate will have to eventually become a full-fledged SQL engine.

VTGate’s SQL engine will likely not be as good as what the underlying database can do, at least initially. So, until such time, the goal will be to push as much of the work down to the databases, and do as little as possible with what comes out.

## Limitations

The above arguments can more or less be used as proof that we can theoretically support the full SQL syntax. In the worst case scenario, VTGate will have to individually scan each table involved in the query and perform the rest of the work by itself.

But there are some things it cannot do:

### Cross-shard consistency

There is currently no way to get a system-wide consistent snapshot of all the keyspaces and shards. If reads are being performed during in-flight cross-shard transactions, then VTGate may see such data as committed in one database, and not committed in the other.

We could rely on MySQL’s MVCC feature that will give you a view of each database as of when a transaction was started, but this is still not perfect because there is no guarantee about the exact timing of each BEGIN, and there are also replication lag related inconsistencies. We currently don’t support read transactions for replicas, but it can be added.

The final option is to lock those rows before reading them. It’s a very high price to pay, but it will work.

### Environmental differences

VTGate will be running on a different machine than the databases. So, functions that rely on the current environment will not produce consistent results. For example, time functions will likely have variations depending on clock skew.

# SQL for dummies

An upfront clarification: we’ll be only focusing on SELECT statements in this document, because that’s where all the complexities are. Also, Vitess currently allows only single-shard DMLs. So, most of this analysis will not be applicable for them.

## Dissecting SELECT

A single table SELECT is a relatively simple thing. Let’s dissect the following statement:

```
select count(*), a+1, b
from t
where c=10
group by a+1, b
having count(*) > 10
order by count(*) desc
limit 10
```

The steps performed by the SQL engine are as follows:

1. Scan t (FROM)
2. Filter rows where c=10 (WHERE)
3. Remove all columns other than a & b, and compute a+1 (SELECT)
4. Count and group unique a+1, b values (GROUP BY)
5. Filter counts that are > 10 (HAVING)
6. order the rows by descending count (ORDER BY)
7. Return the top 10 results (LIMIT)

The one thing that’s not obvious in a SELECT is that the select list is not the first clause that’s executed. It’s the FROM.

## SELECT as a subquery

If a SELECT statement was used just to fetch results, things would be very simple. However, it can be used in other parts of a query, and can produce 5 kinds of values:

1. A simple value: `select * from t where id = (select 1 from dual)`
2. A value tuple: multiple values of the same type: `select * from t1 where id in (select val from t2)`
3. A row tuple: multiple values of different types. A row tuple can also be used as a value: `select * from t1 where (a,b) = (select b,c from t2 where pk=1)`
4. Rows: results of a regular query. Note that rows can be treated as a value tuple of row tuples. For example, this statement is valid: `select * from t1 where (a,b) in (select b,c from t2)`
5. A virtual table: If you don’t strip out the field info from the original query, then a select can act as a table, where the field names act as column names. For example, this statement is valid: `select * from (select a, b from t2) as t where t.a=1`

This opens the door to some powerful expressibility, the other edge of the sword being indefinite complexity.

Subqueries can be found in the following clauses:

1. FROM
2. SELECT list
3. WHERE

In a way, subqueries are brain-dead, including the correlated ones. You just execute your main query as specified by the sequence in the ‘Dissecting SELECT’ section. Any time you encounter a subquery, you just evaluate it as an expression on-the-fly and use the resulting value. The only complication of a correlated subquery is that it references a value from the outer Result to produce its own Result.

An immediate optimization: If a subquery is not correlated, we know that its value is the same for every row of the outer query. So, such a subquery can be "pulled out" and evaluated only once, or we could use lazy evaluation and caching. But we’re venturing into optimizer territory, which we should refrain from for now.

Based on the above properties, we can make some observations:

* A subquery in a FROM clause cannot reference columns of the SELECT list. This is because of the execution order: the SELECT list depends on what comes out of the FROM clause, not the other way around. However, such a subquery can still reference columns from an outer SELECT. But MySQL is more restrictive, and prevents all correlation for subqueries in the FROM clause.
* A subquery in a SELECT list cannot affect the original number of rows returned. All it can do is supply the value for the column it was meant to compute the result for. If there was no result, the value would be NULL. A subquery in a SELECT list must produce a single scalar value.
* A subquery in a WHERE clause cannot change any values of the original result, or produce new rows. It can only cause rows to be eliminated.

It’s easy to get lost in the world of subqueries, and it may get confusing when you start to think about how they interact with joins. But you can retain sanity by following these principles while analyzing a complex query:

* Tables involved in a join are within the same scope, and can reference each other's columns.
* Subqueries are in an inner scope. An inner query can use values from the outer query. Other than this, all it can do is produce a value.
* Other parts of the outer query cannot reference anything about the subquery. It’s a black box in that respect.

Correlated subqueries exhibit the properties of a join. So, there are situations where they could be converted to actual joins by the optimizer.

It’s now time to drill down into the details of the operators. The notations below are not very formal. They’re there mainly to illustrate the concepts.

## What kind of machine is SQL?

Primer on automata theory: [https://en.wikipedia.org/wiki/Automata_theory](https://en.wikipedia.org/wiki/Automata_theory).

Is SQL a turing machine? The answer is no, because it can only make forward progress, and always terminates.

Strangely, there are no references on the internet about what kind of machine SQL is. Looking at whether it can be a finite state machine, the answer seems to be no, because of subqueries.

So, it appears to be a Pushdown Automaton (PDA). This determination is important because it will help us decide the data structure needed to represent an execution plan.

Specifically, a tree structure is sufficient to represent a PDA.

## The operands

This is the list of ‘values’ that a query engine deals with in order to satisfy a query:

1. Result: This is the core data type that every operator deals with. It’s a complex data structure. Sometimes, the primitives may access only a part of the Result. So, the interchangeability rules of primitives will be different based on which parts of the Result each of them accesses.
2. Value expression list: This is used in various places in a statement. Most notably, for converting a table scan into the list of values to be returned.
3. Conditional expression: This is used to filter out results.
4. Aggregate expression list: This has some similarities with a value expression list, but it’s worth differentiating.
5. Scalar values: Used all over.

The above operands are a simplified way of looking at things. For example, a conditional expression is a separate world that has its own set of operators and values, and they follow a different set of algebraic rules. But we need to divide and conquer.

You’ll see how these operands are used in the description of the operators.

## The operators

If one were to ask what are the minimal set of primitives needed to serve SQL queries, we can use [relational algebra](http://www.tutorialspoint.com/dbms/relational_algebra.htm) as starting point. But databases have extended beyond those basic operations. They allow duplicate tuples and ordering. So, here’s a more practical set:

1. Scan (FROM).
2. Join: Join two results as cross-product (JOIN).
3. LeftJoin: The algebraic rules for an outer join are so different from a Join that this needs to be a separate operator (LEFT JOIN).
4. Filter: Removing rows based on a conditional expression (WHERE, HAVING).
5. Select: Evaluating new values based on a value expression list (SELECT list).
6. Aggregate: Grouping rows based on an aggregate expression list (GROUP BY, UNION).
7. Sort (ORDER BY).
8. Limit (LIMIT).
9. Merge: Merge two results into one (UNION ALL).

Windowing functions are out of scope for this doc.

Even the above primitives are fairly theoretical; One would rarely perform a Join without also simultaneously applying a join condition. 

For a given SELECT statement, the order of these operators are fixed. They are as follows:

`Scan <-> (Join & LeftJoin) -> Filter(where) -> Select -> Aggregate -> Filter(having) -> Sort -> Limit -> Merge`

However, following the above order is almost never optimal. This is where we have to study how these operators interact, and try to find ways to resequence them for higher efficiency.

It’s important to note that most of these operators take a Result as input and produce one as output. This means that they can be indefinitely combined. This is achieved by re-SELECTing from a Result. It’s usually rare that people perform such nested selects, but there are legitimate use cases, typically for computing aggregates of aggregates.

Additionally, Filter and Select can perform their own scans (subqueries) as part of their expressions. This adds another dimension to the overall complexity.

### Scan

This is the most basic operation: given a table name, the operation produces a result, which is a list of all the rows in the table, with field names:

`Scan(tableName) -> Result`

Rethinking this for VTGate, the corresponding operation would be to route a query that targets a single database, and the output would be the result.

`Route(query, keyspace, shard) -> Result`

It’s implied that the query is the sql+bindvars.

*If every operator takes a Result as input, who produces the first Result?*

Scan is in this category. Without this operator, there is nothing that can map a query to a Result. Consequently, this is also the lowest level building block that all other operations depend on.

It turns out that this is the only part of VTGate that differs from a traditional SQL engine. But it’s a powerful difference. For well tuned apps, the entire query will most of the time be just passed through to one of the underlying databases.

The optimizer is likely to be very different, because its main task will be to figure out how to break up a query into parts that can be outsourced to different databases. For example, if there’s a three-way join and if two of those pieces can be sent to a single shard, then it has to split that query correctly into two. Because of this, we need the ability to translate a relational expression back into a query, or somehow find a way to reuse the parts of the original query that pertain to the relational expression.

### Filter

The filter operator removes rows from the Result based on a condition:

`Filter(Result, condition) -> Result`

A condition is a boolean expression. The operands can be:

* A column of the Result
* A column of an outer Result (to support correlated subqueries)
* Boolean values or expressions
* Scalar values or expressions
* The result of a subquery

The operators are:

* Pure boolean operators like AND, OR, etc.
* Comparison operators like =, <, etc.
* Other complex operators and functions like IF, etc.

Inferred properties of Filter:

* The filter operation is applied to each row individually. There is no interaction between rows.
* A row is never modified.
* No new rows are produced.
* It does not change the order of the rows in the Result. This is not a requirement, but it’s a property worth maintaining.
* The AND clauses of a filter can be broken into parts and applied separately. This is useful for pushing down WHERE clauses into scans.

It would have been nice if the Result was the only variable input to Filter. However, this is not true if one of the operands is a subquery.

*There are two schools of thought when it comes to boolean algebra: Those that allow interconversion between scalar and boolean values, and those that don’t. The former considers any non-zero scalar value to be TRUE, and zero as FALSE. The latter requires the scalar value to be tested using a comparison operator before it becomes a boolean value.*

*The first school of thought allows statements like this:*

`select * from t where a`

*The second school of thought requires you to rewrite it as:*

`select * from t where a != 0`

*Vitess’s grammar currently doesn’t allow this interconversion, but MySQL does. From a practical perspective, this difference is not significant because it results in a minor syntactic inconvenience. One could argue that ‘a’ could be a boolean column. But boolean values are not common in databases, and there are other alternatives. So, Vitess doesn’t support boolean column types. It assumes that everything from a database (or Result) is a scalar.*

*Not allowing interconversion greatly simplifies the grammar. Also, the boolean engine and expressions engine can be kept separate, which is another simplification.*

### Select

The Select operation produces a new Result by performing arithmetic operations to the input Result.

`Select(Result, expression list) -> Result`

An expression is a subset of a boolean expression. The operands can be:

* A column of the Result
* A column of an outer Result
* Scalar values or expressions
* The result of a subquery

The operators can be:

* Arithmetic operators like +, -
* Functions and other complex expressions

Properties of a Select:

* A Select produces the same number of rows as the input
* A Select may not produce the same columns as its input
* The expressions are applied to each row individually. There is no interaction between rows.
* The order of the result is not changed. This is not a requirement, but it’s a property worth maintaining.
* The different items (columns) of a Select can be broken into parts and independently computed.

### Aggregate

Aggregate splits the Result column into two groups: the aggregate columns and the group by columns. It merges the rows whose group by columns are equal, and simultaneously applies aggregation functions to the aggregate columns.

`Aggregate(Result, aggrExpressionList(columns)) -> Result`

Any columns that are not in the aggregate expression list are implicitly in the group by list.

There are two special cases:

* There are no aggregate columns. In this case, the operation degenerates into a dedup (UNION).
* There are no group by columns. In this case, one and only one row is produced.

The SQL syntax allows expressions to be present in aggregate functions, but that can be broken down into Select->Aggregate.

Properties of Aggregate:

* It produces no more rows than the input.
* It produces the same number of columns as the input.
* Aggregate can be efficient if the rows are sorted by the group by columns. If so, it also retains the sort order.
* Aggregate can also be efficient if there are no group by columns. Then only one row is produced in the Result.
* The different items of an Aggregate cannot be independently computed.

### Sort

Sort reorders the rows of a Result based on a list of columns with ascending or descending properties.

`Sort(Result, (columns asc/desc)) -> Result`

Properties:

* The number of rows produced is the same as the input
* No rows are changed
* An outer Sort supersedes an inner Sort
* Sorting can be broken into parts as long as the resulting row is built in the same order as the values referenced by the sort. For example, if we join a with b, and sorting is by a.col and b.col, then we can sort a by a.col and join with b by sorting b by b.col.

### Limit

Limit returns at the most N rows from the specified starting point.

`Limit(Result, start, count) -> Result`

Limit has algebraic properties that are similar to Filter.

Properties:

* A limit doesn’t change the rows
* An outer limit supersedes an inner Limit
* A Limit is not interchangeable with anything other than Select.
* A Limit without a Sort has non-deterministic results.

### Join

Join produces the cross-product of two Results.

`Join(Result, Result) -> Result`

This primitive is too inefficient in most situations. Here are some practical composites:

`InnerJoin(Result, Result, condition) ⇔ Filter(Join(Result, Result), condition)`

The use case for InnerJoin is easy to explain, because pushing down a condition usually leads to valuable efficiency improvements.

`QueryJoin(query, query, condition) ⇔ Filter(Join(Scan(query), Scan(query)), condition)`

The QueryJoin is relevant mainly because VTGate may not get good at performing joins for a long time. So, it may have to rely on sending nested loop queries to the underlying databases instead. However, there will be situations where a join will have to be performed on two Results. Such queries will be out of VTGate’s ability to execute until it implements the `Join(Result, Result)` primitive.

Properties:

* Rows are not modified
* Lots of rows can be produced
* Interchangeable with Filter as long as the condition can be split properly. This will be a significant driving factor for the optimizer.

### LeftJoin

LeftJoin requires a join condition. If the condition is not satisfied, the left side of the Result is still produced, with NULL RHS values.

`LeftJoin(LeftResult, RightResult, condition) -> Result`

It’s important to differentiate LeftJoin from Join because LeftJoin is not freely interchangeable with Filter or other Joins.

### Merge

Merge merges two results into one. There is no particular order in the rows.

`Merge(Result, Result) -> Result`

Properties:

* Rows are not modified
* Number of rows is the sum of the two Results’ number of rows

## Example from hell

Now that all operators are defined, we have to see if they’re complete. Let’s try this query:

```
01: (select count(*), ta1.col+1, tb1.col 
02: from ta1 join tb1 on ta1.id=tb1.id
03: where ta1.col2=10 and ta1.col2 > any
04:   (select c from tc1 where tc1.id=ta1.id)
05: group by ta1.col+1, tb1.col
06: having count(*) > 10
07: order by count(*) desc
08: limit 10)
09:union
10: (select count(*), ta2.col+1, tb2.col 
11: from ta2 join tb2 on ta2.id=tb2.id
12: where ta2.col2=10
13: group by ta2.col+1, tb2.col)
14:order by count(*) desc
15:limit 5
```

The above query can be represented as a single expression. But let’s start with something that has intermediate results first:

```
// query 1
02: JoinResult1 = InnerJoin(Scan(ta1), Scan(tb1), ta1.id=tb1.id)
// Note that the next operation is within the scope of JoinResult1, so its columns can be used. Nevertheless, it’s an external input.
04: SubqueryCScanResult = Select(
      Filter(
        Scan(tc1),
        tc1.id=JoinResult1.ta1.id,
      ),
      tc1.c)
03: FilterResult1 = Filter(
      JoinResult1,
      ta1.col2=10 and ta1.col2 > any(SubqueryCScanResult),
    )
01: SelectResult1 = Select(FilterResult1, (1, ta1.col+1, tb1.col))
05: GroupResult1 = Aggregate(SelectResult1, count(‘1’))
06: HavingResult1 = Filter(GroupResult1, ‘1’ > 10)
07: SortResult1 = Sort(HavingResult1, (‘1’ desc))
08: Result1 = Limit(SortResult1, 0, 10)
// query 2
11: JoinResult2 = InnerJoin(Scan(ta2), Scan(tb2), ta2.id=tb2.id)
12: FilterResult2 = Filter(JoinResult2, ta2.col2=10)
10: SelectResult2 = Select(FilterResult2, (1, ta2.col+1, tb2.col))
13: Result2 = Aggregate(SelectResult2, count(‘1’))
// union
09: UnionResult = Aggregate(Merge(Result1, Result2), NULL)
14: OrderResult = Sort(UnionResult, (‘1’ desc))
15: Result = Limit(OrderResult, 0, 5)
```

Now, combine everything into one expression, and voila:

```
Result = Limit(
  Sort(
    Aggregate(
      Merge(
        Limit(
          Sort(
            Filter(
              Aggregate(
                Select(
                  Filter(
                    JoinResult1 = InnerJoin(
                      Scan(ta1),
                      Scan(tb1),
                      ta1.id=tb1.id,
                    ),
                    ta1.col=10 and ta1.col > any(
                      Select(
                        Filter(
                          Scan(tc1),
                          tc1.id=JoinResult1.ta1.id,
                        ),
                        tc1.c,
                      ),
                    ),
                  ),
                  (1, ta1.col+1, tb1.col),
                ),
                count(‘1’), // ‘1’ is column 1
              ),
              ‘1’ > 10,
            ),
            (‘1’ desc),
          ),
          0, 10,
        ),
        Aggregate(
          Filter(
            InnerJoin(
              Scan(ta2),
              Scan(tb2),
              ta2.id=tb2.id,
            ),
            ta2.col=10,
          ),
          count(‘1’),
        ),
      ),
      NULL,
    ),
    (‘1’ desc),
  ),
  0, 5,
)
```

There’s a lot of nesting, but the expressions are mostly straightforward by the fact that each result feeds into the other. The only exception is the correlated subquery that uses a value from JoinResult1 from the outer query to perform its operation. You can also see that the lowest level operations are all Scans.

If the primitives defined at the beginning were all implemented, if we had an infinitely fast CPU, or an infinite amount of time, and we also had an infinite amount of memory, the above ‘plan’ will produce the correct result.

However, SQL engines are all about having limited CPU, memory and disk. So, they have to figure out ways to rearrange those operations differently so that the minimum amount of time and resources is used to compute the result. The next section will cover those techniques.

# Approaches and tradeoffs

## Standard approaches

If you analyze the various operators, the expensive ones are:

* Scan: you want to avoid full table scans, unless it’s a necessity. This is ironically the lowest level operation.
* Sort: this is a potentially expensive operation if the result set is large.
* Aggregation: is also potentially expensive

All other operations are considered ‘cheap’ because they can be applied as the rows come. To optimize for the above three operations, databases combine the primitives. The most popular one is the index scan, which allows you to combine Scan, Filter and Sort, all as one operation. Aggregation can also benefit from an index scan if the GROUP BY columns are part of an index. If an index cannot be used, the database resorts to the more traditional approach of full table scans, file sorts, etc.

## Preserving the original representation

In the case of VTGate, the ‘Route’ operation is capable of performing all 9 functions, as long as all the rows needed to fulfil the query reside in a single database. However, those functions have to be expressed as an SQL query. Most often, the original query could just be passed-through to a single shard. So, the one important question is: If we converted a query to its relational representation, can we then convert it back to the original query? The answer is no, or at least not trivially; The relational operators don’t always map one-to-one with the constructs of an SQL statement. Here’s a specific example:

`select count(*), a+1, b from t group by a+1, b`

This gets expressed as:

```
Aggregate(
  Select(
    Scan(t),
    (1, a+1, b),
  ),
  Count(‘1’),  // ‘1’ is column 1
)
```

So, it may not be a good idea to immediately convert the parsed query into the relational representation. When we analyze the query, we should try to retain the original text and see if they could be recombined without loss of readability.

The AST produced by the parser can be reversed back into a representation that’s accurate and close to the original query. This is what VTTablet already does. So, we can use the AST as the intermediate representation that can be retained until we can figure out what can be delegated to the underlying databases.

Once we have identified the parts that can be pushed down, what is left can be converted to use the operations that VTGate can perform.

## Cleverness

Relational databases initially optimized their queries based on a standard set of rules that used the schema info as the only additional input. These were known as rule-based optimizers. However, some queries would fail for certain values due to pathological corner cases.

In order to address such corner cases, databases decided to venture into cost-based optimizers. This was somewhat of a slippery slope. On the one hand, the feature was very convenient, because one could throw any kind of ad-hoc query to a database, and it would figure out the best way to get the results. However, every once in awhile, the database would make the wrong optimization decision. This led to iterations to accommodate the new corner cases. Unfortunately, there is no end to this, and databases are still iterating. Beyond a certain point, this becomes a losing battle because the cost of computing the best plan starts to exceed the cost of executing the query using a less optimal one.

The cost-based optimizers have become so complex that a novice programmer can’t reliably predict the execution plan for a query. It’s also hard to know why an engine chose a certain plan.

An escape hatch was provided: query hints. If the database failed to find the most optimal plan, the application can specify hints in the query. Once hints are provided, the optimizer is bypassed, and the application dictates the plan for the query. The fundamental question is: Who should know the data? This question opens a lot of arguments. The main argument in favor of why the app should be responsible for the query plans is as follows: even if the database can decide the most optimal plan, the app has to know the cost of a query. It needs to know this because it makes a decision on whether it’s going to send 1QPS or 1MQPS of that query. If the plan needs to change, it means that the cost is also changing, which means that the app needs to be revisited. This is why an optimizer that is too clever has diminishing returns, and is sometimes counter-productive.

The rise of NoSQL databases is further evidence that engineers are generally not very sold on how clever the database optimizers are.

One concern with the application dictating the plan is that we may constantly worry that it might not be the best one. To alleviate this, we can develop analyzers, possibly backed by a machine-learning backend, that can suggest rewrites. But it will be up to the application to accept them.

So, we’re going to attempt this brain-dead approach for optimizations in VTGate: By looking at a query, and knowing the vschema, one should be able to predict what VTGate will do with it. If, for some reason, the plan doesn’t work out, then the query must be rewritten to represent the alternate plan. There is a possibility that this approach may fail; Some database users have grown accustomed to the optimizer doing the work for them. If it happens, this decision can be revisited.

### What does this all mean

This just means that the VTGate optimizer will be very simple and transparent. For example, joins will always be performed left-to-right. When you look at a query, you’ll know the join order by just looking at it. VTGate will not try to rewrite subqueries into joins. If a join plan is more efficient than a subquery, then the app must write it that way.

Of course, if there are optimizations that make common sense, those will be implemented. For example, if a subquery is not correlated, then it can be pulled out and executed only once.

We should also be sensitive to situations where the natural expression of a query does not match the best plan.

No matter what, once a query is outsourced to a database, it’s subject to the optimizer decisions of the database. So, the app may still have to give hints for such situations.

## Opcodes vs tree

The execution plan can be represented either using opcodes or as a tree. The opcode approach represents the plan as a list of actions to be performed, whereas a tree represents the same plan as one action that depends on other actions, which recursively ends up with leaf nodes that perform the routing.

Opcodes can be very flexible (turing complete). However, they require the introduction of intermediate variables. Let’s look at this example:

`a - (b+c)`

The opcode approach would express the expression as:

```
temp = b+c
result = a - temp
```

The tree approach would represent the action as:

`Minus(a, Plus(b, c))`

In the theory section, we classified SQL as a pushdown automaton, which means that a tree structure is sufficient to represent a statement.

Using opcodes may still help us come up with better optimizations. However, given that we don’t plan on going overboard on optimizations, the more readable tree representation is preferable.

## Prioritization

Since we have powerful databases underneath us, our first priority is to harness anything they can do first. The next priority will be to do things that VTGate can perform on-the-fly, without accumulating results. And finally, we’ll look at features that require complex memory management or intermediate file storage. The features will be implemented as follows:

1. Single shard or single keyspace joins.
2. Scatter joins that can be merged in any order.
3. Cross-shard joins that don’t require post-processing.
4. Sort, Aggregate and Limit, provided the results can be pre-sorted by the underlying database(s).
5. Filter and Select expression engines.
6. All other constructs.

One hurdle to overcome with #4 is collation. Substantial work may have to be done to make the VTGate collation behavior match MySQL.

## Streaming vs non-streaming

VTGate has two query APIs:

* Non-streaming API: This API has deadlines and row count limits. It collects all the results first and then returns them.
* Streaming API: This API has no deadlines or row count limits. It streams the results as they come.

This difference will eventually cause a divergence in what VTGate can do for streaming vs non-streaming queries. Since the entire result is available in-memory for non-streaming queries, we’ll be able to perform sorts and aggregations after obtaining the results. However, this will not be possible for streaming queries.

For the short term, we’ll maintain parity by only implementing operations that both APIs can perform. This actually means priorities 1 through 5. So, it may be a long time before the two diverge.

## Column names

The vschema currently doesn’t know all the column names of the tables. VTGate cannot resolve joins if it doesn’t have this info. We have two options:

1. Extend vschema to also contain the column names of tables.
2. Require join queries to always qualify column names by the table.

The first option is going to be a maintenance problem. It introduces a dependency that is hard to maintain. For now, we’ll go with option #2. It is a syntactic inconvenience, but let’s hope that users find this acceptable. We could allow exceptions for vindex columns, but that’s likely to be confusing. So, it’s better to require this for all columns. For subqueries (that are themselves not joins), we can make an exception and assume that unqualified column names are addressing the inner table. You’ll need to use qualified names only for addressing columns of an outer query.

## Redundant constructs

SQL has many redundant constructs. There’s diminishing return in supporting all of them. So, the following constructs will not be supported:

* `‘,’` join: This operator was the old way of joining tables. The newer SQL recommends using actual JOIN keywords. Allowing both forms of JOIN will be a big source of confusion because these operators also have different preferences.
* RIGHT JOIN: This is same as a reversed LEFT JOIN, except it’s less readable.
* NATURAL joins: These are rarely used in practice because the join condition applies to all columns.
* JOIN USING: This is just syntactic sugar for JOIN ON a.id=b.id
* WHERE clause join condition: A JOIN without an ON clause is supposed to be a cross product of the two tables. However, old-style ‘,’ users have continued to specify join conditions in the WHERE clause. The initial version of VTGate will only use the ON clause to decide how to route parts of a query. This decision can be revisited if things are too inflexible.

## Limiting results

VTGate currently doesn’t have any limits on how many rows it can hold at any given time. This is dangerous because a scatter query that hits a large number of shards could easily blow up the memory.

Also, as we add support for joins, we’ll have a new way of accumulating a large number of rows.

So, the VTTablet level protection to limit the maximum number of rows fetched may not be sufficient any more.

We’ll add a new maxrows variable to VTGate. If the number of rows accumulated exceeds this count, we’ll return an error.

Note that this limit only applies to non-streaming queries. Streaming queries will continue to work as before.

# Design

Recapitulating what we’ve covered so far:

* The primary function of the optimizer is to push down as much of the query components as possible.
* We’ll not support redundant constructs for joins.
* If a query has to be split across multiple shards or keyspaces, we’ll expect the application to dictate the order of operations. This will be left to right, unless parentheses define a different order. 
* For joins, all column names must be qualified by their table.
* We’ll preserve the original representation of the query to the extent possible.
* We’ll not attempt to flatten subqueries. However, they can still get pushed down with the outer query if they’re constrained by the same keyspace id.

## Symbol table and scoping rules

Once we start allowing joins and subqueries, we have a whole bunch of table aliases and relationships to deal with. We have to contend with name clashes, self-joins, as well as scoping rules. In a way, the vschema has acted as a static symbol table so far. But that’s not going to be enough any more.

The core of the symbol table will contain a map whose key will be a table alias, and the elements will be [similar to the table in vschema](https://github.com/youtube/vitess/blob/master/go/vt/vtgate/planbuilder/schema.go#L22). However, it will also contain a column list that will be built as the query is parsed.

### A simple example

Let’s start with the following vschema that has two tables t1 & t2. Here’s a simplified view:

<table>
  <tr>
    <td>tables</td>
    <td>columns</td>
    <td>vindexes</td>
  </tr>
  <tr>
    <td>t1</td>
    <td>id</td>
    <td>id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td>t2</td>
    <td>t2id</td>
    <td>t2id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>b</td>
    <td>b_idx: lookup_unique</td>
  </tr>
</table>


For the following query:

`select a, b from t1 where id = :id and c = 5`

The starting symbol table will be identical to the vschema. However, as the query is analyzed, b and c will get added to t1 as additional columns:

<table>
  <tr>
    <td>tables</td>
    <td>columns</td>
    <td>vindexes</td>
  </tr>
  <tr>
    <td>t1</td>
    <td>id</td>
    <td>id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td></td>
    <td>b</td>
    <td></td>
  </tr>
  <tr>
    <td></td>
    <td>c</td>
    <td></td>
  </tr>
</table>


If a symbol table contains only one database table, then all unqualified column references are assumed to implicitly reference columns of that table.

*Ideally, we should be validating column references against a known column list for each table. However, as mentioned before, the maintenance of that list comes with its own problems. Instead, we trust that a non-ambiguous column reference implies that the table (or result) does have such a column. In the worst case where such a column doesn’t exist, the query will fail. There are no circumstances where this assumption can lead to incorrect results.*

The bind vars section of the symbol table will contain an entry for id.

### Self-join

For the following query (using the previous vschema):

`select t1.a, t2.c from t1 join t1 as t2 where t2.a = t1.id`

The symbol table will look like this:

<table>
  <tr>
    <td>tables</td>
    <td>columns</td>
    <td>vindexes</td>
  </tr>
  <tr>
    <td>t1</td>
    <td>id</td>
    <td>id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td>t2</td>
    <td>id</td>
    <td>id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td></td>
    <td>c</td>
    <td></td>
  </tr>
</table>


In the symbol table, t2 starts off as identical to t1, and is different from t2 that’s in the original vschema. But t2 eventually adds c to its list of columns. There is currently no value in remembering that t1 and t2 refer to the same underlying table. Maybe a future use case will arise that will introduce this need.

### Subqueries

MySQL allows table names (or aliases) in subqueries to hide an outer name. Due to this, we have to extend the symbol table to support scoping. This is done by changing it into a linked list, where each node represents a scope. A symbol is searched in the list order until it’s found.

Let’s look at this intentionally confusing example:

```
select t1.a, t2.b from t1 join t2 where t2.t2id in (
  select b from t1 as t2 where id = t1.c
)
```

The outerscope symbol table will look like this:

<table>
  <tr>
    <td>t1</td>
    <td>id</td>
    <td>id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td></td>
    <td>c</td>
    <td></td>
  </tr>
  <tr>
    <td>t2</td>
    <td>t2id</td>
    <td>t2id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>b</td>
    <td>b_idx: lookup_unique</td>
  </tr>
</table>


The innerscope will look like this:

<table>
  <tr>
    <td>t2</td>
    <td>id</td>
    <td>id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td></td>
    <td>b</td>
    <td></td>
  </tr>
</table>


Column c for t1 was added because the inner query made a qualified reference to it. The inner scope table gets a column b added because the innerscope query made an implicit reference, and there was only one item in that scope.

Also, the inner query cannot access any columns of the outer t2 because it has its own t2 symbol that hides the outer one.

A corollary: since subquery nesting can have many levels, an inner query can still reference results from any of the outer scopes. For example, this is a valid query:

```
select t1.a, t1.b
from t1
where t1.id in (
  select t2.id
  from t2
  where t2.id in (
    select t3.id
    from t3
    where t3.id = t1.a
    )
  )
```

### The FROM clause

The FROM clause is an exception. This clause creates a new symbol table (scope), with no initial outerscope, even if it was part of a subquery. At the end of the analysis, the newly created symbol table is added to the outerscope, if there was one.

This means that subqueries in the FROM clause are not allowed to refer to elements of any outer query.

The result of such a subquery will be a new virtual table with its own vindex columns, etc. This is facilitated by the fact that the SQL syntax requires you to name the subquery with an alias. Figuring out possible vindexes for the new virtual table is useful because routing rules could be applied for other tables that may join with the subquery. Here’s an example:

```
select sq.a, t2.b
from
  (select a from t1 where id = :id) as sq
  join t2 on t2.b = sq.a
```

The main symbol table will look like this:

<table>
  <tr>
    <td>tables</td>
    <td>columns</td>
    <td>vindexes</td>
  </tr>
  <tr>
    <td>sq</td>
    <td>a</td>
    <td>a_idx: lookup_unique</td>
  </tr>
  <tr>
    <td>t2</td>
    <td>t2id</td>
    <td>t2id_idx: hash</td>
  </tr>
  <tr>
    <td></td>
    <td>b</td>
    <td>b_idx: lookup_unique</td>
  </tr>
</table>


The symbol table for the `‘from t1’` subquery will be in its own scope. Its analysis will yield a new virtual table sq, which will be the one added to the main symbol table. The analyzer should hopefully be smart enough to deduce that column ‘a’ of the subquery can be used for routing because it’s a faithful representation of the column in the underlying table t1.

*Conceptually speaking, there’s no strong reason to prevent the FROM clause from referencing columns of an outer query. MySQL probably disallows this because it leads to confusing scoping rules. Let’s take a look at this example:*

```
select id from a where id in (
  select b.id, a.id
  from
    (select id from b where b.id=a.id) b
    join d as a on a.id=b.id)
```

*If we allowed an outer scope, then b.id=a.id becomes ambiguous. We don’t know if it should refer to the outer ‘a’, or the one that is yet to be created as part of the current scope. Needless to say, one shouldn’t be writing such unreadable queries.*

### ON clauses

By the fact that the ON clause is part of a FROM clause, you cannot refer to an element of an outer query. This is an invalid query:

`select id from a where id in (select b.id from b join c on b.id=a.id)`

But this is valid:

`select id from a where id in (select b.id from b join c where b.id=a.id)`

Furthermore, you can only refer to elements that participate in the current JOIN. But the following query is still valid:

`select * from a join b on b.id = a.id join c on c.id = a.id`

Because of associativity rules, c is actually joined with ‘a join b’.

But this query is not valid:

`select * from a join (b join c on c.id = a.id)`

The ON clause participants are only b and c because of the parenthesis. So, you’re not allowed to reference a.id in that join. In other words, analysis of the ON clause requires a list of participants. The analyzer will need to check with this list before looking inside the symbol table, or we could exploit Go interfaces here by sending a subset symbol table that has the same interface as the original one.

ON clauses are allowed to have correlated subqueries, but only with tables that are the ON clause participants. This statement is valid:

`select * from a join b on b.id in (select c.id from c where c.id=a.id)`

### Naming result columns

Result columns get referenced in the following places:

* For addressing the results of a subquery in the FROM clause
* GROUP BY, HAVING and ORDER BY clauses

The WHERE clause is not allowed to reference them because it happens before the SELECT expressions are evaluated.

Column names are usually built from the select expression itself. Therefore, ‘a+1’ is a valid column name. If such a column is in a FROM subquery, it can be addressed using back-quotes as ``a+1``. However, you can reference such expressions directly in GROUP BY, HAVING and ORDER BY clauses. You can also reference a result by using its column number (starting with 1).

Conceptually, this is similar to the subquery column naming. GROUP BY, etc. are post-processing operations that are applied to the result of a query.

*MySQL allows duplicate column names. Using such column names for joins and subqueries leads to unpredictable results. Because of this, we could go `either way`:*

* *Allow duplicates like MySQL and remain equally ambiguous*
* *Disallow duplicates*

Here’s an example of how MySQL names columns:

```
> select id, (select val    from a), 3, 3, 3 as `4` from a;
+------+------------------------+---+---+---+
| id   | (select val    from a) | 3 | 3 | 4 |
+------+------------------------+---+---+---+
|    1 | a                      | 3 | 3 | 3 |
+------+------------------------+---+---+---+
MariaDB [a]> select `(select val    from a)` from (select id, (select val    from a), 3, 3, 3 as `4` from a) as t;
+------------------------+
| (select val    from a) |
+------------------------+
| a                      |
+------------------------+
1 row in set (0.00 sec)
MariaDB [a]> select `(select val from a)` from (select id, (select val    from a), 3, 3, 3 as `4` from a) as t;
ERROR 1054 (42S22): Unknown column '(select val from a)' in 'field list'
```

The select expression is produced verbatim as column name, and duplicates are allowed.

These quirks don’t matter much if VTGate just has to return these results back to the client without generating its own values. However, the result column names need to be known for analyzing dependencies when we split queries. So, VTGate will do a fake mimic of what MySQL does: Take a guess at the column name by rebuilding a string representation of the AST for that select expression. If there’s a lucky match, well and good. If not, the query analysis will fail, and the user will have to provide a non-ambiguous alias for such columns. In practice, this is not a real problem because users also generally dislike using verbatim expressions as column names when performing complex joins.

So, once the SELECT expression is analyzed, the symbol table (for that scope) will acquire an entry for the result column names. Additionally, if a result column is a faithful reference to an underlying table column, it will contain this additional annotation, which can be used for optimizing other operations.

### Bind variables

When VTGate has to perform a join, it needs to use the results of one query to feed into the other. For example, let’s look at this query:

`select t1.a, t2.b from t1 join t2 on t2.id = t1.id`

If t1 and t2 are on different keyspaces, VTGate has to perform the following operations:

`select t1.a from t1`

For each row of this result, it then has to perform:

`select t2.b from t2 where t2.id = :_t1_a`

The mechanism for this happens by generating a bind variable named ‘_t1_a1’ from the first result. The bind variable name can be generated using a functional algorithm:

`‘_’ + tableAlias + ‘_’ + columnName`

The scoping rule works in our favor here because this type of name generation correctly handles naming conflicts.

*MySQL allows multiple columns in a result to have the same name, and allows using of such ambiguous names in joins, etc. Fortunately, this also works in our favor because we’ll be no worse if we also allowed it.*

For the sake of simplicity, we’ll not allow dependencies to use non-standard column names. For example, we’ll fail a query like this:

`select * from (select a, count(*) from t1) t where t.`count(*)` > 0`

This should instead be rewritten as:

`select * from (select a, count(*) c from t1) t where t.c > 0`

This way, we can build a bind variable like `‘:_t_c’` for the outer query if we have split the above query into two.

There is a subtle difference between column names produced by a result vs. the column names in the symbol table. The WHERE clause constructs reference columns in the symbol table. It’s only the post-processing constructs like GROUP BY, etc. that reference column names produced by the result. This could pose a problem when a SELECT gets pushed down that defines a new alias that has the same column name as an underlying table. Will such a construct hide, and therefore, corrupt the meaning of a join? Let’s look at an example:

`select a.col1 as col2, b.col1 from a join b on b.col2=a.col2`

Rewritten, this would become

```
select a.col1 as col2, a.col2 from a
Join (by producing _a_col2 from column 2)
select b.col1 from b where b.col2 = :_a_col2
```

Because we reference column numbers from the first query, we bypass naming ambiguities. However, we still need to know the source column for each expression so we don’t have to unnecessarily fetch identical results. In other words, the following query:

`select a.col2 as col2, b.col1 from a join b on b.col2=a.col2`

should become:

```
select a.col2 as col2 from a
Join (by producing _a_col2 from column 1)
select b.col1 from b where b.col2 = :_a_col2
```

This means that select expressions need to know if they’re a faithful reference of an underlying table column, and this information will be used for wiring up dependencies.

The other situation where result names interact with the symbol table is during a subquery in the FROM clause. Fortunately, we create a separate virtual table entry for it in the symbol table for this situation. This allows us to keep the two worlds from colliding.

## Pushdown

Pushdown is achieved by analyzing the query in the execution order and seeing if the next stage can be pushed down into the database. For VTGate, the execution order is slightly different from the standard one: Route replaces Scan, and also, the Route primitive can perform a scatter.

In terms of relational primitives, a scatter is a bunch of `Route` operations followed by a `Merge`. We’ll call it  `RouteMerge`. So, what you can push down into a simple `Route` may not be allowed for a `RouteMerge`. Here’s the VTGate-specific order of operations:

`Route/RouteMerge <-> (Join & LeftJoin) -> Filter(where) -> Select -> Aggregate -> Filter(having) -> Sort -> Limit -> Merge`

The rest of the analysis tries to push the subsequent operations into Route or RouteMerge. Whatever cannot be pushed down is the work that VTGate has to do by itself. The fact that we won’t try to flatten subqueries simplifies our analysis a little bit.

*MySQL allows you to Sort and Limit the result of a UNION. This is something that the Vitess grammar doesn’t allow right now. We’ll eventually need to support this construct, which will add `Aggregate -> Sort -> Limit` as things that can further happen after the last `Merge`.*

The overall strategy is as follows:

1. Identify groups: Form groups of tables (or subqueries) in the FROM clause that can stay together for Route operations.
2. Pushdown higher-level operations into the various groups: This step uses the AST as input and produces an execution tree where the leaf nodes are all Route operations.
3. Wire up dependencies: This step computes the dependencies between different routes, and potentially modifies the queries in the routes so that additional values are returned that may be needed by other routes.
4. Produce the execution plan.

### Starting primitives

In order to align ourselves with our priorities, we’ll start off with a limited set of primitives, and then we can expand from there.

VTGate already has `Route` and `RouteMerge` as primitives. To this list, let’s add `Join` and `LeftJoin`. Using these primitives, we should be able to cover priorities 1-3 (mentioned in the [Prioritization](https://github.com/youtube/vitess/blob/sugudoc/doc/V3HighLevelDesign.md#prioritization) section). So, any constructs that will require VTGate to do additional work will not be supported. Here’s a recap of what each primitive must do:

* `Route`: Sends a query to a single shard or unsharded keyspace.
* `RouteMerge`: Sends a (mostly) identical query to multiple shards and returns the combined results in no particular order.
* `Join`: Executes the LHS operation, which could be any primitive. For each row returned, it builds additional bind vars using the result, and uses them to execute the RHS. For each row of the RHS, it builds a new row that combines both the results.
* `LeftJoin`: Same as Join, except that a row is returned even if the RHS fails to return a row.

### FROM clause

The first step of the pushdown algorithm is to identify tables that can stay together. Requiring the ON clause to specify the join condition helps simplify this analysis. If this rule is followed in the query, we can determine this grouping by just analyzing the FROM clause.

The premise is as follows: A join can be pushed down if all the rows needed to satisfy a query are within the same database. If all the tables are in an unsharded keyspace, then this constraint is automatically true. If the tables are in a sharded keyspace, then the join condition must guarantee this. This is best explained with an example. Let’s start with a sharded keyspace with user and extra as tables, and analyze this query:

`select user.name, extra.info from user join extra on extra.id=user.id`

The join algorithm will first scan user:

`select user.name, user.id from user`

Then for each user.id (:user_id), it will execute:

`select extra.info from extra where extra.id = :user_id`

If user and extra had the same vindex on id, and if it was unique, then we know that all the necessary rows to satisfy the second query belong to the same kesypace_id as the row that came from user. So, they will be in the same shard. This means that it’s safe to push down the join.

Note that this push-down will work for RouteMerge queries also.

Given that the unsharded keyspace is an easy special case, we’ll focus mainly on the analysis of sharded keyspaces. Any place where we mention ‘same shard’ or ‘same keyspace id’, such condition will equally apply to an unsharded keyspace.

*Rule: For a join to be groupable, the join condition must be an equality condition where the participant columns use the same vindex, and the vindex must be unique.*

Some examples of what kind of AST will be produced by joins:

```
a join b join c
    J
   / \
  J   c
 / \
a   b
A join (b join c)
  J
 / \
a   J
   / \
  b   c
a left join b on c1 left join c on c2
    L:c2
   / \
 L:c1 c
 / \
a   b
```

For left joins, the ON clause is mandatory and enforced by the grammar. Because of this, the placement of ON clauses can also dictate grouping, which is not the case for normal joins:

```
a left join b left join c on c1 on c2
  L:c2
 / \
a   L:c1
   / \
  b   c
```

By looking at the above tree, we can deduce that c1 can reference b and c, while c2 can reference a, b and c.

As mentioned before, when the required data is in multiple shards or keyspaces, VTGate will perform the join, and the order will be dictated by the SQL statement, as represented by the above execution trees. The join conditions themselves may try to dictate a different order. However, that will not be used to reorder the joins.

For example:

`from a join (b join c on c.id=b.id) on c.id=a.id`

is likely better executed this way:

`from a join c on c.id=a.id join b on b.id=c.id`

or this way:

`from b join c on c.id=b.id join a on a.id=c.id`

But VTGate will not perform such reordering tricks.

The work of the analyzer is to recursively walk the JOIN tree. The decisions are made from bottom to top.

The lowest level node of a parse tree is a ‘table’. This could be a real table or the result of a subquery that can be treated as a table. Knowing the table name, we can deduce the keyspace to which it belongs. So, the output of this analysis is the creation of the table entry in the symbol table, and the node being marked as a RouteMerge, because the rows for that table are scattered across all shards until additional constraints are applied.

When analyzing a join node, we first extract the ON clause conditions as a list, and we see if these requirements are satisfied:

* The node types have to be Route or RouteMerge
* They should be for the same keyspace (sharded)
* There should be at least one join condition between the two nodes: left.col = right.col
* The columns referenced in the join must use the same vindex
* The vindex must be unique
* Other conditions in the ON clause must also be executable under the new group. This basically means that if there are subqueries, they should be correlated in such a way that the required rows can all be found within the new group. The algorithm for this analysis is explained in the WHERE clause section.

Two nodes can also be grouped (without a join condition) if they are both constrained by the same keyspace id (Route nodes).

The above rules work for both JOIN and LEFT JOIN nodes.

###### Why does grouping work for LEFT JOIN?

*It’s easy to explain why grouping works for the simple case:*

`a left join b on b.id=a.id`

*In the above case, the only rows that are inspected in b are those where b.id=a.id. So, the join can be grouped. But what about this join:*

`(a left join b on a.id=b.id) join (c left join d on c.id=d.id) on b.id=d.id`

*In the above case, rows from b or c could be NULL. Fortunately, in SQL, NULL != NULL. So, the outer-scope join will succeed only if rows from b and c are not NULL. If they’re not NULL, they’re guaranteed to be from the same shard. So, this makes them groupable. In fact, in the above case, the left joins in the inner scope are unnecessary. They could have just been normal joins. However, things would be very different if one had used the null-safe equal operator (<=>) for joins. But we’ll not treat null-safe equal as a valid join operator.*

When two nodes are grouped, the current join condition becomes the root of the new group, and it gets a routing property:

* If it’s a JOIN, the new property is the more restrictive of the two nodes. So, if one of them is a Route, then the new node is also a Route.
* For a LEFT JOIN, the new property is the same as the LHS node.

If the grouping conditions are not met, then the node remains a join node. In this case, we have to see if the ON clause conditions can be pushed down into the left and/or right nodes. By the fact that the current join is split into two, the ON clause cannot be be pushed as is. Instead, we use associativity rules to our benefit and merge the ON clause conditions into the WHERE clauses of the underlying nodes. The rules are the same as the ones described for a normal WHERE clause.

But left joins are slightly different, because the join condition is applied *to the RHS only*. Also, the condition cannot be further pushed into other nested left joins, because they will change the meaning of the statement. For example:

`from a left join (b left join c on c.id=b.id) on c.id=a.id`

is same as:

`a LeftJoin (b LeftJoin c where c.id=b.id) where c.id=a.id)`

But it’s not the same as:

`a LeftJoin (b left join c where c.id=b.id and c.id=a.id)`

If the above joins were normal joins, then all three would be equivalent.

*The RHS-only rule for left joins has some non-intuitive repercussions. For example, this query will return all rows of table ‘a’:*

`select a.id, b.id from a left join b on (a.id=5 and b.id=a.id)`

*But b.id will be mostly null, except when a.id and b.id were 5.*

An ON clause can contain other constraints that are not joins. If so, they’re treated just like a WHERE clause constraint. So, a keyspace id constraint in an ON clause can change a RouteMerge to a Route. This means that we have to look at how to combine Route nodes with others.

A subquery in the FROM clause can result in any of the four primitive node. If it’s a join node, then the ON clause cannot be pushed down because it depends on the result of the join. If it was a Route or RouteMerge, the ON clause is pushable. Here’s how:

`from a join (select id, count(*) from b where b.id=1) t on t.id=a.id`

becomes

```
  J
 / \
a   (select id, count(*) from b where b,id=1) t where t.id=a.id
```

Here’s a tabular version of the above algorithm:

<table>
  <tr>
    <td>LHS</td>
    <td>Join type</td>
    <td>RHS</td>
    <td>Result</td>
  </tr>
  <tr>
    <td>Route</td>
    <td>Join</td>
    <td>Route</td>
    <td>Route</td>
  </tr>
  <tr>
    <td>Route</td>
    <td>Join</td>
    <td>RouteMerge</td>
    <td>Route</td>
  </tr>
  <tr>
    <td>RouteMerge</td>
    <td>Join</td>
    <td>Route</td>
    <td>Route (RHS)</td>
  </tr>
  <tr>
    <td>RouteMerge</td>
    <td>Join</td>
    <td>RouteMerge</td>
    <td>RouteMerge</td>
  </tr>
  <tr>
    <td>Route</td>
    <td>LeftJoin</td>
    <td>Route</td>
    <td>Route (LHS)</td>
  </tr>
  <tr>
    <td>Route</td>
    <td>LeftJoin</td>
    <td>RouteMerge</td>
    <td>Route (LHS)</td>
  </tr>
  <tr>
    <td>RouteMerge</td>
    <td>LeftJoin</td>
    <td>Route</td>
    <td>RouteMerge</td>
  </tr>
  <tr>
    <td>RouteMerge</td>
    <td>LeftJoin</td>
    <td>RouteMerge</td>
    <td>RouteMerge</td>
  </tr>
  <tr>
    <td>Join/LeftJoin</td>
    <td>All joins</td>
    <td>All primitives</td>
    <td>New join</td>
  </tr>
  <tr>
    <td>All primitives</td>
    <td>All joins</td>
    <td>Join/LeftJoin</td>
    <td>New join</td>
  </tr>
</table>


At the end of the analysis, the output is a symbol table and a reduced tree representing the grouped joins. After this phase, the groups are finalized. They will not be further split or merged.

Here’s an example:

`a join b on b.id=a.id join c on c.id=b.id2 join d on d.id=a.id`

will be represented as:

```
                        J2
                       /  \
                      J1   d where d.id=a.id
                     /  \    
a join b on b.id=a.id    c where c.id=b.id2
```

### WHERE clause

The ON clause analysis is similar to the WHERE clause, which is why it wasn’t covered in detail in the previous section. The main difference between WHERE and ON is that the ON clause can cause groups to merge. If two groups cannot be merged, the ON clauses get converted to WHERE clauses and get pushed down using the same rules. The only exception is the forced RHS push in the case of left joins.

*The symbol table used by the ON clause is a subset of the one for the WHERE clause. So, it’s safe to move on ON clause condition into a WHERE clause. Attempting to move a WHERE clause condition to an ON clause may require more care.*

The first step here is to convert the parse tree into a list of conditions that can be put back together using AND clauses. Then we analyze each condition separately and decide if it can be pushed into a Route or RouteMerge.

We look at the columns that each condition references, and we identify the rightmost group that the expression references and we push the where clause there. Any references outside of that group will later be changed to bind vars. By the fact that the where clause was pushed to the right most group gives us the guarantee that values will be available for the rest of the column references.

Each time a WHERE clause gets successfully pushed into a Route or RouteMerge, we re-evaluate the routing plan to see if it can be improved. The rules for this are described in the vindex document.

There are situations where we cannot push down:

###### LEFT JOIN

A WHERE clause cannot be pushed inside a LEFT JOIN, because it has to be applied after 

the ON clause is processed, which is not until `LeftJoin` returns the results.

*There are some practical use cases where you’d want to add conditions in the WHERE clause, typically for NULL checks. To support this in the future, we can use this technique that converts a LEFT JOIN into a normal JOIN:*

`from a left join b on b.id=a.id where b.id is NULL`

*After split, the query on b should have become:*

`from b where b.id=a.id`

*We cannot push down the null check in the above case. But we can, if we rewrote it as:*

`from (select 1) as t left join b on b.id=a.id where b.id is NULL`

*The (select 1) acts as a fake driver row for the left join, which allows us to tack on the where clause and also change the `LeftJoin` to a `Join`.*

###### Subqueries

To be able to push a subquery, we have to determine that the subquery does not escape out of the shard of the parent query. For this, we identify the target group using the same method as above. Then we check to see if the subquery can get a Route plan where the input is the same keyspace id as the group’s query. If this is true, then the where clause can be pushed down just like a normal condition.

If this condition is not met, the subquery becomes a separate group. This complexity is beyond what can be expressed using the four primitives we have. So, we’ll address such constructs when we add more primitives to the mix.

When recombining WHERE clauses, additional parenthesis will be needed to prevent lower precedence operators from changing the intended order.

###### An example

`from a join b on b.id=a.id join c on c.id=b.id where cond1(a.col, b.col) and cond2(a.col, c.col)`

If a, b and c where in different groups, the output would be:

```
    J
   / \
  J   c where (c.id=b.id) and (cond2(a.col, c.col))
 / \
a   b where (b.id=a.id) and (cond1(a.col, b.col))
```

The cond2 expression gets pushed into the the where clause for table ‘c’ because it’s the right-most group that’s referenced by the condition. External references will be changed to appropriate bind variables by the rewiring phase.

*Once VTGate acquires the ability to perform its own filters, should we stop pushing these conditions into the dependent queries and do it ourselves instead? The answer will usually be no. You almost always want to push down filters. This is because it will let the underlying database scan fewer rows, or choose better indexes. The more restrictive the query is, the better.*

At the end of the analysis, if no WHERE clauses remain, we can go to the next step. If not, the query is too complex, and we error out.

### SELECT clause, non-aggregate case

The SELECT clause can represent two relational primitives: a Select or Aggregate (or both). In the case of an Aggregate, it works in conjunction with the GROUP BY clause. So, it will be simpler if the analysis branched off depending on whether the SELECT clause has aggregates or not.

Before starting to process select, we have to perform the pre-step of naming the results. During this process, we generate a name for each item in the SELECT clause. Additionally, if a column is a faithful reference to an underlying table column, we add this qualification. This information will be used for future analysis.

The important property of a non-aggregate SELECT clause is that the column expressions don’t depend on each other. Therefore, they can be split and pushed down past joins into different groups.

If an expression references columns from a single group, then it can be pushed down into that group.

If an expression references columns from more than one group, then the expression cannot be pushed down, we fail the query.

*It is possible to push-down combo expressions using the same technique we use for WHERE clauses. For example:*

`select a.col+b.col from a join b where b.foo=a.bar`

*can be resolved with the following join*

```
select a.col, a.bar from a // produce _a_col & _a_bar
Join
select :_a_col+b.col from b where b.foo=:_a_bar
```

*However, such queries are not very efficient for VTTablet. It can’t cache field info. Such push-downs don’t benefit MySQL either. So, it’s better to wait till VTGate implements the ability to evaluate expressions.*

SELECT can also contain subqueries. The same restriction as WHERE clauses can be applied here: If the keyspace id of the subquery is not the same as the outer query’s, we fail the query. Otherwise, we push it down.

###### ‘*’ expressions

Since VTGate does not know all the columns of a table, a ‘SELECT *’ may not be resolvable. We can consider disallowing it altogether. However, here are situations where we could allow it:

* If the underlying primitive is a Route or RouteMerge, we could just push it down without knowing what it means.
* If the underlying ‘table’ is a subquery, we know the full column list. In such cases, we know how to expand the ‘*’ into an actual list.

###### Additional expressions for joins

Note: This phase could be postponed till the time we do the wire-up.

After the SELECT expressions are pushed down, we have to see if there are dependencies across joins that are still not addressed. If there are WHERE clause dependencies from one group to another that are not listed in the SELECT expression, we need to add those to the list of expressions. This is where we use the info for each select column to see if it already references a column we need for the RHS query. Here’s an example of the metadata in a Join primitive:

`select a.col1, b.col1, a.col3 from a join b on b.col2=a.col2`

will be represented as

```
Join:
  LHS: select a.col1, a.col3, a.col2 from a
  Bindvars: "_a_col2": 2 // Col 2 produces bind var _a_col2
  RHS: select b.col1 from b where b.col2=:_a_col2
  Select: [ LHS(0), RHS(0), LHS(1) ]
```

Consequently, this means that the lower level routes fetch columns that should not be returned in the result. This is the reason why a Join primitive has to also specify the columns that have to be returned in the final result.

*It’s safe to assume that no further changes will be made to the SELECT list, and consider it finalized. Subsequent clauses will only be able to operate on the outcome of the result. This fact is not material right now. But it may become significant when VTGate starts to perform other operations. Specifically, VTGate may split the SELECT clause into a Select and Aggregate and push the Select part down. If the select list changes due to other external dependencies, then the Aggregate cannot be pushed down.*

*MySQL allows you to address columns that are not in the SELECT list in GROUP BY and HAVING clauses, which can further affect the Select list. This is not standard SQL.*

###### Handling empty lists

The converse of the above rule is the case where the result references only columns from one side of a join. In such cases, the other side of the join doesn’t have a SELECT expression yet. For such situations, we create a dummy ‘select 1’ to make the query work. Example:

`select a.col from a join b on b.col=a.col`

will be represented as:

```
Join:
  LHS: select a.col from a
  Bindvars: "_a_col": 0
  RHS: select 1 from b where b.col=:_a_col
  Select: [LHS(0)]
```

###### Pushing past Filter

If/when VTGate implements filters, we may have to ask if a SELECT clause can be pushed past such filters into a Route primitive. The answer is yes, because Select and Filter are orthogonal operations. However, we may prefer performing the Select at the VTGate level instead of pushing it down.

### SELECT with aggregates

If a SELECT clause uses aggregate functions, then it can be pushed down if the underlying node is a Route. As usual, subquery escape analysis will still need to be applied.

If the underlying node is a RouteMerge, then it can still be pushed down if one of the non-aggregate result columns has a unique vindex. Such a constraint guarantees that all the rows of each group are within one shard.

You generally can’t push an Aggregate past a join. Theoretically, there are situations where you can do this, but such opportunities are rare. So, it’s not worth considering these optimizations.

### GROUP BY

The act of pushing an Aggregate down also necessitates that the associated GROUP BY clause be pushed. It’s just the other side of the same coin.

The GROUP BY clause is slightly unusual because its contents can actually be automatically inferred in most cases. More often than not, the SQL engine has to validate that the GROUP BY columns match the inferred values.

It is possible that a GROUP BY to be present while a SELECT doesn’t have aggregates. It’s a common technique that people use to dedupe rows in the result. In such cases, the same rules as an Aggregate apply to the GROUP BY clause.

*As mentioned previously, MySQL analyzes the GROUP BY clause using the query’s scope. It also does not enforce that the GROUP BY columns are consistent with the SELECT. For example, this is valid in MySQL:*

`select a.col1, a.col2 from a group by a.col1`

*This also valid:*

`select a.col1 from a group by a.col1, a.col2`

*Both of the above constructs are invalid by SQL standards. We could piggy-back on this lax attitude and just pass-through the GROUP BY as is. After all, these are not very meaningful constructs. If MySQL allows them, why should we work against them?*

### HAVING

We reach this phase only if SELECT and GROUP BY were successfully pushed.

The difference between HAVING and WHERE is that the HAVING clause applies filters after the result is computed. So, it can only reference columns from the result.

For each condition in the HAVING clause, we find out which result columns it references. If all those columns originate from the same group, then the clause is pushable into that group.

For conditions that span multiple groups, we can apply the bind variable dependency technique just like we did for the WHERE clause. If we decide to do this, we’ll need a separate bind variable naming scheme, because these refer to result columns and not table columns.

As usual, subquery processing will need to be done as needed.

Note: HAVING clauses are usually very simple. So, there may be diminishing return in allowing complexities here. However, if we’re just reusing code, we can allow these. Otherwise, we shouldn’t go overboard.

### ORDER BY

Even though this clause near the end of the execution chain, it’s a popular one. It’s also an expensive one. So, we have to make every effort to push the work down into the underlying routes.

We’ll start with the usual restriction that this clause will only reference the result columns.

If the underlying node is a Route, then the clause can just be pushed down.

If the underlying node is a RouteMerge, then it cannot be pushed down.

If the underlying node is a join, then we perform the following analysis: Take each expression and identify the group it originates from. If it references more than one group, then we cannot push that expression down. If the group order is no less than the current group order, the expression can be pushed. We continue this until we reach an expression that is not pushable, or we successfully push everything. Here are two examples:

`select a.col, b.col, c.col from a join b join c order by a.col, b.col, c.col`

This will produce:

```
select a.col from a order by a.col // group 1
select b.col from b order by b.col // group 2
select c.col from c order by c.col // group 3
```

Example 2:

`select a.col, b.col, c.col from a join b join c order by a.col, c.col, b.col`

For the above case, a.col is from group 1, c.col is from group 3, but b.col is from group 2, which is less than group 3. So, the last expression is not pushable.

### LIMIT

The LIMIT clause has two forms:

LIMIT m, n: Fetch m+n rows, but return rows m through n.

LIMIT n: same as LIMIT 0, n

If the underlying node is a Route, then the LIMIT can be pushed down.

If the underlying node is a RouteMerge, then we fail the query. This is because ORDER BY cannot be pushed into a RouteMerge, and a LIMIT without an ORDER BY is not very meaningful. Also, the order of rows is unpredictable for RouteMerge. So, it’s even less valuable to apply a limit.

In the case of a join, the query could be passed down without a limit and we can do the filtering on the entire result. Maybe this is not worth supporting.

## Wire-up

If we’ve reached this phase, it means that we have successfully broken up the original query and pushed the different parts into route primitives that are now wired together by joins.

However, these parts would still be referring to each other. This section will describe how those dependencies will get resolved.

In order to resolve dependencies correctly, we need the symbol tables that were used for analyzing each context. With the symbol table at hand, we walk the AST for each group, and look up the referenced symbols in the symbol table. If the symbol belongs to the same group, then nothing needs to be done. If the reference is outside, we perform the following steps:

1. Change the external reference to the corresponding bind var name.
2. If that column is not already in the select list of the other group, we add it.
3. Mark the column number to be exported as the bind var name that we chose.

This step has a caveat: if the SELECT list contained an Aggregate, then we cannot add such a column. However, we’re currently protected by the constraint that Aggregate cannot be pushed into a join, and external dependencies can only happen for joins. But this is something to keep in mind as we add more primitives and flexibilities.

### Accessing the symbol table

Currently, the AST contains only information parsed from the query. When we analyze the query, we build various symbol tables. However, they would go away as we finish each analysis.

Since we need them again for resolving dependencies, we have to store them somewhere. We also have to retrieve the right one for each subquery. This gives rise to a dependency issue: Subqueries can be anywhere in the parse tree. So, it’s unnatural to have an outside data structure that points to various parts of the AST.

So, we make a compromise: we extend the Select structure and add a Symbols (interface{}) member to it. Every time we finish analyzing a subquery, we set the Symbols member to point to the symbol table. With this compromise, the rest of the code base will flow naturally while resolving dependencies.

## Plan generation

Now, all we have to do is generate the execution plan. During this step, we just generate the code for the AST of each route node, and perform additional substitutions needed to perform the correct routing, as dictated by the vindexes chosen for each route.

A Route node will become one of: SelectUnsharded, SelectEqual, SelectKeyrange

A RouteMerge will become one of: SelectScatter, SelectIN

# Future primitives

Looking at how far we can go with just four primitives is pretty amazing. But we can’t stop there. There are a few more low-hanging fruits we can get to, and then we can also start looking at the high-hanging ones.

## MergeSort and MergeAggregate

If the ORDER BY clause can be pushed down for a Route, then we’re not far away from supporting it for a RouteMerge also. The results from each shard are already coming sorted. Merge-sorting such results becomes trivial.

Along the same lines, if we ordered the rows of an Aggregate by the columns it’s grouping, we can push down such a query through a RouteMerge, and perform the final aggregation as the results come.

The one challenge is collation. We’ll need to make sure that our collation algorithm can match what MySQL does.

## Subquery

Uncorrelated subqueries can be trivially ‘pulled out’ to be executed separately, and their results can be pushed down into the query that needs the result.

Here’s an example:

`select * from a where col in (select col from lookaside)`

This can be expressed as:

```
Subquery:
  _lookaside: select col from lookaside
  query: select * from a where col in ::_lookaside
```

## Select & Filter

If we built an expression engine, then we can do our own evaluation and filtering when something is not pushable. Adding support for this will automatically include correlated subqueries, because the result of the underlying query can be used to execute the subquery during the Select or Filter stage. Example:

`select a.id from a where a.id = (select id from b where b.col=a.col)`

Plan:

```
Filter:
  query: select a.id, a.col from a
  bindvars: "_a_col": 1
  constraint: a.id = ConvertVal: Route:
    select id from b where b.col=:_a_col
```

## Other expensive primitives

The rest of the primitives have potentially unbounded memory consequences. For these, we’ll have to explore map-reduce based approaches.

