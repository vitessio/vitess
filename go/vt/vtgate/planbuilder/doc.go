// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package planbuilder allows you to build execution
plans that describe how to fulfill a query that may
span multiple keyspaces or shards.

The main entry point for the planbuilder is the
BuildPlan function that accepts a query and vschema
and returns the plan.

This package also provides various convenience functions
to build the VSchema object, which it can later utilize
to build query plans.

Additionally, this package defines the various Vindex
related interfaces. These interfaces need to be satisfied
by plugin code that wants to define a Vindex type.
*/
package planbuilder

/*
The planbuilder for the SELECT statement has the highest
complexity. Currently, VTGate can only perform the following
two primitives: Route and Join.

The Route primitive executes a query and returns the result.
This can be either to a single keyspace or shard, or it can
be a scatter query that spans multiple shards. In the case
of a scatter, the rows can be returned in any order.

The Join primitive can perform a normal or a left join.
If there is a join condition, it's actually executed
as a constraint on the second (RHS) query. The Join
primitive specifies the join variables (bind vars)
that need to be built from the results of the first
(LHS) query. For example:

	select ... from a join b on b.col = a.col

will be executed as:

	select ..., a.col from a (produce "a_col" from a.col)
	select ... from b where b.col = :a_col

The act of breaking up a join into two such statements
like the above example is called a vertical break.
As opposed to this, a horizontal break is one
that breaks the individual clauses of a statement.
For example, separating out a where clause from
the select expressions would be a horizontal break.
Breaking out a subquery would also be considered
a horizontal break. The current set of primitives
only allow for vertical breaks.

The planbuilder tries to push all the constructs of
the original request into these two primitives. If
successful, we return the built plan. Otherwise, it's
an error.

The central design element for analyzing queries and
building plans is the symbol table (symtab). This data
structure contains tableAlias and colsym elements.
A tableAlias element represents a table alias defined
in the FROM clause. A tableAlias must always point
to a routeBuilder, which is responsible for building
the SELECT statement for that alias.
A colsym represents a result column. It can optionally
point to a tableAlias if it's a plain column reference
of that alias. A colsym must always point to a routeBuilder.
One symtab is created per SELECT statement. tableAlias
names must be unique within each symtab. Currently,
duplicates are allowed among colsyms, just like MySQL
does. Different databases implement different rules
about whether colsym symbols can hide the tableAlias
symbols. The rules used by MySQL are not well documented.
Therefore, we use the conservative rule that no
tableAlias can be seen if colsyms are present.

The symbol table is modified as various sections of the
query are parsed. The parsing of the FROM clause
populates the table aliases. These are then used
by the WHERE and SELECT clauses. The SELECT clause
produces the colsyms, which are added to the
symtab after the analysis is done. Consequently,
the GROUP BY, HAVING and ORDER BY clauses can only
see the colsyms. They're not allowed to reference
the table aliases.

The planbuilder supports subqueries. This gives rise
to multiple symbol tables. As per SQL rules, symbols
in the inner query can hide those in the outer query.
However, the life-cycle of the symbol table also needs to
be taken into account. For example, a subquery that's
in a WHERE clause cannot see the SELECT symbols (colsyms)
of the outer query, because those have not been created
yet. But a subquery in the HAVING clause will be able
to see them.

The plan builder builds the plan in two phases. In the
first phase (break-up and push-down), the query is
broken into smaller parts and pushed down into
Join or Route primitives. In the second phase (generator),
external references are wired up using bind vars, and
the individual ASTs are converted into actual queries.

In the case of joins, the plan builder effectively
performs a vertical break of the query. This gives rise
to a possible conflict in the symbol tables. Specifically,
a WHERE clause was only able to see tableAlias symbols
during analysis. However, once the query is partitioned
vertically, the query actually becomes a SELECT statement feeding
into another. This means that symbols that were previously
not visible during analysis are now visible. It's very important
to remember the original resolution. Otherwise, there is
risk that incorrent values are used when the RHS of a join
requests values from the LHS. In order to achieve this,
the sqlparser.ColName type has been ammended with a Metadata
field that is populated as soon as a symbol is resolved.
During the generator phase, we do not perform any more symtab
lookups, but rely on the previously stored Metadata instead.

In the case of a vertical break, we split a select into
two select statements. If this happens, the symbol table
is not split, because we still need the ability to find
all symbols being referenced. Instead, each tableAlias
points to the Route where the split queries are being
built. When we need to know if a symbol is local or
not, we compare the route of the table alias against
the current route. In the case of a subquery, it initially
starts off with its own route. After the analysis is done,
if we decide to merge it with an outer route, we repoint
all the symbols of the subquery to the outer route.

The VSchema currently doesn't contain the full list of columns
in the tables. For the sake of convenience, if a query
references only one table, then we implicitly assume that
all column references are against that table. However,
in the case of a join, the query must qualify every
column reference (table.col). Otherwise, we can't know which
tables they're referring to. This same rule applies to subqueries.
There is one possibility that's worth mentioning, because
it may become relevant in the future: A subquery in the
HAVING clause may refer to an unqualified symbol. If that
subquery references only one table, then that symbol is
automatically presumed to be part of that table. However,
in reality, it may actually be a reference to a select
expression of the outer query. Fortunately, such use cases
are virtually non-exisitent. So, we don't have to worry
about it right now.

Due to the absence of the column list, we assume that
any qualified or implicit column reference of a table
is valid. In terms of data structure, the Metadata
only points to the table alias. Therefore, a unique
column reference is a pointer to a table alias and a
column name. This is the basis for the definition of the
colref type. If the colref points to a colsym, then
the column name is irrelevant.
*/
