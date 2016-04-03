// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package planbuilder allows you to build execution
plans that describe how to fulfill a query that may
span multiple keyspaces or shards.

The main entry point for the planbuilder is the
Build function that accepts a query and vschema
and returns the plan.
*/
package planbuilder

/*
The two core primitives built by this package are Route and Join.

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

The planbuilder tries to push all the constructs of
the original request into a Route. If it's not possible,
we see if we can build a primitive for it. If none exist,
we return an error.

The central design element for analyzing queries and
building plans is the symbol table (symtab). This data
structure contains tabsym and colsym elements.
A tabsym element represents a table alias defined
in the FROM clause. A tabsym must always point
to a route, which is responsible for building
the SELECT statement for that alias.
A colsym represents a result column. It can optionally
point to a tabsym if it's a plain column reference
of that alias. A colsym must always point to a route.
One symtab is created per SELECT statement. tabsym
names must be unique within each symtab. Currently,
duplicates are allowed among colsyms, just like MySQL
does. Different databases implement different rules
about whether colsym symbols can hide the tabsym
symbols. The rules used by MySQL are not well documented.
Therefore, we use the conservative rule that no
tabsym can be seen if colsyms are present.

The symbol table is modified as various sections of the
query are parsed. The parsing of the FROM clause
populates the table aliases. These are then used
by the WHERE and SELECT clauses. The SELECT clause
produces the colsyms, which are added to the
symtab after the analysis is done. Consequently,
the GROUP BY, HAVING and ORDER BY clauses can only
see the colsyms. They're not allowed to reference
the table aliases. These access rules transitively
apply to subqueries in those clauses, which is the
intended behavior.

The plan is built in two phases. In the
first phase (break-up and push-down), the query is
broken into smaller parts and pushed down into
Join or Route primitives. In the second phase (wire-up),
external references are wired up using bind vars, and
the individual ASTs are converted into actual queries.

Since the symbol table evolved during the first phase,
the wire-up phase cannot reuse it. In order to address
this, we save a pointer to the resolved symbol inside
every column reference, which we can reuse during
the wire-up. The Route for each symbol is used to
determine if a reference is local or not. Anything
external is converted to a bind var.

A route can be merged with another one. If this happens,
we should ideally repoint all symbols of that route to the
new one. This gets complicated because each route would
need to know the symbols that point to it.
Instead, we mark the current route as defunct
by redirecting to point to the new one. During symbol resolution
we chase this pointer until we reach a non-defunct route.

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

Variable naming: The AST, planbuilder and engine
are three different worlds that use overloaded
names that are contextually similar, but different.
For example a join is:
	Join is the AST node that represents the SQL construct
	join is a builder in the current package
	Join is a primitive in the engine package
In order to disambiguate, we'll use the 'a' prefix
for AST vars, and the 'e' prefix for engine vars.
So, 'ajoin' would be of type *sqlparser.Join, and
'ejoin' would be of type *engine.Join. For the planbuilder
join we'll use 'jb'.
*/
