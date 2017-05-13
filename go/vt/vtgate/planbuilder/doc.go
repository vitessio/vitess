/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
structure evolves as a query is analyzed. Therefore,
searches are not repeatable. To resolve this, search
results are persisted inside the ColName as 'Metadata',
and reused as needed.

The plan is built in two phases. In the
first phase (break-up and push-down), the query is
broken into smaller parts and pushed down into
Join or Route primitives. In the second phase (wire-up),
external references are wired up using bind vars, and
the individual ASTs are converted into actual queries.

In current architecture, VTGate does not know the
underlying MySQL schema. Due to this, we assume that
any qualified or implicit column reference of a table
is valid and we rely on the underlying vttablet/MySQL
to eventually validate such references.

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
