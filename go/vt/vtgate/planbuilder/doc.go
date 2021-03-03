/*
Copyright 2019 The Vitess Authors.

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
span multiple keyspaces or shards. The main entry
points for this package are Build and BuildFromStmt.
*/
package planbuilder

/*
The main strategy of the planbuilder is to push down as
much of the work as possible down to the vttablets. The
special primitive for doing this is route, which can
execute any SQL on a single shard (or scatter). Any
work that cannot be done by a single route is stitched
together by VTGate using relational primitives. If
stitching is not possible using existing primitives,
then an "unsupported" error is returned.

If a query is split into multiple parts, like a
cross-shard join, the latter parts may carry references
to the former parts. If this happens, the primitive
specifies how to build these cross-shard references as
"join variables" that will essentially be sent in
as bind vars during execution. For example:

	select ... from a join b on b.col = a.col

will be executed as:

	select ... a.col from a (produce "a_col" from a.col)
	select ... from b where b.col = :a_col

The central design element for analyzing queries and
building plans is the symbol table (symtab). This data
structure evolves as a query is analyzed. Therefore,
searches are not repeatable. To resolve this, search
results are persisted inside the ColName as 'Metadata',
and reused as needed.

The plan is built in two phases. In the
first phase (break-up and push-down), the query is
broken into smaller parts and pushed down into
various primitives. In the second phase (wire-up),
external references are wired up using bind vars, and
the individual ASTs are converted into actual queries.

In current architecture, VTGate does not know the
underlying MySQL schema. Due to this, we assume that
any qualified or implicit column reference of a table
is valid and we rely on the underlying vttablet/MySQL
to eventually validate such references.

Every 'logicalPlan' primitive must satisfy the logicalPlan
interface. This allows the planbuilder to outsource
primitive-specific handling into those implementations.

Variable naming: The AST, planbuilder and engine
are three different worlds that use overloaded
names that are contextually similar, but different.
For example a join is:
	Join is the AST node that represents the SQL construct
	join is a logicalPlan in the current package
	Join is a primitive in the engine package
In order to disambiguate, we'll use the 'a' prefix
for AST vars, and the 'e' prefix for engine vars.
So, 'ajoin' would be of type *sqlparser.Join, and
'ejoin' would be of type *engine.Join. For the planbuilder
join we'll use 'jb'.
*/
