/*
Copyright 2022 The Vitess Authors.

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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

/*
-- correlated projection subquery. connecting predicate: u.id:s.id
SELECT id, (select max(sale) from sales where u.id = s.id) from user

-- uncorrelated projection subquery: no connecting predicate
SELECT id, (select max(sale) from sales) from user

-- correlated predicate subquery. connecting predicate: user.foo = sales.foo AND user_extra.bar = sales.bar
correlated with two tables
SELECT id
FROM user
	JOIN user_extra on user.id = user_extra.user_id
WHERE user.foo = (
	SELECT foo
	FROM sales
	WHERE user_extra.bar = sales.bar
)

-- correlated predicate subquery. connecting predicate: user.foo = sales.foo AND user_extra.bar = sales.bar
correlated with two tables
SELECT id
FROM user
	JOIN user_extra on user.id = user_extra.user_id
WHERE EXISTS(
	SELECT 1
	FROM sales
	WHERE user_extra.bar = sales.bar AND user.foo = sales.foo
)

-- correlated predicate subquery. connecting predicate: user.foo = sales.foo AND user_extra.bar = sales.bar
correlated with two tables
SELECT id
FROM user
	JOIN user_extra on user.id = user_extra.user_id
WHERE EXISTS(
	SELECT 1
	FROM sales
	WHERE user_extra.bar = sales.bar
	UNION
	SELECT 1
	FROM sales
	WHERE user.foo = sales.foo
)

-- correlated predicate subquery: connecting predicate: user_extra.bar = sales.bar
correlated only with user_extra
SELECT id
FROM user
	JOIN user_extra on user.id = user_extra.user_id
WHERE user.foo = (
	SELECT MAX(foo)
	FROM sales
	WHERE user_extra.bar = sales.bar
)

-- correlated predicate subquery: connecting predicate: user_extra.bar = sales.bar
correlated only with user_extra
SELECT id
FROM user
	JOIN user_extra on user.id = user_extra.user_id
WHERE EXISTS(SELECT 1
	FROM sales
	WHERE user_extra.bar = sales.bar
	HAVING MAX(user.foo) = sales.foo
)

-- uncorrelated predicate subquery: no connecting predicate
SELECT id
FROM user
WHERE user.foo = (
	SELECT MAX(foo)
	FROM sales
)


*/

func isMergeable(ctx *plancontext.PlanningContext, query sqlparser.SelectStatement, op ops.Operator) bool {
	validVindex := func(expr sqlparser.Expr) bool {
		sc := findColumnVindex(ctx, op, expr)
		return sc != nil && sc.IsUnique()
	}

	if query.GetLimit() != nil {
		return false
	}

	sel, ok := query.(*sqlparser.Select)
	if !ok {
		return false
	}

	if len(sel.GroupBy) > 0 {
		// iff we are grouping, we need to check that we can perform the grouping inside a single shard, and we check that
		// by checking that one of the grouping expressions used is a unique single column vindex.
		// TODO: we could also support the case where all the columns of a multi-column vindex are used in the grouping
		for _, gb := range sel.GroupBy {
			if validVindex(gb) {
				return true
			}
		}
		return false
	}

	// if we have grouping, we have already checked that it's safe, and don't need to check for aggregations
	// but if we don't have groupings, we need to check if there are aggregations that will mess with us
	if sqlparser.ContainsAggregation(sel.SelectExprs) {
		return false
	}

	if sqlparser.ContainsAggregation(sel.Having) {
		return false
	}

	return true
}
