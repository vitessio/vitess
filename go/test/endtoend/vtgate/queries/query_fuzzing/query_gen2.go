/*
Copyright 2023 The Vitess Authors.

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

package query_fuzzing

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

// maruts is a query generator that generates random queries to be used in fuzzing
type maruts struct {
}

func (m *maruts) randomQuery() sqlparser.SelectStatement {
	expr := &sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral("1")}
	tableExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.NewTableName("dual")}
	return &sqlparser.Select{
		SelectExprs: sqlparser.SelectExprs{expr},
		From:        []sqlparser.TableExpr{tableExpr},
	}
}

var _ queryGen = (*maruts)(nil)
