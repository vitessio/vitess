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
	"math/rand"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// maruts is a query generator that generates random queries to be used in fuzzing
	maruts struct {
		cfg config
	}

	config struct {
		r            *rand.Rand
		genConfig    sqlparser.ExprGeneratorConfig
		maxTables    int
		maxAggrs     int
		maxGBs       int
		schemaTables []tableT
	}
)

func (m *maruts) randomQuery() sqlparser.SelectStatement {
	exprGen := sqlparser.NewGenerator(m.cfg.r, 2)
	expr := exprGen.Expression(m.cfg.genConfig)

	ae := &sqlparser.AliasedExpr{Expr: expr}
	tableExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.NewTableName("dual")}
	return &sqlparser.Select{
		SelectExprs: sqlparser.SelectExprs{ae},
		From:        []sqlparser.TableExpr{tableExpr},
	}
}

var _ queryGen = (*maruts)(nil)
