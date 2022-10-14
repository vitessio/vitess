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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Update struct {
	Table       *QueryTable
	TableInfo   semantics.TableInfo
	Assignments map[string]sqlparser.Expr
	AST         *sqlparser.Update
}

var _ LogicalOperator = (*Update)(nil)

// TableID implements the LogicalOperator interface
func (u *Update) TableID() semantics.TableSet {
	return u.Table.ID
}

// UnsolvedPredicates implements the LogicalOperator interface
func (u *Update) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the LogicalOperator interface
func (u *Update) CheckValid() error {
	return nil
}

// iLogical implements the LogicalOperator interface
func (u *Update) iLogical() {}

// Compact implements the LogicalOperator interface
func (u *Update) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	return u, nil
}
