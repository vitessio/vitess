/*
Copyright 2021 The Vitess Authors.

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

// Derived represents a derived table in the query
type Derived struct {
	Sel           sqlparser.SelectStatement
	Inner         LogicalOperator
	Alias         string
	ColumnAliases sqlparser.Columns
}

var _ LogicalOperator = (*Derived)(nil)

func (*Derived) iLogical() {}

// TableID implements the Operator interface
func (d *Derived) TableID() semantics.TableSet {
	return d.Inner.TableID()
}

// UnsolvedPredicates implements the Operator interface
func (d *Derived) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return d.Inner.UnsolvedPredicates(semTable)
}

// CheckValid implements the Operator interface
func (d *Derived) CheckValid() error {
	return d.Inner.CheckValid()
}
