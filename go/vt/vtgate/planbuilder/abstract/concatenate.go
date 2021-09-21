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

package abstract

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Concatenate represents a UNION ALL.
type Concatenate struct {
	SelectStmts []*sqlparser.Select
	Sources     []Operator
}

var _ Operator = (*Concatenate)(nil)

// TableID implements the Operator interface
func (c *Concatenate) TableID() semantics.TableSet {
	var tableSet semantics.TableSet
	for _, source := range c.Sources {
		tableSet |= source.TableID()
	}
	return tableSet
}

// PushPredicate implements the Operator interface
func (c *Concatenate) PushPredicate(sqlparser.Expr, *semantics.SemTable) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
}

// UnsolvedPredicates implements the Operator interface
func (c *Concatenate) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the Operator interface
func (c *Concatenate) CheckValid() error {
	for _, source := range c.Sources {
		err := source.CheckValid()
		if err != nil {
			return err
		}
	}
	return nil
}
