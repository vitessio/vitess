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
	Distinct    bool
	SelectStmts []*sqlparser.Select
	Sources     []Operator
	OrderBy     sqlparser.OrderBy
	Limit       *sqlparser.Limit
}

var _ Operator = (*Concatenate)(nil)

// TableID implements the Operator interface
func (c *Concatenate) TableID() semantics.TableSet {
	var tableSet semantics.TableSet
	for _, source := range c.Sources {
		tableSet.MergeInPlace(source.TableID())
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

// Compact implements the Operator interface
func (c *Concatenate) Compact(*semantics.SemTable) (Operator, error) {
	var newSources []Operator
	var newSels []*sqlparser.Select
	for i, source := range c.Sources {
		other, isConcat := source.(*Concatenate)
		if !isConcat {
			newSources = append(newSources, source)
			newSels = append(newSels, c.SelectStmts[i])
			continue
		}
		switch {
		case other.Limit == nil && len(other.OrderBy) == 0 && !other.Distinct:
			fallthrough
		case c.Distinct && other.Limit == nil:
			// if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
			newSources = append(newSources, other.Sources...)
			newSels = append(newSels, other.SelectStmts...)

		default:
			newSources = append(newSources, other)
			newSels = append(newSels, nil)
		}
	}
	c.Sources = newSources
	c.SelectStmts = newSels
	return c, nil
}
