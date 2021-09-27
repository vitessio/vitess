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

package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	concatenateTree struct {
		distinct    bool
		ordering    sqlparser.OrderBy
		limit       *sqlparser.Limit
		selectStmts []*sqlparser.Select
		sources     []queryTree
	}
)

var _ queryTree = (*concatenateTree)(nil)

func (c *concatenateTree) tableID() semantics.TableSet {
	var tableSet semantics.TableSet
	for _, source := range c.sources {
		tableSet |= source.tableID()
	}
	return tableSet
}

func (c *concatenateTree) clone() queryTree {
	var sourcesCopy []queryTree
	for _, source := range c.sources {
		sourcesCopy = append(sourcesCopy, source.clone())
	}
	return &concatenateTree{sources: sourcesCopy, selectStmts: c.selectStmts}
}

func (c *concatenateTree) cost() int {
	var totalCost int
	for _, source := range c.sources {
		totalCost += source.cost()
	}
	return totalCost
}

func (c *concatenateTree) pushOutputColumns(columns []*sqlparser.ColName, semTable *semantics.SemTable) ([]int, error) {
	return nil, vterrors.New(vtrpc.Code_INTERNAL, "pushOutputColumns should not be called on this struct")
}
