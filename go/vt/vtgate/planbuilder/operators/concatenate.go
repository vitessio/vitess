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
)

// Concatenate represents a UNION ALL/DISTINCT.
type Concatenate struct {
	Distinct    bool
	SelectStmts []*sqlparser.Select
	Sources     []Operator
	OrderBy     sqlparser.OrderBy
	Limit       *sqlparser.Limit
}

var _ Operator = (*Concatenate)(nil)

func (c *Concatenate) Clone(inputs []Operator) Operator {
	clone := *c
	clone.Sources = inputs
	return &clone
}

func (c *Concatenate) Inputs() []Operator {
	return c.Sources
}
