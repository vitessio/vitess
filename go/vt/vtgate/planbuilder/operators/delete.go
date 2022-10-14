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

type Delete struct {
	Table     *QueryTable
	TableInfo semantics.TableInfo
	AST       *sqlparser.Delete
}

var _ LogicalOperator = (*Delete)(nil)

// Introduces implements the tableIDIntroducer interface
func (d *Delete) Introduces() semantics.TableSet {
	return d.Table.ID
}

// CheckValid implements the LogicalOperator interface
func (d *Delete) CheckValid() error {
	return nil
}

// iLogical implements the LogicalOperator interface
func (d *Delete) iLogical() {}
