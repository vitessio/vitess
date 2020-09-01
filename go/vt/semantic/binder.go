/*
Copyright 2020 The Vitess Authors.

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

package semantic

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// table is an internal struct used while binding
	table struct {
		name, qualifier, alias string
	}

	// Table is the information known about which table this column belongs to
	Table struct {
		Name, Qualifier string
		Local           bool
	}
)

func (t table) hasAlias() bool {
	return t.alias != ""
}

//DoBinding annotates the provided scope with table info, or writes table info to the ColNames
func DoBinding(s *scope, node sqlparser.SQLNode) error {
	switch n := node.(type) {
	case *sqlparser.AliasedTableExpr:
		switch t := n.Expr.(type) {
		case sqlparser.TableName:
			s.tableExprs = append(s.tableExprs, &table{
				name:      t.Name.String(),
				qualifier: t.Qualifier.String(),
				alias:     n.As.String(),
			})
		}
	case *sqlparser.ColName:
		table, local := s.FindTable(n.Qualifier.Qualifier.String(), n.Qualifier.Name.String())
		if table == nil {
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "ERROR 1054 (42S22): Unknown column '%s' in 'field list'", sqlparser.String(n))
		}
		n.Metadata = Table{
			Name:      table.name,
			Qualifier: table.qualifier,
			Local:     local,
		}
	}
	return nil
}
