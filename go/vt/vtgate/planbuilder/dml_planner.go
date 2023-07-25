/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func rewriteRoutedTables(stmt sqlparser.Statement, vschema plancontext.VSchema) error {
	// Rewrite routed tables
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		aliasTbl, isAlias := node.(*sqlparser.AliasedTableExpr)
		if !isAlias {
			return true, nil
		}
		tableName, ok := aliasTbl.Expr.(sqlparser.TableName)
		if !ok {
			return true, nil
		}
		vschemaTable, vindexTbl, _, _, _, err := vschema.FindTableOrVindex(tableName)
		if err != nil {
			return false, err
		}
		if vindexTbl != nil {
			// vindex cannot be present in a dml statement.
			return false, vterrors.VT09014()
		}

		if vschemaTable.Name.String() != tableName.Name.String() {
			name := tableName.Name
			if aliasTbl.As.IsEmpty() {
				// if the user hasn't specified an alias, we'll insert one here so the old table name still works
				aliasTbl.As = sqlparser.NewIdentifierCS(name.String())
			}
			tableName.Name = sqlparser.NewIdentifierCS(vschemaTable.Name.String())
			aliasTbl.Expr = tableName
		}

		return true, nil
	}, stmt)
}

func setLockOnAllSelect(plan logicalPlan) {
	_, _ = visit(plan, func(plan logicalPlan) (bool, logicalPlan, error) {
		switch node := plan.(type) {
		case *route:
			node.Select.SetLock(sqlparser.ShareModeLock)
			return true, node, nil
		}
		return true, plan, nil
	})
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}
