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
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// SupplyProjection pushes the given aliased expression into the fields and cols slices of the
// vindexFunc engine primitive. The method returns the offset of the new expression in the columns
// list.
func SupplyProjection(eVindexFunc *engine.VindexFunc, expr *sqlparser.AliasedExpr, reuse bool) error {
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	if !isColName {
		return vterrors.VT12001("expression on results of a vindex function")
	}

	enum := vindexColumnToIndex(colName)
	if enum == -1 {
		return vterrors.VT03016(colName.Name.String())
	}

	if reuse {
		for _, col := range eVindexFunc.Cols {
			if col == enum {
				return nil
			}
		}
	}

	eVindexFunc.Fields = append(eVindexFunc.Fields, &querypb.Field{
		Name:    expr.ColumnName(),
		Type:    querypb.Type_VARBINARY,
		Charset: collations.CollationBinaryID,
		Flags:   uint32(querypb.MySqlFlag_BINARY_FLAG),
	})
	eVindexFunc.Cols = append(eVindexFunc.Cols, enum)
	return nil
}

// UnsupportedSupplyWeightString represents the error where the supplying a weight string is not supported
type UnsupportedSupplyWeightString struct {
	Type string
}

// Error function implements the error interface
func (err UnsupportedSupplyWeightString) Error() string {
	return fmt.Sprintf("cannot do collation on %s", err.Type)
}

func vindexColumnToIndex(column *sqlparser.ColName) int {
	switch column.Name.String() {
	case "id":
		return 0
	case "keyspace_id":
		return 1
	case "range_start":
		return 2
	case "range_end":
		return 3
	case "hex_keyspace_id":
		return 4
	case "shard":
		return 5
	default:
		return -1
	}
}
