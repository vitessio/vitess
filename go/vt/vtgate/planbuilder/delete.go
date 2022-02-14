package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// buildDeletePlan builds the instructions for a DELETE statement.
func buildDeletePlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	del := stmt.(*sqlparser.Delete)
	if del.With != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in delete statement")
	}
	var err error
	if len(del.TableExprs) == 1 && len(del.Targets) == 1 {
		del, err = rewriteSingleTbl(del)
		if err != nil {
			return nil, err
		}
	}
	dml, ksidVindex, err := buildDMLPlan(vschema, "delete", del, reservedVars, del.TableExprs, del.Where, del.OrderBy, del.Limit, del.Comments, del.Targets)
	if err != nil {
		return nil, err
	}
	edel := &engine.Delete{DML: dml}

	if dml.Opcode == engine.Unsharded {
		return edel, nil
	}

	if len(del.Targets) > 1 {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "multi-table delete statement in not supported in sharded database")
	}

	if len(del.Targets) == 1 && del.Targets[0].Name != edel.Table.Name {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.UnknownTable, "Unknown table '%s' in MULTI DELETE", del.Targets[0].Name.String())
	}

	if len(edel.Table.Owned) > 0 {
		aTblExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: delete on complex table expression")
		}
		tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: edel.Table.Name}, As: aTblExpr.As}
		edel.OwnedVindexQuery = generateDMLSubquery(tblExpr, del.Where, del.OrderBy, del.Limit, edel.Table, ksidVindex.Columns)
		edel.KsidVindex = ksidVindex.Vindex
		edel.KsidLength = len(ksidVindex.Columns)
	}

	return edel, nil
}

func rewriteSingleTbl(del *sqlparser.Delete) (*sqlparser.Delete, error) {
	atExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return del, nil
	}
	if !atExpr.As.IsEmpty() && !sqlparser.EqualsTableIdent(del.Targets[0].Name, atExpr.As) {
		//Unknown table in MULTI DELETE
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.UnknownTable, "Unknown table '%s' in MULTI DELETE", del.Targets[0].Name.String())
	}

	tbl, ok := atExpr.Expr.(sqlparser.TableName)
	if !ok {
		// derived table
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.NonUpdateableTable, "The target table %s of the DELETE is not updatable", atExpr.As.String())
	}
	if atExpr.As.IsEmpty() && !sqlparser.EqualsTableIdent(del.Targets[0].Name, tbl.Name) {
		//Unknown table in MULTI DELETE
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.UnknownTable, "Unknown table '%s' in MULTI DELETE", del.Targets[0].Name.String())
	}

	del.TableExprs = sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: tbl}}
	del.Targets = nil
	if del.Where != nil {
		_ = sqlparser.Rewrite(del.Where, func(cursor *sqlparser.Cursor) bool {
			switch node := cursor.Node().(type) {
			case *sqlparser.ColName:
				if !node.Qualifier.IsEmpty() {
					node.Qualifier = tbl
				}
			}
			return true
		}, nil)
	}
	return del, nil
}
