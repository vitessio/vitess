package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

func buildUnionPlan(union *sqlparser.Union, vschema VSchema) (primitive engine.Primitive, err error) {
	bindvars := sqlparser.GetBindvars(union)
	bldr, err := processUnion(union, vschema, nil)
	if err != nil {
		return nil, err
	}
	jt := newJointab(bindvars)
	err = bldr.Wireup(bldr, jt)
	if err != nil {
		return nil, err
	}
	return bldr.Primitive(), nil
}

func processUnion(union *sqlparser.Union, vschema VSchema, outer builder) (builder, error) {
	var err error
	var lbldr, rbldr builder
	switch lhs := union.Left.(type) {
	case *sqlparser.Union:
		lbldr, err = processUnion(lhs, vschema, outer)
	case *sqlparser.Select:
		lbldr, err = processSelect(lhs, vschema, outer)
	default:
		panic("unreachable")
	}
	if err != nil {
		return nil, err
	}
	switch rhs := union.Right.(type) {
	case *sqlparser.Union:
		rbldr, err = processUnion(rhs, vschema, outer)
	case *sqlparser.Select:
		rbldr, err = processSelect(rhs, vschema, outer)
	default:
		panic("unreachable")
	}
	if err != nil {
		return nil, err
	}
	bldr, err := unionRouteMerge(union, lbldr, rbldr, vschema)
	if err == nil {
		return bldr, nil
	}
	if union.Type != sqlparser.UnionAllStr {
		return nil, errors.New("unsupported: multi-shard union without ALL")
	}
	//TODO(acharis): need to check that lhs and rhs have same number of columns
	return newUnionBuilder(lbldr, rbldr)
}

func unionRouteMerge(union *sqlparser.Union, left, right builder, vschema VSchema) (builder, error) {
	lroute, ok := left.(*route)
	if !ok {
		return nil, errors.New("can't merge")
	}
	rroute, ok := right.(*route)
	if !ok {
		return nil, errors.New("can't merge")
	}
	if err := subqueryCanMerge(lroute, rroute); err != nil {
		return nil, err
	}
	table := &vindexes.Table{
		Keyspace: lroute.ERoute.Keyspace,
	}
	// TODO: If we can identify common colsyms from both sides
	// as being same, we can make this union inherit those properties
	// like we do in the from clause handling. Code is commented out
	// for now:
	/*
		for _, colsyms := range lroute.Colsyms {
			if colsyms.Vindex == nil {
				continue
			}
			// Check if a colvindex of the same name already exists.
			// Dups are not allowed in subqueries in this situation.
			for _, colVindex := range table.ColumnVindexes {
				if colVindex.Column.Equal(colsyms.Alias) {
					return nil, fmt.Errorf("duplicate column aliases: %v", colsyms.Alias)
				}
			}
			table.ColumnVindexes = append(table.ColumnVindexes, &vindexes.ColumnVindex{
				Column: colsyms.Alias,
				Vindex: colsyms.Vindex,
			})
		}
	*/
	rtb := newRoute(
		union,
		lroute.ERoute,
		table,
		vschema,
		&sqlparser.TableName{Name: sqlparser.NewTableIdent("")}, // Unions don't have an addressable table name.
		sqlparser.NewTableIdent(""),
	)
	lroute.Redirect = rtb
	rroute.Redirect = rtb
	return rtb, nil
}
