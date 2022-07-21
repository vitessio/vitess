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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// costDML is used to compare the cost of vindexOptionDML
	costDML struct {
		vindexCost int
		isUnique   bool
		opCode     engine.Opcode
	}

	// vindexPlusPredicatesDML is a struct used to store all the predicates that the vindex can be used to query
	vindexPlusPredicatesDML struct {
		colVindex *vindexes.ColumnVindex

		// during planning, we store the alternatives found for this DML in this slice
		options []*vindexOptionDML
	}

	// vindexOptionDML stores the information needed to know if we have all the information needed to use a vindex
	vindexOptionDML struct {
		ready  bool
		values []evalengine.Expr
		// columns that we have seen so far. Used only for multi-column vindexes so that we can track how many columns part of the vindex we have seen
		colsSeen    map[string]any
		opcode      engine.Opcode
		foundVindex vindexes.Vindex
		cost        costDML
	}
)

// getDMLRouting returns the vindex and values for the DML,
// If it cannot find a unique vindex match, it returns an error.
func getDMLRouting(where *sqlparser.Where, table *vindexes.Table) (
	engine.Opcode,
	*vindexes.ColumnVindex,
	vindexes.Vindex,
	[]evalengine.Expr,
	error,
) {
	// Check that we have a primary vindex which is valid
	if len(table.ColumnVindexes) == 0 || !table.ColumnVindexes[0].IsUnique() {
		return engine.Scatter, nil, nil, nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.RequiresPrimaryKey, vterrors.PrimaryVindexNotSet, table.Name)
	}
	// ksidVindex is the primary vindex
	ksidVindex := table.ColumnVindexes[0]
	if where == nil {
		return engine.Scatter, ksidVindex, nil, nil, nil
	}

	filters := sqlparser.SplitAndExpression(nil, where.Expr)
	// go over the vindexes in the order of increasing cost
	for _, colVindex := range table.Ordered {
		if lu, isLu := colVindex.Vindex.(vindexes.LookupBackfill); isLu && lu.IsBackfilling() {
			// Checking if the Vindex is currently backfilling or not, if it isn't we can read from the vindex table
			// and we will be able to do a delete equal. Otherwise, we continue to look for next best vindex.
			continue
		}
		// get the best vindex option that can be used for this vindexes.ColumnVindex
		if vindexOption := getBestVindexOption(filters, colVindex); vindexOption != nil {
			return vindexOption.opcode, ksidVindex, colVindex.Vindex, vindexOption.values, nil
		}
	}
	return engine.Scatter, ksidVindex, nil, nil, nil
}

// getBestVindexOption returns the best vindex option that can be used for this vindexes.ColumnVindex
// It returns nil if there is no suitable way to use the ColumnVindex
func getBestVindexOption(exprs []sqlparser.Expr, index *vindexes.ColumnVindex) *vindexOptionDML {
	vindexPlusPredicates := &vindexPlusPredicatesDML{
		colVindex: index,
	}
	for _, filter := range exprs {
		comparison, ok := filter.(*sqlparser.ComparisonExpr)
		if !ok {
			continue
		}
		var colName *sqlparser.ColName
		var valExpr sqlparser.Expr
		if col, ok := comparison.Left.(*sqlparser.ColName); ok {
			colName = col
			valExpr = comparison.Right
		} else if col, ok := comparison.Right.(*sqlparser.ColName); ok {
			colName = col
			valExpr = comparison.Left
		} else {
			continue
		}

		var opcode engine.Opcode
		switch comparison.Operator {
		case sqlparser.EqualOp:
			if !sqlparser.IsValue(valExpr) {
				continue
			}
			opcode = engine.Equal
		case sqlparser.InOp:
			if !sqlparser.IsSimpleTuple(valExpr) {
				continue
			}
			opcode = engine.IN
		default:
			continue
		}
		expr, err := evalengine.Translate(comparison.Right, semantics.EmptySemTable())
		if err != nil {
			continue
		}
		addVindexOptions(colName, expr, opcode, vindexPlusPredicates)
	}
	return vindexPlusPredicates.bestOption()
}

// bestOption returns the option which is ready and has the lowest associated cost
func (vpp *vindexPlusPredicatesDML) bestOption() *vindexOptionDML {
	var best *vindexOptionDML
	for _, option := range vpp.options {
		if option.ready {
			if best == nil || lessCostDML(option.cost, best.cost) {
				best = option
			}
		}
	}
	return best
}

// lessCostDML compares two costDML and returns true if the first cost is cheaper than the second
func lessCostDML(c1, c2 costDML) bool {
	switch {
	case c1.opCode != c2.opCode:
		return c1.opCode < c2.opCode
	case c1.isUnique == c2.isUnique:
		return c1.vindexCost <= c2.vindexCost
	default:
		return c1.isUnique
	}
}

// addVindexOptions adds new vindexOptionDML if it matches any column of the vindexes.ColumnVindex
func addVindexOptions(column *sqlparser.ColName, value evalengine.Expr, opcode engine.Opcode, v *vindexPlusPredicatesDML) {
	switch v.colVindex.Vindex.(type) {
	case vindexes.SingleColumn:
		col := v.colVindex.Columns[0]
		if column.Name.Equal(col) {
			// single column vindex - just add the option
			vindex := v.colVindex
			v.options = append(v.options, &vindexOptionDML{
				values:      []evalengine.Expr{value},
				opcode:      opcode,
				foundVindex: vindex.Vindex,
				cost:        costForDML(v.colVindex, opcode),
				ready:       true,
			})
		}
	case vindexes.MultiColumn:
		colLoweredName := ""
		indexOfCol := -1
		for idx, col := range v.colVindex.Columns {
			if column.Name.Equal(col) {
				colLoweredName = column.Name.Lowered()
				indexOfCol = idx
				break
			}
		}
		if colLoweredName == "" {
			break
		}

		var newOption []*vindexOptionDML
		for _, op := range v.options {
			if op.ready {
				continue
			}
			_, isPresent := op.colsSeen[colLoweredName]
			if isPresent {
				continue
			}
			option := copyOptionDML(op)
			option.updateWithNewColumn(colLoweredName, indexOfCol, value, v.colVindex, opcode)
			newOption = append(newOption, option)
		}
		v.options = append(v.options, newOption...)

		// multi column vindex - just always add as new option
		option := createOptionDML(v.colVindex)
		option.updateWithNewColumn(colLoweredName, indexOfCol, value, v.colVindex, opcode)
		v.options = append(v.options, option)
	}
}

// copyOptionDML is used to copy vindexOptionDML
func copyOptionDML(orig *vindexOptionDML) *vindexOptionDML {
	colsSeen := make(map[string]any, len(orig.colsSeen))
	values := make([]evalengine.Expr, len(orig.values))

	copy(values, orig.values)
	for k, v := range orig.colsSeen {
		colsSeen[k] = v
	}
	vo := &vindexOptionDML{
		values:      values,
		colsSeen:    colsSeen,
		opcode:      orig.opcode,
		foundVindex: orig.foundVindex,
		cost:        orig.cost,
	}
	return vo
}

// updateWithNewColumn is used to update vindexOptionDML with a new column that matches one of its unseen columns
func (option *vindexOptionDML) updateWithNewColumn(colLoweredName string, indexOfCol int, value evalengine.Expr, colVindex *vindexes.ColumnVindex, opcode engine.Opcode) {
	option.colsSeen[colLoweredName] = true
	option.values[indexOfCol] = value
	option.ready = len(option.colsSeen) == len(colVindex.Columns)
	if option.opcode < opcode {
		option.opcode = opcode
		option.cost = costForDML(colVindex, opcode)
	}
}

// createOptionDML is used to create a vindexOptionDML
func createOptionDML(
	colVindex *vindexes.ColumnVindex,
) *vindexOptionDML {
	values := make([]evalengine.Expr, len(colVindex.Columns))
	vindex := colVindex.Vindex

	return &vindexOptionDML{
		values:      values,
		colsSeen:    map[string]any{},
		foundVindex: vindex,
	}
}

// costForDML returns a cost struct to make route choices easier to compare
func costForDML(foundVindex *vindexes.ColumnVindex, opcode engine.Opcode) costDML {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.Unsharded, engine.Scatter:
		return costDML{
			opCode: opcode,
		}
	}

	return costDML{
		vindexCost: foundVindex.Cost(),
		isUnique:   foundVindex.IsUnique(),
		opCode:     opcode,
	}
}

func buildDMLPlan(
	vschema plancontext.VSchema,
	dmlType string,
	stmt sqlparser.Statement,
	reservedVars *sqlparser.ReservedVars,
	tableExprs sqlparser.TableExprs,
	where *sqlparser.Where,
	orderBy sqlparser.OrderBy,
	limit *sqlparser.Limit,
	comments *sqlparser.ParsedComments,
	nodes ...sqlparser.SQLNode,
) (*engine.DML, []string, *vindexes.ColumnVindex, error) {
	edml := engine.NewDML()
	pb := newPrimitiveBuilder(vschema, newJointab(reservedVars))
	rb, err := pb.processDMLTable(tableExprs, reservedVars, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	edml.Keyspace = rb.eroute.Keyspace
	tc := &tableCollector{}
	for _, tval := range pb.st.tables {
		tc.addVindexTable(tval.vschemaTable)
	}

	edml.Table, err = pb.st.AllVschemaTableNames()
	if err != nil {
		return nil, nil, nil, err
	}
	if !edml.Keyspace.Sharded {
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		var subqueryArgs []sqlparser.SQLNode
		subqueryArgs = append(subqueryArgs, nodes...)
		subqueryArgs = append(subqueryArgs, where, orderBy, limit)
		subqueryIsUnsharded, subqueryTables := pb.finalizeUnshardedDMLSubqueries(reservedVars, subqueryArgs...)
		if subqueryIsUnsharded {
			vschema.WarnUnshardedOnly("subqueries can't be sharded in DML")
		} else {
			return nil, nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: sharded subqueries in DML")
		}
		edml.Opcode = engine.Unsharded
		// Generate query after all the analysis. Otherwise table name substitutions for
		// routed tables won't happen.
		edml.Query = generateQuery(stmt)
		edml.Table = append(edml.Table, subqueryTables...)
		return edml, tc.getTables(), nil, nil
	}

	if hasSubquery(stmt) {
		return nil, nil, nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: subqueries in sharded DML")
	}

	// Generate query after all the analysis. Otherwise table name substitutions for
	// routed tables won't happen.
	edml.Query = generateQuery(stmt)

	directives := comments.Directives()
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		edml.MultiShardAutocommit = true
	}

	edml.QueryTimeout = queryTimeout(directives)

	if len(pb.st.tables) != 1 {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "multi-table %s statement is not supported in sharded database", dmlType)
	}
	edmlTable, err := edml.GetSingleTable()
	if err != nil {
		return nil, nil, nil, err
	}
	routingType, ksidVindex, vindex, values, err := getDMLRouting(where, edmlTable)
	if err != nil {
		return nil, nil, nil, err
	}

	if rb.eroute.TargetDestination != nil {
		if rb.eroute.TargetTabletType != topodatapb.TabletType_PRIMARY {
			return nil, nil, nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.InnodbReadOnly, "unsupported: %s statement with a replica target", dmlType)
		}
		edml.Opcode = engine.ByDestination
		edml.TargetDestination = rb.eroute.TargetDestination
		return edml, tc.getTables(), ksidVindex, nil
	}

	edml.Opcode = routingType
	if routingType == engine.Scatter {
		if limit != nil {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "multi shard %s with limit is not supported", dmlType)
		}
	} else {
		edml.Vindex = vindex
		edml.Values = values
	}

	return edml, tc.getTables(), ksidVindex, nil
}

func generateDMLSubquery(tblExpr sqlparser.TableExpr, where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit, table *vindexes.Table, ksidCols []sqlparser.IdentifierCI) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	for idx, col := range ksidCols {
		if idx == 0 {
			buf.Myprintf("select %v", col)
		} else {
			buf.Myprintf(", %v", col)
		}
	}
	for _, cv := range table.Owned {
		for _, column := range cv.Columns {
			buf.Myprintf(", %v", column)
		}
	}
	buf.Myprintf(" from %v%v%v%v for update", tblExpr, where, orderBy, limit)
	return buf.String()
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}

// dmlFormatter strips out keyspace name from dmls.
func dmlFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case sqlparser.TableName:
		node.Name.Format(buf)
		return
	}
	node.Format(buf)
}
