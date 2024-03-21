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

package semantics

import (
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// TableInfo contains information about tables
	TableInfo interface {
		// Name returns the table name
		Name() (sqlparser.TableName, error)

		// GetVindexTable returns the vschema version of this TableInfo
		GetVindexTable() *vindexes.Table

		// IsInfSchema returns true if this table is information_schema
		IsInfSchema() bool

		// matches returns true if the provided table name matches this TableInfo
		matches(name sqlparser.TableName) bool

		// authoritative is true if we have exhaustive column information
		authoritative() bool

		// getAliasedTableExpr returns the AST struct behind this table
		GetAliasedTableExpr() *sqlparser.AliasedTableExpr

		// canShortCut will return nil when the keyspace needs to be checked,
		// and a true/false if the decision has been made already
		canShortCut() shortCut

		// getColumns returns the known column information for this table
		getColumns() []ColumnInfo

		dependencies(colName string, org originable) (dependencies, error)
		getExprFor(s string) (sqlparser.Expr, error)
		getTableSet(org originable) TableSet
	}

	// ColumnInfo contains information about columns
	ColumnInfo struct {
		Name      string
		Type      evalengine.Type
		Invisible bool
	}

	// ExprDependencies stores the tables that an expression depends on as a map
	ExprDependencies map[sqlparser.Expr]TableSet

	// QuerySignature is used to identify shortcuts in the planning process
	QuerySignature struct {
		Aggregation bool
		DML         bool
		Distinct    bool
		HashJoin    bool
		SubQueries  bool
		Union       bool
	}

	// SemTable contains semantic analysis information about the query.
	SemTable struct {
		// Tables stores information about the tables in the query, including derived tables
		Tables []TableInfo
		// Comments stores any comments of the /* vt+ */ type in the query
		Comments *sqlparser.ParsedComments
		// Warning stores any warnings generated during semantic analysis.
		Warning string
		// Collation represents the default collation for the query, usually inherited
		// from the connection's default collation.
		Collation collations.ID
		// ExprTypes maps expressions to their respective types in the query.
		ExprTypes map[sqlparser.Expr]evalengine.Type

		// NotSingleRouteErr stores errors related to missing schema information.
		// This typically occurs when a column's existence is uncertain.
		// Instead of failing early, the query is allowed to proceed, possibly
		// succeeding once it reaches MySQL.
		NotSingleRouteErr error

		// NotUnshardedErr stores errors that occur if the query isn't planned as a single route
		// targeting an unsharded keyspace. This typically arises when information is missing, but
		// for unsharded tables, the code operates in a passthrough mode, relying on the underlying
		// MySQL engine to handle errors appropriately.
		NotUnshardedErr error

		// Recursive contains dependencies from the expression to the actual tables
		// in the query (excluding derived tables). For columns in derived tables,
		// this map holds the accumulated dependencies for the column expression.
		Recursive ExprDependencies
		// Direct stores information about the closest dependency for an expression.
		// It doesn't recurse inside derived tables to find the original dependencies.
		Direct ExprDependencies

		// Targets contains the TableSet of each table getting modified by the update/delete statement.
		Targets TableSet

		// ColumnEqualities is used for transitive closures (e.g., if a == b and b == c, then a == c).
		ColumnEqualities map[columnName][]sqlparser.Expr

		// ExpandedColumns is a map of all the added columns for a given table.
		// The columns were added because of the use of `*` in the query
		ExpandedColumns map[sqlparser.TableName][]*sqlparser.ColName

		columns map[*sqlparser.Union]sqlparser.SelectExprs

		comparator *sqlparser.Comparator

		// StatementIDs is a map of statements and all the table IDs that are contained within
		StatementIDs map[sqlparser.Statement]TableSet

		// QuerySignature is used to identify shortcuts in the planning process
		QuerySignature QuerySignature

		// We store the child and parent foreign keys that are involved in the given query.
		// The map is keyed by the tableset of the table that each of the foreign key belongs to.
		childForeignKeysInvolved  map[TableSet][]vindexes.ChildFKInfo
		parentForeignKeysInvolved map[TableSet][]vindexes.ParentFKInfo
		childFkToUpdExprs         map[string]sqlparser.UpdateExprs
		collEnv                   *collations.Environment
	}

	columnName struct {
		Table      TableSet
		ColumnName string
	}

	// SchemaInformation is used to provide table information from Vschema.
	SchemaInformation interface {
		FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
		ConnCollation() collations.ID
		Environment() *vtenv.Environment
		// ForeignKeyMode returns the foreign_key flag value
		ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error)
		GetForeignKeyChecksState() *bool
		KeyspaceError(keyspace string) error
	}

	shortCut = int
)

const (
	canShortCut shortCut = iota
	cannotShortCut
	dependsOnKeyspace
)

var (
	// ErrNotSingleTable refers to an error happening when something should be used only for single tables
	ErrNotSingleTable = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should only be used for single tables")
)

// CopyDependencies copies the dependencies from one expression into the other
func (st *SemTable) CopyDependencies(from, to sqlparser.Expr) {
	if ValidAsMapKey(to) {
		st.Recursive[to] = st.RecursiveDeps(from)
		st.Direct[to] = st.DirectDeps(from)
		if ValidAsMapKey(from) {
			if typ, found := st.ExprTypes[from]; found {
				st.ExprTypes[to] = typ
			}
		}
	}
}

// GetChildForeignKeysForTargets gets the child foreign keys as a list for all the target tables.
func (st *SemTable) GetChildForeignKeysForTargets() (fks []vindexes.ChildFKInfo) {
	for _, ts := range st.Targets.Constituents() {
		fks = append(fks, st.childForeignKeysInvolved[ts]...)
	}
	return fks
}

// GetChildForeignKeysForTableSet gets the child foreign keys as a listfor the TableSet.
func (st *SemTable) GetChildForeignKeysForTableSet(target TableSet) (fks []vindexes.ChildFKInfo) {
	for _, ts := range st.Targets.Constituents() {
		if target.IsSolvedBy(ts) {
			fks = append(fks, st.childForeignKeysInvolved[ts]...)
		}
	}
	return fks
}

// GetChildForeignKeysForTable gets the child foreign keys as a list for the specified TableName.
func (st *SemTable) GetChildForeignKeysForTable(tbl sqlparser.TableName) ([]vindexes.ChildFKInfo, error) {
	ts, err := st.GetTargetTableSetForTableName(tbl)
	if err != nil {
		return nil, err
	}
	return st.childForeignKeysInvolved[ts], nil
}

// GetChildForeignKeysList gets the child foreign keys as a list.
func (st *SemTable) GetChildForeignKeysList() []vindexes.ChildFKInfo {
	var childFkInfos []vindexes.ChildFKInfo
	for _, infos := range st.childForeignKeysInvolved {
		childFkInfos = append(childFkInfos, infos...)
	}
	return childFkInfos
}

// GetParentForeignKeysForTargets gets the parent foreign keys as a list for all the target tables.
func (st *SemTable) GetParentForeignKeysForTargets() (fks []vindexes.ParentFKInfo) {
	for _, ts := range st.Targets.Constituents() {
		fks = append(fks, st.parentForeignKeysInvolved[ts]...)
	}
	return fks
}

// GetParentForeignKeysForTableSet gets the parent foreign keys as a list for the TableSet.
func (st *SemTable) GetParentForeignKeysForTableSet(target TableSet) (fks []vindexes.ParentFKInfo) {
	for _, ts := range st.Targets.Constituents() {
		if target.IsSolvedBy(ts) {
			fks = append(fks, st.parentForeignKeysInvolved[ts]...)
		}
	}
	return fks
}

// GetParentForeignKeysList gets the parent foreign keys as a list.
func (st *SemTable) GetParentForeignKeysList() []vindexes.ParentFKInfo {
	var parentFkInfos []vindexes.ParentFKInfo
	for _, infos := range st.parentForeignKeysInvolved {
		parentFkInfos = append(parentFkInfos, infos...)
	}
	return parentFkInfos
}

// GetUpdateExpressionsForFk gets the update expressions for the given serialized foreign key constraint.
func (st *SemTable) GetUpdateExpressionsForFk(foreignKey string) sqlparser.UpdateExprs {
	return st.childFkToUpdExprs[foreignKey]
}

// RemoveParentForeignKey removes the given foreign key from the parent foreign keys that sem table stores.
func (st *SemTable) RemoveParentForeignKey(fkToIgnore string) error {
	for ts, fkInfos := range st.parentForeignKeysInvolved {
		ti, err := st.TableInfoFor(ts)
		if err != nil {
			return err
		}
		vt := ti.GetVindexTable()
		for idx, info := range fkInfos {
			if info.String(vt) == fkToIgnore {
				st.parentForeignKeysInvolved[ts] = append(fkInfos[0:idx], fkInfos[idx+1:]...)
				return nil
			}
		}
	}
	return nil
}

// RemoveNonRequiredForeignKeys prunes the list of foreign keys that the query involves.
// This function considers whether VTGate needs to validate all foreign keys
// or can delegate some of the responsibility to MySQL.
// In the latter case, the following types of foreign keys can be safely removed from our list:
// 1. Shard-scoped parent foreign keys: MySQL itself will reject a DML operation that violates these constraints.
// 2. Shard-scoped RESTRICT foreign keys: MySQL will also fail the operation if these foreign key constraints are breached.
func (st *SemTable) RemoveNonRequiredForeignKeys(verifyAllFks bool, getAction func(fk vindexes.ChildFKInfo) sqlparser.ReferenceAction) error {
	if verifyAllFks {
		return nil
	}
	// Go over all the parent foreign keys.
	for ts, parentFKs := range st.parentForeignKeysInvolved {
		ti, err := st.TableInfoFor(ts)
		if err != nil {
			return err
		}
		vt := ti.GetVindexTable()
		var updatedParentFks []vindexes.ParentFKInfo
		for _, fk := range parentFKs {
			// Cross-keyspace foreign keys require verification.
			if vt.Keyspace.Name != fk.Table.Keyspace.Name {
				updatedParentFks = append(updatedParentFks, fk)
				continue
			}
			// Non shard-scoped foreign keys require verification.
			if !isShardScoped(fk.Table, vt, fk.ParentColumns, fk.ChildColumns) {
				updatedParentFks = append(updatedParentFks, fk)
			}
		}
		st.parentForeignKeysInvolved[ts] = updatedParentFks
	}

	// Go over all the child foreign keys.
	for ts, childFks := range st.childForeignKeysInvolved {
		ti, err := st.TableInfoFor(ts)
		if err != nil {
			return err
		}
		vt := ti.GetVindexTable()
		var updatedChildFks []vindexes.ChildFKInfo
		for _, fk := range childFks {
			// Cross-keyspace foreign keys require verification.
			if vt.Keyspace.Name != fk.Table.Keyspace.Name {
				updatedChildFks = append(updatedChildFks, fk)
				continue
			}
			switch getAction(fk) {
			case sqlparser.Cascade, sqlparser.SetNull, sqlparser.SetDefault:
				updatedChildFks = append(updatedChildFks, fk)
				continue
			}
			// sqlparser.Restrict, sqlparser.NoAction, sqlparser.DefaultAction
			// all the actions means the same thing i.e. Restrict
			// do not allow modification if there is a child row.
			// Check if the restrict is shard scoped.
			if !isShardScoped(vt, fk.Table, fk.ParentColumns, fk.ChildColumns) {
				updatedChildFks = append(updatedChildFks, fk)
			}
		}
		st.childForeignKeysInvolved[ts] = updatedChildFks
	}

	return nil
}

// ErrIfFkDependentColumnUpdated checks if a foreign key column that is being updated is dependent on another column which also being updated.
func (st *SemTable) ErrIfFkDependentColumnUpdated(updateExprs sqlparser.UpdateExprs) error {
	// Go over all the update expressions
	for _, updateExpr := range updateExprs {
		deps := st.RecursiveDeps(updateExpr.Name)
		if deps.NumberOfTables() != 1 {
			return vterrors.VT13001("expected to have single table dependency")
		}
		// Get all the child and parent foreign keys for the given table that the update expression belongs to.
		childFks := st.childForeignKeysInvolved[deps]
		parentFKs := st.parentForeignKeysInvolved[deps]

		involvedInFk := false
		// Check if this updated column is part of any child or parent foreign key.
		for _, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				involvedInFk = true
				break
			}
		}
		for _, parentFk := range parentFKs {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				involvedInFk = true
				break
			}
		}

		if !involvedInFk {
			continue
		}

		// We cannot support updating a foreign key column that is using a column which is also being updated for 2 reasons—
		// 1. For the child foreign keys, we aren't sure what the final value of the updated foreign key column will be. So we don't know
		// what to cascade to the child. The selection that we do isn't enough to know if the updated value, since one of the columns used in the update is also being updated.
		// 2. For the parent foreign keys, we don't know if we need to reject this update. Because we don't know the final updated value, the update might need to be failed,
		// but we can't say for certain.
		var dependencyUpdatedErr error
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			col, ok := node.(*sqlparser.ColName)
			if !ok {
				return true, nil
			}
			// self reference column dependency is not considered a dependent column being updated.
			if st.EqualsExpr(updateExpr.Name, col) {
				return true, nil
			}
			for _, updExpr := range updateExprs {
				if st.EqualsExpr(updExpr.Name, col) {
					dependencyUpdatedErr = vterrors.VT12001(fmt.Sprintf("%v column referenced in foreign key column %v is itself updated", sqlparser.String(col), sqlparser.String(updateExpr.Name)))
					return false, nil
				}
			}
			return false, nil
		}, updateExpr.Expr)
		if dependencyUpdatedErr != nil {
			return dependencyUpdatedErr
		}
	}
	return nil
}

// HasNonLiteralForeignKeyUpdate checks for non-literal updates in expressions linked to a foreign key.
func (st *SemTable) HasNonLiteralForeignKeyUpdate(updExprs sqlparser.UpdateExprs) bool {
	for _, updateExpr := range updExprs {
		if sqlparser.IsLiteral(updateExpr.Expr) {
			continue
		}
		parentFks := st.parentForeignKeysInvolved[st.RecursiveDeps(updateExpr.Name)]
		for _, parentFk := range parentFks {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				return true
			}
		}
		childFks := st.childForeignKeysInvolved[st.RecursiveDeps(updateExpr.Name)]
		for _, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				return true
			}
		}
	}
	return false
}

// isShardScoped checks if the foreign key constraint is shard-scoped or not. It uses the vindex information to make this call.
func isShardScoped(pTable *vindexes.Table, cTable *vindexes.Table, pCols sqlparser.Columns, cCols sqlparser.Columns) bool {
	if !pTable.Keyspace.Sharded {
		return true
	}

	pPrimaryVdx := pTable.ColumnVindexes[0]
	cPrimaryVdx := cTable.ColumnVindexes[0]

	// If the primary vindexes don't match between the parent and child table,
	// we cannot infer that the fk constraint in shard scoped.
	if cPrimaryVdx.Vindex != pPrimaryVdx.Vindex {
		return false
	}

	childFkContatined, childFkIndexes := cCols.Indexes(cPrimaryVdx.Columns)
	if !childFkContatined {
		// PrimaryVindex is not part of the foreign key constraint on the children side.
		// So it is a cross-shard foreign key.
		return false
	}

	// We need to run the same check for the parent columns.
	parentFkContatined, parentFkIndexes := pCols.Indexes(pPrimaryVdx.Columns)
	if !parentFkContatined {
		return false
	}

	// Both the child and parent table contain the foreign key and that the vindexes are the same,
	// now we need to make sure, that the indexes of both match.
	// For example, consider the following tables,
	//	t1 (primary vindex (x,y))
	//	t2 (primary vindex (a,b))
	//	If we have a foreign key constraint from t1(x,y) to t2(b,a), then they are not shard scoped.
	//	Let's say in t1, (1,3) will be in -80 and (3,1) will be in 80-, then in t2 (1,3) will end up in 80-.
	for i := range parentFkIndexes {
		if parentFkIndexes[i] != childFkIndexes[i] {
			return false
		}
	}
	return true
}

// ForeignKeysPresent returns whether there are any foreign key constraints left in the semantic table that require handling.
func (st *SemTable) ForeignKeysPresent() bool {
	for _, fkInfos := range st.childForeignKeysInvolved {
		if len(fkInfos) > 0 {
			return true
		}
	}
	for _, fkInfos := range st.parentForeignKeysInvolved {
		if len(fkInfos) > 0 {
			return true
		}
	}
	return false
}

func (st *SemTable) SelectExprs(sel sqlparser.SelectStatement) sqlparser.SelectExprs {
	switch sel := sel.(type) {
	case *sqlparser.Select:
		return sel.SelectExprs
	case *sqlparser.Union:
		exprs, found := st.columns[sel]
		if found {
			return exprs
		}
		for stmt, exprs := range st.columns {
			if sqlparser.Equals.SelectStatement(stmt, sel) {
				return exprs
			}
		}
		panic("BUG: union not found in semantic table for select expressions")
	}
	panic(fmt.Sprintf("BUG: unexpected select statement type %T", sel))
}

func getColumnNames(exprs sqlparser.SelectExprs) (expanded bool, selectExprs sqlparser.SelectExprs) {
	expanded = true
	for _, col := range exprs {
		switch col := col.(type) {
		case *sqlparser.AliasedExpr:
			expr := sqlparser.NewColName(col.ColumnName())
			selectExprs = append(selectExprs, &sqlparser.AliasedExpr{Expr: expr})
		default:
			selectExprs = append(selectExprs, col)
			expanded = false
		}
	}
	return
}

// CopySemanticInfo copies all semantic information we have about this SQLNode so that it also applies to the `to` node
func (st *SemTable) CopySemanticInfo(from, to sqlparser.SQLNode) {
	if f, ok := from.(sqlparser.Statement); ok {
		t, ok := to.(sqlparser.Statement)
		if ok {
			st.StatementIDs[t] = st.StatementIDs[f]
		}
	}

	switch f := from.(type) {
	case sqlparser.Expr:
		t, ok := to.(sqlparser.Expr)
		if !ok {
			return
		}
		st.CopyDependencies(f, t)
	case *sqlparser.Union:
		t, ok := to.(*sqlparser.Union)
		if !ok {
			return
		}
		exprs := st.columns[f]
		st.columns[t] = exprs
	default:
		return
	}
}

// Cloned copies the dependencies from one expression into the other
func (st *SemTable) Cloned(from, to sqlparser.SQLNode) {
	f, fromOK := from.(sqlparser.Expr)
	t, toOK := to.(sqlparser.Expr)
	if !(fromOK && toOK) {
		return
	}
	st.CopyDependencies(f, t)
}

// EmptySemTable creates a new empty SemTable
func EmptySemTable() *SemTable {
	return &SemTable{
		Recursive:        map[sqlparser.Expr]TableSet{},
		Direct:           map[sqlparser.Expr]TableSet{},
		ColumnEqualities: map[columnName][]sqlparser.Expr{},
		columns:          map[*sqlparser.Union]sqlparser.SelectExprs{},
		ExprTypes:        make(map[sqlparser.Expr]evalengine.Type),
	}
}

// TableSetFor returns the bitmask for this particular table
func (st *SemTable) TableSetFor(t *sqlparser.AliasedTableExpr) TableSet {
	for idx, t2 := range st.Tables {
		if t == t2.GetAliasedTableExpr() {
			return SingleTableSet(idx)
		}
	}
	return EmptyTableSet()
}

// ReplaceTableSetFor replaces the given single TabletSet with the new *sqlparser.AliasedTableExpr
func (st *SemTable) ReplaceTableSetFor(id TableSet, t *sqlparser.AliasedTableExpr) {
	if st == nil {
		return
	}
	if id.NumberOfTables() != 1 {
		// This is probably a derived table
		return
	}
	tblOffset := id.TableOffset()
	if tblOffset > len(st.Tables) {
		// This should not happen and is probably a bug, but the output query will still work fine
		return
	}
	switch tbl := st.Tables[id.TableOffset()].(type) {
	case *RealTable:
		tbl.ASTNode = t
	case *DerivedTable:
		tbl.ASTNode = t
	}
}

// TableInfoFor returns the table info for the table set. It should contains only single table.
func (st *SemTable) TableInfoFor(id TableSet) (TableInfo, error) {
	offset := id.TableOffset()
	if offset < 0 {
		return nil, ErrNotSingleTable
	}
	return st.Tables[offset], nil
}

// RecursiveDeps return the table dependencies of the expression.
func (st *SemTable) RecursiveDeps(expr sqlparser.Expr) TableSet {
	return st.Recursive.dependencies(expr)
}

// DirectDeps return the table dependencies of the expression.
func (st *SemTable) DirectDeps(expr sqlparser.Expr) TableSet {
	return st.Direct.dependencies(expr)
}

// AddColumnEquality adds a relation of the given colName to the ColumnEqualities map
func (st *SemTable) AddColumnEquality(colName *sqlparser.ColName, expr sqlparser.Expr) {
	ts := st.Direct.dependencies(colName)
	columnName := columnName{
		Table:      ts,
		ColumnName: colName.Name.String(),
	}
	elem := st.ColumnEqualities[columnName]
	elem = append(elem, expr)
	st.ColumnEqualities[columnName] = elem
}

// GetExprAndEqualities returns a slice containing the given expression, and it's known equalities if any
func (st *SemTable) GetExprAndEqualities(expr sqlparser.Expr) []sqlparser.Expr {
	result := []sqlparser.Expr{expr}
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		table := st.DirectDeps(expr)
		k := columnName{Table: table, ColumnName: expr.Name.String()}
		result = append(result, st.ColumnEqualities[k]...)
	}
	return result
}

// TableInfoForExpr returns the table info of the table that this expression depends on.
// Careful: this only works for expressions that have a single table dependency
func (st *SemTable) TableInfoForExpr(expr sqlparser.Expr) (TableInfo, error) {
	return st.TableInfoFor(st.Direct.dependencies(expr))
}

// AddExprs adds new select exprs to the SemTable.
func (st *SemTable) AddExprs(tbl *sqlparser.AliasedTableExpr, cols sqlparser.SelectExprs) {
	tableSet := st.TableSetFor(tbl)
	for _, col := range cols {
		st.Recursive[col.(*sqlparser.AliasedExpr).Expr] = tableSet
	}
}

// TypeForExpr returns the type of expressions in the query
func (st *SemTable) TypeForExpr(e sqlparser.Expr) (evalengine.Type, bool) {
	if typ, found := st.ExprTypes[e]; found {
		return typ, true
	}

	// We add a lot of WeightString() expressions to queries at late stages of the planning,
	// which means that they don't have any type information. We can safely assume that they
	// are VarBinary, since that's the only type that WeightString() can return.
	ws, isWS := e.(*sqlparser.WeightStringFuncExpr)
	if isWS {
		wt, _ := st.TypeForExpr(ws.Expr)
		return evalengine.NewTypeEx(sqltypes.VarBinary, collations.CollationBinaryID, wt.Nullable(), 0, 0), true
	}

	return evalengine.Type{}, false
}

// NeedsWeightString returns true if the given expression needs weight_string to do safe comparisons
func (st *SemTable) NeedsWeightString(e sqlparser.Expr) bool {
	switch e := e.(type) {
	case *sqlparser.WeightStringFuncExpr, *sqlparser.Literal:
		return false
	default:
		typ, found := st.ExprTypes[e]
		if !found {
			return true
		}

		if !sqltypes.IsText(typ.Type()) {
			return false
		}

		return !st.collEnv.IsSupported(typ.Collation())
	}
}

func (st *SemTable) DefaultCollation() collations.ID {
	return st.Collation
}

// dependencies return the table dependencies of the expression. This method finds table dependencies recursively
func (d ExprDependencies) dependencies(expr sqlparser.Expr) (deps TableSet) {
	if ValidAsMapKey(expr) {
		// we have something that could live in the cache
		var found bool
		deps, found = d[expr]
		if found {
			return deps
		}
		defer func() {
			d[expr] = deps
		}()
	}

	// During the original semantic analysis, all ColNames were found and bound to the corresponding tables
	// Here, we'll walk the expression tree and look to see if we can find any sub-expressions
	// that have already set dependencies.
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		expr, ok := node.(sqlparser.Expr)
		if !ok || !ValidAsMapKey(expr) {
			// if this is not an expression, or it is an expression we can't use as a map-key,
			// just carry on down the tree
			return true, nil
		}

		set, found := d[expr]
		deps = deps.Merge(set)

		// if we found a cached value, there is no need to continue down to visit children
		return !found, nil
	}, expr)

	return deps
}

// RewriteDerivedTableExpression rewrites all the ColName instances in the supplied expression with
// the expressions behind the column definition of the derived table
// SELECT foo FROM (SELECT id+42 as foo FROM user) as t
// We need `foo` to be translated to `id+42` on the inside of the derived table
func RewriteDerivedTableExpression(expr sqlparser.Expr, vt TableInfo) sqlparser.Expr {
	return sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		node, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}
		exp, err := vt.getExprFor(node.Name.String())
		if err == nil {
			cursor.Replace(exp)
			return
		}

		// cloning the expression and removing the qualifier
		col := *node
		col.Qualifier = sqlparser.TableName{}
		cursor.Replace(&col)

	}, nil).(sqlparser.Expr)
}

// CopyExprInfo lookups src in the ExprTypes map and, if a key is found, assign
// the corresponding Type value of src to dest.
func (st *SemTable) CopyExprInfo(src, dest sqlparser.Expr) {
	if srcType, found := st.ExprTypes[src]; found {
		st.ExprTypes[dest] = srcType
	}
}

var columnNotSupportedErr = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "column access not supported here")

// ColumnLookup implements the TranslationLookup interface
func (st *SemTable) ColumnLookup(col *sqlparser.ColName) (int, error) {
	return 0, columnNotSupportedErr
}

// SingleUnshardedKeyspace returns the single keyspace if all tables in the query are in the same, unsharded keyspace
func (st *SemTable) SingleUnshardedKeyspace() (ks *vindexes.Keyspace, tables []*vindexes.Table) {
	return singleUnshardedKeyspace(st.Tables)
}

func singleUnshardedKeyspace(tableInfos []TableInfo) (ks *vindexes.Keyspace, tables []*vindexes.Table) {
	validKS := func(this *vindexes.Keyspace) bool {
		if this == nil || this.Sharded {
			return false
		}
		if ks == nil {
			// first keyspace we see
			ks = this
		} else if ks != this {
			// even if both are unsharded, we only allow a single keyspace for these queries
			return false
		}
		return true
	}

	for _, table := range tableInfos {
		if _, isDT := table.(*DerivedTable); isDT {
			continue
		}

		sc := table.canShortCut()
		var vtbl *vindexes.Table

		switch sc {
		case dependsOnKeyspace:
			// we have to check the KS if the table doesn't know if it can be shortcut or not
			vtbl = table.GetVindexTable()
			if !validKS(vtbl.Keyspace) {
				return nil, nil
			}
		case canShortCut:
			// the table knows that it's safe to shortcut
			vtbl = table.GetVindexTable()
			if vtbl == nil {
				continue
			}
		case cannotShortCut:
			// the table knows that we can't shortcut
			return nil, nil
		}

		tables = append(tables, vtbl)
	}
	return ks, tables
}

// SingleUnshardedKeyspace returns the single keyspace if all tables in the query are in the same keyspace
func (st *SemTable) SingleKeyspace() (ks *vindexes.Keyspace) {
	validKS := func(this *vindexes.Keyspace) bool {
		if this == nil {
			return true
		}
		if ks == nil {
			// first keyspace we see
			ks = this
		} else if ks != this {
			return false
		}
		return true
	}

	for _, table := range st.Tables {
		if _, isDT := table.(*DerivedTable); isDT {
			continue
		}

		vtbl := table.GetVindexTable()
		if !validKS(vtbl.Keyspace) {
			return nil
		}
	}
	return
}

// EqualsExpr compares two expressions using the semantic analysis information.
// This means that we use the binding info to recognize that two ColName's can point to the same
// table column even though they are written differently. Example would be the `foobar` column in the following query:
// `SELECT foobar FROM tbl ORDER BY tbl.foobar`
// The expression in the select list is not equal to the one in the ORDER BY,
// but they point to the same column and would be considered equal by this method
func (st *SemTable) EqualsExpr(a, b sqlparser.Expr) bool {
	// If there is no SemTable, then we cannot compare the expressions.
	if st == nil {
		return sqlparser.Equals.Expr(a, b)
	}
	return st.ASTEquals().Expr(a, b)
}

// EqualsExprWithDeps compares two expressions taking into account their semantic
// information. Dependency data typically pertains only to column expressions,
// this method considers them for all expression types. The method checks
// if dependency information exists for both expressions. If it does, the dependencies
// must match. If we are missing dependency information for either
func (st *SemTable) EqualsExprWithDeps(a, b sqlparser.Expr) bool {
	eq := st.ASTEquals().Expr(a, b)
	if !eq {
		return false
	}
	adeps := st.RecursiveDeps(a)
	bdeps := st.RecursiveDeps(b)
	if adeps.IsEmpty() || bdeps.IsEmpty() || adeps == bdeps {
		return true
	}
	return false
}

func (st *SemTable) ContainsExpr(e sqlparser.Expr, expres []sqlparser.Expr) bool {
	for _, expre := range expres {
		if st.EqualsExpr(e, expre) {
			return true
		}
	}
	return false
}

// Uniquify takes a slice of expressions and removes any duplicates
func (st *SemTable) Uniquify(in []sqlparser.Expr) []sqlparser.Expr {
	result := make([]sqlparser.Expr, 0, len(in))
	idx := 0
outer:
	for _, expr := range in {
		for i := 0; i < idx; i++ {
			if st.EqualsExprWithDeps(result[i], expr) {
				continue outer
			}
			result = append(result, expr)
			idx++
		}
	}
	return result
}

// AndExpressions ands together two or more expressions, minimising the expr when possible
func (st *SemTable) AndExpressions(exprs ...sqlparser.Expr) sqlparser.Expr {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		result := (sqlparser.Expr)(nil)
	outer:
		// we'll loop and remove any duplicates
		for i, expr := range exprs {
			if expr == nil {
				continue
			}
			if result == nil {
				result = expr
				continue outer
			}

			for j := 0; j < i; j++ {
				if st.EqualsExpr(expr, exprs[j]) {
					continue outer
				}
			}
			result = &sqlparser.AndExpr{Left: result, Right: expr}
		}
		return result
	}
}

// ASTEquals returns a sqlparser.Comparator that uses the semantic information in this SemTable to
// explicitly compare column names for equality.
func (st *SemTable) ASTEquals() *sqlparser.Comparator {
	if st == nil {
		return sqlparser.Equals
	}
	if st.comparator == nil {
		st.comparator = &sqlparser.Comparator{
			RefOfColName_: func(a, b *sqlparser.ColName) bool {
				aDeps := st.RecursiveDeps(a)
				bDeps := st.RecursiveDeps(b)
				if aDeps != bDeps && (aDeps.IsEmpty() || bDeps.IsEmpty()) {
					// if we don't know, we don't know
					return sqlparser.Equals.RefOfColName(a, b)
				}
				return a.Name.Equal(b.Name) && aDeps == bDeps
			},
		}
	}
	return st.comparator
}

func (st *SemTable) Clone(n sqlparser.SQLNode) sqlparser.SQLNode {
	return sqlparser.CopyOnRewrite(n, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		expr, isExpr := cursor.Node().(sqlparser.Expr)
		if !isExpr {
			return
		}
		cursor.Replace(sqlparser.CloneExpr(expr))
	}, st.CopySemanticInfo)
}

// UpdateChildFKExpr updates the child foreign key expression with the new expression.
func (st *SemTable) UpdateChildFKExpr(origUpdExpr *sqlparser.UpdateExpr, newExpr sqlparser.Expr) {
	for _, exprs := range st.childFkToUpdExprs {
		for idx, updateExpr := range exprs {
			if updateExpr == origUpdExpr {
				exprs[idx].Expr = newExpr
			}
		}
	}
}

// GetTargetTableSetForTableName returns the TableSet for the given table name from the target tables.
func (st *SemTable) GetTargetTableSetForTableName(name sqlparser.TableName) (TableSet, error) {
	for _, target := range st.Targets.Constituents() {
		tbl, err := st.Tables[target.TableOffset()].Name()
		if err != nil {
			return "", err
		}
		if tbl.Name == name.Name {
			return target, nil
		}
	}
	return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "target table '%s' not found", sqlparser.String(name))
}
