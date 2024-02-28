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

package schemadiff

import (
	"errors"
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Schema represents a database schema, which may contain entities such as tables and views.
// Schema is not in itself an Entity, since it is more of a collection of entities.
type Schema struct {
	tables []*CreateTableEntity
	views  []*CreateViewEntity

	named  map[string]Entity
	sorted []Entity

	foreignKeyParents  []*CreateTableEntity // subset of tables
	foreignKeyChildren []*CreateTableEntity // subset of tables
	foreignKeyLoopMap  map[string][]string  // map of table name that either participate, or directly or indirectly reference foreign key loops

	env *Environment
}

// newEmptySchema is used internally to initialize a Schema object
func newEmptySchema(env *Environment) *Schema {
	schema := &Schema{
		tables: []*CreateTableEntity{},
		views:  []*CreateViewEntity{},
		named:  map[string]Entity{},
		sorted: []Entity{},

		foreignKeyParents:  []*CreateTableEntity{},
		foreignKeyChildren: []*CreateTableEntity{},
		foreignKeyLoopMap:  map[string][]string{},

		env: env,
	}
	return schema
}

// NewSchemaFromEntities creates a valid and normalized schema based on list of entities
func NewSchemaFromEntities(env *Environment, entities []Entity) (*Schema, error) {
	schema := newEmptySchema(env)
	for _, e := range entities {
		switch c := e.(type) {
		case *CreateTableEntity:
			schema.tables = append(schema.tables, c)
		case *CreateViewEntity:
			schema.views = append(schema.views, c)
		default:
			return nil, &UnsupportedEntityError{Entity: c.Name(), Statement: c.Create().CanonicalStatementString()}
		}
	}
	err := schema.normalize()
	return schema, err
}

// NewSchemaFromStatements creates a valid and normalized schema based on list of valid statements
func NewSchemaFromStatements(env *Environment, statements []sqlparser.Statement) (*Schema, error) {
	entities := make([]Entity, 0, len(statements))
	for _, s := range statements {
		switch stmt := s.(type) {
		case *sqlparser.CreateTable:
			c, err := NewCreateTableEntity(env, stmt)
			if err != nil {
				return nil, err
			}
			entities = append(entities, c)
		case *sqlparser.CreateView:
			v, err := NewCreateViewEntity(env, stmt)
			if err != nil {
				return nil, err
			}
			entities = append(entities, v)
		default:
			return nil, &UnsupportedStatementError{Statement: sqlparser.CanonicalString(s)}
		}
	}
	return NewSchemaFromEntities(env, entities)
}

// NewSchemaFromQueries creates a valid and normalized schema based on list of queries
func NewSchemaFromQueries(env *Environment, queries []string) (*Schema, error) {
	statements := make([]sqlparser.Statement, 0, len(queries))
	for _, q := range queries {
		stmt, err := env.Parser().ParseStrictDDL(q)
		if err != nil {
			return nil, err
		}
		statements = append(statements, stmt)
	}
	return NewSchemaFromStatements(env, statements)
}

// NewSchemaFromSQL creates a valid and normalized schema based on a SQL blob that contains
// CREATE statements for various objects (tables, views)
func NewSchemaFromSQL(env *Environment, sql string) (*Schema, error) {
	statements, err := env.Parser().SplitStatements(sql)
	if err != nil {
		return nil, err
	}
	return NewSchemaFromStatements(env, statements)
}

// getForeignKeyParentTableNames analyzes a CREATE TABLE definition and extracts all referenced foreign key tables names.
// A table name may appear twice in the result output, if it is referenced by more than one foreign key
func getForeignKeyParentTableNames(createTable *sqlparser.CreateTable) (names []string) {
	for _, cs := range createTable.TableSpec.Constraints {
		if check, ok := cs.Details.(*sqlparser.ForeignKeyDefinition); ok {
			parentTableName := check.ReferenceDefinition.ReferencedTable.Name.String()
			names = append(names, parentTableName)
		}
	}
	return names
}

// findForeignKeyLoop is a stateful recursive function that determines whether a given table participates in a foreign
// key loop or derives from one. It returns a list of table names that form a loop, or nil if no loop is found.
// The function updates and checks the stateful map s.foreignKeyLoopMap to avoid re-analyzing the same table twice.
func (s *Schema) findForeignKeyLoop(tableName string, seen []string) (loop []string) {
	if loop := s.foreignKeyLoopMap[tableName]; loop != nil {
		return loop
	}
	t := s.Table(tableName)
	if t == nil {
		return nil
	}
	seen = append(seen, tableName)
	for i, seenTable := range seen {
		if i == len(seen)-1 {
			// as we've just appended the table name to the end of the slice, we should skip it.
			break
		}
		if seenTable == tableName {
			// This table alreay appears in `seen`.
			// We only return the suffix of `seen` that starts (and now ends) with this table.
			return seen[i:]
		}
	}
	for _, referencedTableName := range getForeignKeyParentTableNames(t.CreateTable) {
		if loop := s.findForeignKeyLoop(referencedTableName, seen); loop != nil {
			// Found loop. Update cache.
			// It's possible for one table to participate in more than one foreign key loop, but
			// we suffice with one loop, since we already only ever report one foreign key error
			// per table.
			s.foreignKeyLoopMap[tableName] = loop
			return loop
		}
	}
	return nil
}

// getViewDependentTableNames analyzes a CREATE VIEW definition and extracts all tables/views read by this view
func getViewDependentTableNames(createView *sqlparser.CreateView) (names []string) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.TableName:
			names = append(names, node.Name.String())
		case *sqlparser.AliasedTableExpr:
			if tableName, ok := node.Expr.(sqlparser.TableName); ok {
				names = append(names, tableName.Name.String())
			}
			// or, this could be a more complex expression, like a derived table `(select * from v1) as derived`,
			// in which case further Walk-ing will eventually find the "real" table name
		}
		return true, nil
	}, createView)
	return names
}

// normalize is called as part of Schema creation process. The user may only get a hold of normalized schema.
// It validates some cross-entity constraints, and orders entity based on dependencies (e.g. tables, views that read from tables, 2nd level views, etc.)
func (s *Schema) normalize() error {
	var errs error

	s.named = make(map[string]Entity, len(s.tables)+len(s.views))
	s.sorted = make([]Entity, 0, len(s.tables)+len(s.views))
	// Verify no two entities share same name
	for _, t := range s.tables {
		name := t.Name()
		if _, ok := s.named[name]; ok {
			return &ApplyDuplicateEntityError{Entity: name}
		}
		s.named[name] = t
	}
	for _, v := range s.views {
		name := v.Name()
		if _, ok := s.named[name]; ok {
			return &ApplyDuplicateEntityError{Entity: name}
		}
		s.named[name] = v
	}

	// Generally speaking, we want tables and views to be sorted alphabetically
	sort.SliceStable(s.tables, func(i, j int) bool {
		return s.tables[i].Name() < s.tables[j].Name()
	})
	sort.SliceStable(s.views, func(i, j int) bool {
		return s.views[i].Name() < s.views[j].Name()
	})

	// More importantly, we want tables and views to be sorted in applicable order.
	// For example, if a view v reads from table t, then t must be defined before v.
	// We actually prioritise all tables first, then views.
	// If a view v1 depends on v2, then v2 must come before v1, even though v1
	// precedes v2 alphabetically
	dependencyLevels := make(map[string]int, len(s.tables)+len(s.views))

	allNamesFoundInLowerLevel := func(names []string, level int) bool {
		for _, name := range names {
			dependencyLevel, ok := dependencyLevels[name]
			if !ok {
				if strings.ToLower(name) != "dual" {
					// named table is not yet handled. This means this view cannot be defined yet.
					return false
				}
			}
			if dependencyLevel >= level {
				// named table/view is in same dependency level as this view; we want to postpone this
				// view for s future iteration because we want to first maintain alphabetical ordering.
				return false
			}
		}
		return true
	}

	// Utility map and function to only record one foreign-key error per table. We make this limitation
	// because the search algorithm below could review the same table twice, thus potentially unnecessarily duplicating
	// found errors.
	entityFkErrors := map[string]error{}
	addEntityFkError := func(e Entity, err error) error {
		if _, ok := entityFkErrors[e.Name()]; ok {
			// error already recorded for this entity
			return nil
		}
		entityFkErrors[e.Name()] = err
		return err
	}
	// We now iterate all tables. We iterate "dependency levels":
	// - first we want all tables that don't have foreign keys or which only reference themselves
	// - then we only want tables that reference 1st level tables. these are 2nd level tables
	// - etc.
	// we stop when we have been unable to find a table in an iteration.
	fkParents := map[string]bool{}
	iterationLevel := 0
	for {
		handledAnyTablesInIteration := false
		for _, t := range s.tables {
			name := t.Name()
			if _, ok := dependencyLevels[name]; ok {
				// already handled; skip
				continue
			}
			// Not handled. Is this view dependent on already handled objects?
			referencedTableNames := getForeignKeyParentTableNames(t.CreateTable)
			if len(referencedTableNames) > 0 {
				s.foreignKeyChildren = append(s.foreignKeyChildren, t)
			}
			nonSelfReferenceNames := []string{}
			for _, referencedTableName := range referencedTableNames {
				if referencedTableName != name {
					nonSelfReferenceNames = append(nonSelfReferenceNames, referencedTableName)
				}
				referencedEntity, ok := s.named[referencedTableName]
				if !ok {
					errs = errors.Join(errs, addEntityFkError(t, &ForeignKeyNonexistentReferencedTableError{Table: name, ReferencedTable: referencedTableName}))
					continue
				}
				if _, ok := referencedEntity.(*CreateViewEntity); ok {
					errs = errors.Join(errs, addEntityFkError(t, &ForeignKeyReferencesViewError{Table: name, ReferencedView: referencedTableName}))
					continue
				}

				fkParents[referencedTableName] = true
			}
			if allNamesFoundInLowerLevel(nonSelfReferenceNames, iterationLevel) {
				s.sorted = append(s.sorted, t)
				dependencyLevels[t.Name()] = iterationLevel
				handledAnyTablesInIteration = true
			}
		}
		if !handledAnyTablesInIteration {
			break
		}
		iterationLevel++
	}
	for _, t := range s.tables {
		if fkParents[t.Name()] {
			s.foreignKeyParents = append(s.foreignKeyParents, t)
		}
	}
	// We now iterate all views. We iterate "dependency levels":
	// - first we want all views that only depend on tables. These are 1st level views.
	// - then we only want views that depend on 1st level views or on tables. These are 2nd level views.
	// - etc.
	// we stop when we have been unable to find a view in an iteration.

	// It's possible that there's never been any tables in this schema. Which means
	// iterationLevel remains zero.
	// To deal with views, we must have iterationLevel at least 1. This is because any view reads
	// from _something_: at the very least it reads from DUAL (implicitly or explicitly). Which
	// puts the view at a higher level.
	if iterationLevel < 1 {
		iterationLevel = 1
	}
	for {
		handledAnyViewsInIteration := false
		for _, v := range s.views {
			name := v.Name()
			if _, ok := dependencyLevels[name]; ok {
				// already handled; skip
				continue
			}
			// Not handled. Is this view dependent on already handled objects?
			dependentNames := getViewDependentTableNames(v.CreateView)
			if allNamesFoundInLowerLevel(dependentNames, iterationLevel) {
				s.sorted = append(s.sorted, v)
				dependencyLevels[v.Name()] = iterationLevel
				handledAnyViewsInIteration = true
			}
		}
		if !handledAnyViewsInIteration {
			break
		}
		iterationLevel++
	}

	if len(s.sorted) != len(s.tables)+len(s.views) {

		for _, t := range s.tables {
			if _, ok := dependencyLevels[t.Name()]; !ok {
				if loop := s.findForeignKeyLoop(t.Name(), nil); loop != nil {
					errs = errors.Join(errs, addEntityFkError(t, &ForeignKeyLoopError{Table: t.Name(), Loop: loop}))
				}
			}
		}
		// We have leftover tables or views. This can happen if the schema definition is invalid:
		// - a table's foreign key references a nonexistent table
		// - two or more tables have circular FK dependency
		// - a view depends on a nonexistent table
		// - two or more views have a circular dependency
		for _, t := range s.tables {
			if _, ok := dependencyLevels[t.Name()]; !ok {
				// We _know_ that in this iteration, at least one foreign key is not found.
				// We return the first one.
				errs = errors.Join(errs, addEntityFkError(t, &ForeignKeyDependencyUnresolvedError{Table: t.Name()}))
				s.sorted = append(s.sorted, t)
			}
		}
		for _, v := range s.views {
			if _, ok := dependencyLevels[v.Name()]; !ok {
				// We _know_ that in this iteration, at least one view is found unassigned a dependency level.
				// We gather all the errors.
				errs = errors.Join(errs, &ViewDependencyUnresolvedError{View: v.ViewName.Name.String()})
				// We still add it so it shows up in the output if that is used for anything.
				s.sorted = append(s.sorted, v)
			}
		}
	}

	// Validate views' referenced columns: do these columns actually exist in referenced tables/views?
	if err := s.ValidateViewReferences(); err != nil {
		errs = errors.Join(errs, err)
	}

	// Validate table definitions
	for _, t := range s.tables {
		if err := t.validate(); err != nil {
			return errors.Join(errs, err)
		}
	}
	colTypeCompatibleForForeignKey := func(child, parent *sqlparser.ColumnType) bool {
		if child.Type == parent.Type {
			return true
		}
		if child.Type == "char" && parent.Type == "varchar" {
			return true
		}
		if child.Type == "varchar" && parent.Type == "char" {
			return true
		}
		return false
	}
	colTypeEqualForForeignKey := func(child, parent *sqlparser.ColumnType) bool {
		if colTypeCompatibleForForeignKey(child, parent) &&
			child.Unsigned == parent.Unsigned &&
			child.Zerofill == parent.Zerofill &&
			sqlparser.Equals.ColumnCharset(child.Charset, parent.Charset) &&
			child.Options.Collate == parent.Options.Collate &&
			sqlparser.Equals.SliceOfString(child.EnumValues, parent.EnumValues) {
			// Complete identify (other than precision which is ignored)
			return true
		}
		return false
	}

	// Now validate foreign key columns:
	// - referenced table columns must exist
	// - foreign key columns must match in count and type to referenced table columns
	// - referenced table has an appropriate index over referenced columns
	for _, t := range s.tables {
		if len(t.TableSpec.Constraints) == 0 {
			continue
		}

		tableColumns := map[string]*sqlparser.ColumnDefinition{}
		for _, col := range t.CreateTable.TableSpec.Columns {
			colName := col.Name.Lowered()
			tableColumns[colName] = col
		}

		for _, cs := range t.TableSpec.Constraints {
			check, ok := cs.Details.(*sqlparser.ForeignKeyDefinition)
			if !ok {
				continue
			}
			referencedTableName := check.ReferenceDefinition.ReferencedTable.Name.String()
			referencedTable := s.Table(referencedTableName)
			if referencedTable == nil {
				// This can happen because earlier, when we validated existence of reference table, we took note
				// of nonexisting tables, but kept on going.
				continue
			}

			referencedColumns := map[string]*sqlparser.ColumnDefinition{}
			for _, col := range referencedTable.CreateTable.TableSpec.Columns {
				colName := col.Name.Lowered()
				referencedColumns[colName] = col
			}
			// Thanks to table validation, we already know the foreign key covered columns count is equal to the
			// referenced table column count. Now ensure their types are identical
			for i, col := range check.Source {
				coveredColumn, ok := tableColumns[col.Lowered()]
				if !ok {
					return errors.Join(errs, &InvalidColumnInForeignKeyConstraintError{Table: t.Name(), Constraint: cs.Name.String(), Column: col.String()})
				}
				referencedColumnName := check.ReferenceDefinition.ReferencedColumns[i].Lowered()
				referencedColumn, ok := referencedColumns[referencedColumnName]
				if !ok {
					return errors.Join(errs, &InvalidReferencedColumnInForeignKeyConstraintError{Table: t.Name(), Constraint: cs.Name.String(), ReferencedTable: referencedTableName, ReferencedColumn: referencedColumnName})
				}
				if !colTypeEqualForForeignKey(coveredColumn.Type, referencedColumn.Type) {
					return errors.Join(errs, &ForeignKeyColumnTypeMismatchError{Table: t.Name(), Constraint: cs.Name.String(), Column: coveredColumn.Name.String(), ReferencedTable: referencedTableName, ReferencedColumn: referencedColumnName})
				}
			}

			if !referencedTable.columnsCoveredByInOrderIndex(check.ReferenceDefinition.ReferencedColumns) {
				return errors.Join(errs, &MissingForeignKeyReferencedIndexError{Table: t.Name(), Constraint: cs.Name.String(), ReferencedTable: referencedTableName})
			}
		}
	}
	return errs
}

// Entities returns this schema's entities in good order (may be applied without error)
func (s *Schema) Entities() []Entity {
	return s.sorted
}

// EntityNames is a convenience function that returns just the names of entities, in good order
func (s *Schema) EntityNames() []string {
	var names []string
	for _, e := range s.Entities() {
		names = append(names, e.Name())
	}
	return names
}

// Tables returns this schema's tables in good order (may be applied without error)
func (s *Schema) Tables() []*CreateTableEntity {
	var tables []*CreateTableEntity
	for _, entity := range s.sorted {
		if table, ok := entity.(*CreateTableEntity); ok {
			tables = append(tables, table)
		}
	}
	return tables
}

// TableNames is a convenience function that returns just the names of tables, in good order
func (s *Schema) TableNames() []string {
	var names []string
	for _, e := range s.Tables() {
		names = append(names, e.Name())
	}
	return names
}

// Views returns this schema's views in good order (may be applied without error)
func (s *Schema) Views() []*CreateViewEntity {
	var views []*CreateViewEntity
	for _, entity := range s.sorted {
		if view, ok := entity.(*CreateViewEntity); ok {
			views = append(views, view)
		}
	}
	return views
}

// ViewNames is a convenience function that returns just the names of views, in good order
func (s *Schema) ViewNames() []string {
	var names []string
	for _, e := range s.Views() {
		names = append(names, e.Name())
	}
	return names
}

// Diff compares this schema with another schema, and sees what it takes to make this schema look
// like the other. It returns a list of diffs.
func (s *Schema) diff(other *Schema, hints *DiffHints) (diffs []EntityDiff, err error) {
	// dropped entities
	var dropDiffs []EntityDiff
	for _, e := range s.Entities() {
		if _, ok := other.named[e.Name()]; !ok {
			// other schema does not have the entity
			// Entities are sorted in foreign key CREATE TABLE valid order (create parents first, then children).
			// When issuing DROPs, we want to reverse that order. We want to first do it for children, then parents.
			// Instead of analyzing all relationships again, we just reverse the entire order of DROPs, foreign key
			// related or not.
			dropDiffs = append([]EntityDiff{e.Drop()}, dropDiffs...)
		}
	}
	// We iterate by order of "other" schema because we need to construct queries that will be valid
	// for that schema (we need to maintain view dependencies according to target, not according to source)
	var alterDiffs []EntityDiff
	var createDiffs []EntityDiff
	for _, e := range other.Entities() {
		if fromEntity, ok := s.named[e.Name()]; ok {
			// entities exist by same name in both schemas. Let's diff them.
			diff, err := fromEntity.Diff(e, hints)

			switch {
			case err != nil && errors.Is(err, ErrEntityTypeMismatch):
				// e.g. comparing a table with a view
				// there's no single "diff", ie no single ALTER statement to convert from one to another,
				// hence the error.
				// But in our schema context, we know better. We know we should DROP the one, CREATE the other.
				// We proceed to do that, and implicitly ignore the error
				dropDiffs = append(dropDiffs, fromEntity.Drop())
				createDiffs = append(createDiffs, e.Create())
				// And we're good. We can move on to comparing next entity.
			case err != nil:
				// Any other kind of error
				return nil, err
			default:
				// No error, let's check the diff:
				if diff != nil && !diff.IsEmpty() {
					alterDiffs = append(alterDiffs, diff)
				}
			}
		} else { // !ok
			// Added entity
			// this schema does not have the entity
			createDiffs = append(createDiffs, e.Create())
		}
	}
	dropDiffs, createDiffs, renameDiffs := s.heuristicallyDetectTableRenames(dropDiffs, createDiffs, hints)
	diffs = append(diffs, dropDiffs...)
	diffs = append(diffs, alterDiffs...)
	diffs = append(diffs, createDiffs...)
	diffs = append(diffs, renameDiffs...)

	return diffs, err
}

func (s *Schema) heuristicallyDetectTableRenames(
	dropDiffs []EntityDiff,
	createDiffs []EntityDiff,
	hints *DiffHints,
) (
	updatedDropDiffs []EntityDiff,
	updatedCreateDiffs []EntityDiff,
	renameDiffs []EntityDiff,
) {
	renameDiffs = []EntityDiff{}

	findRenamedTable := func() bool {
		// What we're doing next is to try and identify a table RENAME.
		// We do so by cross-referencing dropped and created tables.
		// The check is heuristic, and looks like this:
		// We consider a table renamed iff:
		// - the DROP and CREATE table definitions are identical other than the table name
		// In the case where multiple dropped tables have identical schema, and likewise multiple created tables
		// have identical schemas, schemadiff makes an arbitrary match.
		// Once we heuristically decide that we found a RENAME, we cancel the DROP,
		// cancel the CREATE, and inject a RENAME in place of both.

		// findRenamedTable cross-references dropped and created tables to find a single renamed table. If such is found:
		// we remove the entry from DROPped tables, remove the entry from CREATEd tables, add an entry for RENAMEd tables,
		// and return 'true'.
		// Successive calls to this function will then find the next heuristic RENAMEs.
		// the function returns 'false' if it is unable to heuristically find a RENAME.
		for iDrop, drop1 := range dropDiffs {
			for iCreate, create2 := range createDiffs {
				dropTableDiff, ok := drop1.(*DropTableEntityDiff)
				if !ok {
					continue
				}
				createTableDiff, ok := create2.(*CreateTableEntityDiff)
				if !ok {
					continue
				}
				if !dropTableDiff.from.identicalOtherThanName(createTableDiff.to) {
					continue
				}
				// Yes, it looks like those tables have the exact same spec, just with different names.
				dropDiffs = append(dropDiffs[0:iDrop], dropDiffs[iDrop+1:]...)
				createDiffs = append(createDiffs[0:iCreate], createDiffs[iCreate+1:]...)
				renameTable := &sqlparser.RenameTable{
					TablePairs: []*sqlparser.RenameTablePair{
						{FromTable: dropTableDiff.from.Table, ToTable: createTableDiff.to.Table},
					},
				}
				renameTableEntityDiff := &RenameTableEntityDiff{
					from:        dropTableDiff.from,
					to:          createTableDiff.to,
					renameTable: renameTable,
				}
				renameDiffs = append(renameDiffs, renameTableEntityDiff)
				return true
			}
		}
		return false
	}
	switch hints.TableRenameStrategy {
	case TableRenameAssumeDifferent:
		// do nothing
	case TableRenameHeuristicStatement:
		for findRenamedTable() {
			// Iteratively detect all RENAMEs
		}
	}

	return dropDiffs, createDiffs, renameDiffs
}

// Entity returns an entity by name, or nil if nonexistent
func (s *Schema) Entity(name string) Entity {
	return s.named[name]
}

// Table returns a table by name, or nil if nonexistent
func (s *Schema) Table(name string) *CreateTableEntity {
	if table, ok := s.named[name].(*CreateTableEntity); ok {
		return table
	}
	return nil
}

// View returns a view by name, or nil if nonexistent
func (s *Schema) View(name string) *CreateViewEntity {
	if view, ok := s.named[name].(*CreateViewEntity); ok {
		return view
	}
	return nil
}

// ToStatements returns an ordered list of statements which can be applied to create the schema
func (s *Schema) ToStatements() []sqlparser.Statement {
	stmts := make([]sqlparser.Statement, 0, len(s.Entities()))
	for _, e := range s.Entities() {
		stmts = append(stmts, e.Create().Statement())
	}
	return stmts
}

// ToQueries returns an ordered list of queries which can be applied to create the schema
func (s *Schema) ToQueries() []string {
	queries := make([]string, 0, len(s.Entities()))
	for _, e := range s.Entities() {
		queries = append(queries, e.Create().CanonicalStatementString())
	}
	return queries
}

// ToSQL returns a SQL blob with ordered sequence of queries which can be applied to create the schema
func (s *Schema) ToSQL() string {
	var buf strings.Builder
	for _, query := range s.ToQueries() {
		buf.WriteString(query)
		buf.WriteString(";\n")
	}
	return buf.String()
}

// copy returns a shallow copy of the schema. This is used when applying changes for example.
// applying changes will ensure we copy new entities themselves separately.
func (s *Schema) copy() *Schema {
	dup := newEmptySchema(s.env)
	dup.tables = make([]*CreateTableEntity, len(s.tables))
	copy(dup.tables, s.tables)
	dup.views = make([]*CreateViewEntity, len(s.views))
	copy(dup.views, s.views)
	dup.named = make(map[string]Entity, len(s.named))
	for k, v := range s.named {
		dup.named[k] = v
	}
	dup.sorted = make([]Entity, len(s.sorted))
	copy(dup.sorted, s.sorted)
	return dup
}

// apply attempts to apply given list of diffs to this object.
// These diffs are CREATE/DROP/ALTER TABLE/VIEW.
func (s *Schema) apply(diffs []EntityDiff) error {
	for _, diff := range diffs {
		switch diff := diff.(type) {
		case *CreateTableEntityDiff:
			// We expect the table to not exist
			name := diff.createTable.Table.Name.String()
			if _, ok := s.named[name]; ok {
				return &ApplyDuplicateEntityError{Entity: name}
			}
			s.tables = append(s.tables, &CreateTableEntity{CreateTable: diff.createTable})
			_, s.named[name] = diff.Entities()
		case *CreateViewEntityDiff:
			// We expect the view to not exist
			name := diff.createView.ViewName.Name.String()
			if _, ok := s.named[name]; ok {
				return &ApplyDuplicateEntityError{Entity: name}
			}
			s.views = append(s.views, &CreateViewEntity{CreateView: diff.createView})
			_, s.named[name] = diff.Entities()
		case *DropTableEntityDiff:
			// We expect the table to exist
			found := false
			for i, t := range s.tables {
				if name := t.Table.Name.String(); name == diff.from.Table.Name.String() {
					s.tables = append(s.tables[0:i], s.tables[i+1:]...)
					delete(s.named, name)
					found = true
					break
				}
			}
			if !found {
				return &ApplyTableNotFoundError{Table: diff.from.Table.Name.String()}
			}
		case *DropViewEntityDiff:
			// We expect the view to exist
			found := false
			for i, v := range s.views {
				if name := v.ViewName.Name.String(); name == diff.from.ViewName.Name.String() {
					s.views = append(s.views[0:i], s.views[i+1:]...)
					delete(s.named, name)
					found = true
					break
				}
			}
			if !found {
				return &ApplyViewNotFoundError{View: diff.from.ViewName.Name.String()}
			}
		case *AlterTableEntityDiff:
			// We expect the table to exist
			found := false
			for i, t := range s.tables {
				if name := t.Table.Name.String(); name == diff.from.Table.Name.String() {
					to, err := t.Apply(diff)
					if err != nil {
						return err
					}
					toCreateTableEntity, ok := to.(*CreateTableEntity)
					if !ok {
						return ErrEntityTypeMismatch
					}
					s.tables[i] = toCreateTableEntity
					s.named[name] = toCreateTableEntity
					found = true
					break
				}
			}
			if !found {
				return &ApplyTableNotFoundError{Table: diff.from.Table.Name.String()}
			}
		case *AlterViewEntityDiff:
			// We expect the view to exist
			found := false
			for i, v := range s.views {
				if name := v.ViewName.Name.String(); name == diff.from.ViewName.Name.String() {
					to, err := v.Apply(diff)
					if err != nil {
						return err
					}
					toCreateViewEntity, ok := to.(*CreateViewEntity)
					if !ok {
						return ErrEntityTypeMismatch
					}
					s.views[i] = toCreateViewEntity
					s.named[name] = toCreateViewEntity
					found = true
					break
				}
			}
			if !found {
				return &ApplyViewNotFoundError{View: diff.from.ViewName.Name.String()}
			}
		case *RenameTableEntityDiff:
			// We expect the table to exist
			found := false
			for i, t := range s.tables {
				if name := t.Table.Name.String(); name == diff.from.Table.Name.String() {
					s.tables[i] = diff.to
					delete(s.named, name)
					s.named[diff.to.Table.Name.String()] = diff.to
					found = true
					break
				}
			}
			if !found {
				return &ApplyTableNotFoundError{Table: diff.from.Table.Name.String()}
			}
		default:
			return &UnsupportedApplyOperationError{Statement: diff.CanonicalStatementString()}
		}
	}
	if err := s.normalize(); err != nil {
		return err
	}
	return nil
}

// Apply attempts to apply given list of diffs to the schema described by this object.
// These diffs are CREATE/DROP/ALTER TABLE/VIEW.
// The operation does not modify this object. Instead, if successful, a new (modified) Schema is returned.
func (s *Schema) Apply(diffs []EntityDiff) (*Schema, error) {
	dup := s.copy()
	if err := dup.apply(diffs); err != nil {
		return nil, err
	}
	return dup, nil
}

// SchemaDiff calculates a rich diff between this schema and the given schema. It builds on top of diff():
// on top of the list of diffs that can take this schema into the given schema, this function also
// evaluates the dependencies between those diffs, if any, and the resulting SchemaDiff object offers OrderedDiffs(),
// the safe ordering of diffs that, when applied sequentially, does not produce any conflicts and keeps schema valid
// at each step.
func (s *Schema) SchemaDiff(other *Schema, hints *DiffHints) (*SchemaDiff, error) {
	diffs, err := s.diff(other, hints)
	if err != nil {
		return nil, err
	}
	schemaDiff := NewSchemaDiff(s)
	schemaDiff.loadDiffs(diffs)

	// Utility function to see whether the given diff has dependencies on diffs that operate on any of the given named entities,
	// and if so, record that dependency
	checkDependencies := func(diff EntityDiff, dependentNames []string) (dependentDiffs []EntityDiff, relationsMade bool) {
		for _, dependentName := range dependentNames {
			dependentDiffs = schemaDiff.diffsByEntityName(dependentName)
			for _, dependentDiff := range dependentDiffs {
				// 'diff' refers to an entity (call it "e") that has changed. But here we find that one of the
				// entities that "e" depends on, has also changed.
				relationsMade = true
				schemaDiff.addDep(diff, dependentDiff, DiffDependencyOrderUnknown)
			}
		}
		return dependentDiffs, relationsMade
	}

	checkChildForeignKeyDefinition := func(fk *sqlparser.ForeignKeyDefinition, diff EntityDiff) (bool, error) {
		// We add a foreign key. Normally that's fine, expect for a couple specific scenarios
		parentTableName := fk.ReferenceDefinition.ReferencedTable.Name.String()
		dependentDiffs, ok := checkDependencies(diff, []string{parentTableName})
		if !ok {
			// No dependency. Not interesting
			return true, nil
		}
		for _, parentDiff := range dependentDiffs {
			switch parentDiff := parentDiff.(type) {
			case *CreateTableEntityDiff:
				// We add a foreign key constraint onto a new table... That table must therefore be first created,
				// and only then can we proceed to add the FK
				schemaDiff.addDep(diff, parentDiff, DiffDependencySequentialExecution)
			case *AlterTableEntityDiff:
				// The current diff is ALTER TABLE ... ADD FOREIGN KEY, or it is a CREATE TABLE with a FOREIGN KEY
				// and the parent table also has an ALTER TABLE.
				// so if the parent's ALTER in any way modifies the referenced FK columns, that's
				// a sequential execution dependency.
				// Also, if there is no index on the parent's referenced columns, and a migration adds an index
				// on those columns, that's a sequential execution dependency.
				referencedColumnNames := map[string]bool{}
				for _, referencedColumn := range fk.ReferenceDefinition.ReferencedColumns {
					referencedColumnNames[referencedColumn.Lowered()] = true
				}
				// Walk parentDiff.Statement()
				_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
					switch node := node.(type) {
					case *sqlparser.ModifyColumn:
						if referencedColumnNames[node.NewColDefinition.Name.Lowered()] {
							schemaDiff.addDep(diff, parentDiff, DiffDependencySequentialExecution)
						}
					case *sqlparser.AddColumns:
						for _, col := range node.Columns {
							if referencedColumnNames[col.Name.Lowered()] {
								schemaDiff.addDep(diff, parentDiff, DiffDependencySequentialExecution)
							}
						}
					case *sqlparser.DropColumn:
						if referencedColumnNames[node.Name.Name.Lowered()] {
							schemaDiff.addDep(diff, parentDiff, DiffDependencySequentialExecution)
						}
					case *sqlparser.AddIndexDefinition:
						referencedTableEntity, _ := parentDiff.Entities()
						// We _know_ the type is *CreateTableEntity
						referencedTable, _ := referencedTableEntity.(*CreateTableEntity)
						if indexCoversColumnsInOrder(node.IndexDefinition, fk.ReferenceDefinition.ReferencedColumns) {
							// This diff adds an index covering referenced columns
							if !referencedTable.columnsCoveredByInOrderIndex(fk.ReferenceDefinition.ReferencedColumns) {
								// And there was no earlier index on referenced columns. So this is a new index.
								// In MySQL, you can't add a foreign key constraint on a child, before the parent
								// has an index of referenced columns. This is a sequential dependency.
								schemaDiff.addDep(diff, parentDiff, DiffDependencySequentialExecution)
							}
						}
					}
					return true, nil
				}, parentDiff.Statement())
			}
		}
		return true, nil
	}

	for _, diff := range schemaDiff.UnorderedDiffs() {
		switch diff := diff.(type) {
		case *CreateViewEntityDiff:
			checkDependencies(diff, getViewDependentTableNames(diff.createView))
		case *AlterViewEntityDiff:
			checkDependencies(diff, getViewDependentTableNames(diff.from.CreateView))
			checkDependencies(diff, getViewDependentTableNames(diff.to.CreateView))
		case *DropViewEntityDiff:
			checkDependencies(diff, getViewDependentTableNames(diff.from.CreateView))
		case *CreateTableEntityDiff:
			checkDependencies(diff, getForeignKeyParentTableNames(diff.CreateTable()))
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ConstraintDefinition:
					// Only interested in a foreign key
					fk, ok := node.Details.(*sqlparser.ForeignKeyDefinition)
					if !ok {
						return true, nil
					}
					return checkChildForeignKeyDefinition(fk, diff)
				}
				return true, nil
			}, diff.Statement())

		case *AlterTableEntityDiff:
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.AddConstraintDefinition:
					// Only interested in adding a foreign key
					fk, ok := node.ConstraintDefinition.Details.(*sqlparser.ForeignKeyDefinition)
					if !ok {
						return true, nil
					}
					return checkChildForeignKeyDefinition(fk, diff)
				case *sqlparser.DropKey:
					if node.Type != sqlparser.ForeignKeyType {
						// Not interesting
						return true, nil
					}
					// Dropping a foreign key; we need to understand which table this foreign key used to reference.
					// The DropKey statement itself only _names_ the constraint, but does not have information
					// about the parent, columns, etc. So we need to find the constraint in the CreateTable statement.
					for _, cs := range diff.from.CreateTable.TableSpec.Constraints {
						if strings.EqualFold(cs.Name.String(), node.Name.String()) {
							if check, ok := cs.Details.(*sqlparser.ForeignKeyDefinition); ok {
								parentTableName := check.ReferenceDefinition.ReferencedTable.Name.String()
								checkDependencies(diff, []string{parentTableName})
							}
						}
					}
				}

				return true, nil
			}, diff.Statement())
		case *DropTableEntityDiff:
			// No need to handle. Any dependencies will be resolved by any of the other cases
		}
	}

	// Check and assign capabilities:
	// Reminder: schemadiff assumes a MySQL flavor, so we only check for MySQL capabilities.
	if capableOf := capabilities.MySQLVersionCapableOf(s.env.MySQLVersion()); capableOf != nil {
		for _, diff := range schemaDiff.UnorderedDiffs() {
			switch diff := diff.(type) {
			case *AlterTableEntityDiff:
				instantDDLCapable, err := AlterTableCapableOfInstantDDL(diff.AlterTable(), diff.from.CreateTable, capableOf)
				if err != nil {
					return nil, err
				}
				if instantDDLCapable {
					diff.instantDDLCapability = InstantDDLCapabilityPossible
				} else {
					diff.instantDDLCapability = InstantDDLCapabilityImpossible
				}
			}
		}
	}
	return schemaDiff, nil
}

func (s *Schema) ValidateViewReferences() error {
	var errs error
	schemaInformation := newDeclarativeSchemaInformation(s.env)

	// Remember that s.Entities() is already ordered by dependency. ie. tables first, then views
	// that only depend on those tables (or on dual), then 2nd tier views, etc.
	// Thus, the order of iteration below is valid and sufficient, to build
	for _, e := range s.Entities() {
		entityColumns, err := s.getEntityColumnNames(e.Name(), schemaInformation)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		schemaInformation.addTable(e.Name())
		for _, col := range entityColumns {
			schemaInformation.addColumn(e.Name(), col.Lowered())
		}
	}

	// Add dual table with no explicit columns for dual style expressions in views.
	schemaInformation.addTable("dual")

	for _, view := range s.Views() {
		sel := sqlparser.CloneSelectStatement(view.CreateView.Select) // Analyze(), below, rewrites the select; we don't want to actually modify the schema
		_, err := semantics.AnalyzeStrict(sel, semanticKS.Name, schemaInformation)
		formalizeErr := func(err error) error {
			if err == nil {
				return nil
			}
			switch e := err.(type) {
			case *semantics.AmbiguousColumnError:
				return &InvalidColumnReferencedInViewError{
					View:      view.Name(),
					Column:    e.Column,
					Ambiguous: true,
				}
			case semantics.ColumnNotFoundError:
				return &InvalidColumnReferencedInViewError{
					View:   view.Name(),
					Column: e.Column.Name.String(),
				}
			}
			return err
		}
		errs = errors.Join(errs, formalizeErr(err))
	}
	return errs
}

// getEntityColumnNames returns the names of columns in given entity (either a table or a view)
func (s *Schema) getEntityColumnNames(entityName string, schemaInformation *declarativeSchemaInformation) (
	columnNames []*sqlparser.IdentifierCI,
	err error,
) {
	entity := s.Entity(entityName)
	if entity == nil {
		if strings.ToLower(entityName) == "dual" {
			// this is fine. DUAL does not exist but is allowed
			return nil, nil
		}
		return nil, &EntityNotFoundError{Name: entityName}
	}
	// The entity is either a table or a view
	switch entity := entity.(type) {
	case *CreateTableEntity:
		return s.getTableColumnNames(entity), nil
	case *CreateViewEntity:
		return s.getViewColumnNames(entity, schemaInformation)
	}
	return nil, &UnsupportedEntityError{Entity: entity.Name(), Statement: entity.Create().CanonicalStatementString()}
}

// getTableColumnNames returns the names of columns in given table.
func (s *Schema) getTableColumnNames(t *CreateTableEntity) (columnNames []*sqlparser.IdentifierCI) {
	for _, c := range t.TableSpec.Columns {
		columnNames = append(columnNames, &c.Name)
	}
	return columnNames
}

// getViewColumnNames returns the names of aliased columns returned by a given view.
func (s *Schema) getViewColumnNames(v *CreateViewEntity, schemaInformation *declarativeSchemaInformation) ([]*sqlparser.IdentifierCI, error) {
	var columnNames []*sqlparser.IdentifierCI
	for _, node := range v.Select.GetColumns() {
		switch node := node.(type) {
		case *sqlparser.StarExpr:
			if tableName := node.TableName.Name.String(); tableName != "" {
				for _, col := range schemaInformation.Tables[tableName].Columns {
					name := sqlparser.CloneRefOfIdentifierCI(&col.Name)
					columnNames = append(columnNames, name)
				}
			} else {
				dependentNames := getViewDependentTableNames(v.CreateView)
				// add all columns from all referenced tables and views
				for _, entityName := range dependentNames {
					if schemaInformation.Tables[entityName] != nil { // is nil for dual/DUAL
						for _, col := range schemaInformation.Tables[entityName].Columns {
							name := sqlparser.CloneRefOfIdentifierCI(&col.Name)
							columnNames = append(columnNames, name)
						}
					}
				}
			}
			if len(columnNames) == 0 {
				return nil, &InvalidStarExprInViewError{View: v.Name()}
			}
		case *sqlparser.AliasedExpr:
			ci := sqlparser.NewIdentifierCI(node.ColumnName())
			columnNames = append(columnNames, &ci)
		}
	}

	return columnNames, nil
}
