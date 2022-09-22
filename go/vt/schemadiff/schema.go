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
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Schema represents a database schema, which may contain entities such as tables and views.
// Schema is not in itself an Entity, since it is more of a collection of entities.
type Schema struct {
	tables []*CreateTableEntity
	views  []*CreateViewEntity

	named  map[string]Entity
	sorted []Entity
}

// newEmptySchema is used internally to initialize a Schema object
func newEmptySchema() *Schema {
	schema := &Schema{
		tables: []*CreateTableEntity{},
		views:  []*CreateViewEntity{},
		named:  map[string]Entity{},
		sorted: []Entity{},
	}
	return schema
}

// NewSchemaFromEntities creates a valid and normalized schema based on list of entities
func NewSchemaFromEntities(entities []Entity) (*Schema, error) {
	schema := newEmptySchema()
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
	if err := schema.normalize(); err != nil {
		return nil, err
	}
	return schema, nil
}

// NewSchemaFromStatements creates a valid and normalized schema based on list of valid statements
func NewSchemaFromStatements(statements []sqlparser.Statement) (*Schema, error) {
	entities := make([]Entity, 0, len(statements))
	for _, s := range statements {
		switch stmt := s.(type) {
		case *sqlparser.CreateTable:
			c, err := NewCreateTableEntity(stmt)
			if err != nil {
				return nil, err
			}
			entities = append(entities, c)
		case *sqlparser.CreateView:
			v, err := NewCreateViewEntity(stmt)
			if err != nil {
				return nil, err
			}
			entities = append(entities, v)
		default:
			return nil, &UnsupportedStatementError{Statement: sqlparser.CanonicalString(s)}
		}
	}
	return NewSchemaFromEntities(entities)
}

// NewSchemaFromQueries creates a valid and normalized schema based on list of queries
func NewSchemaFromQueries(queries []string) (*Schema, error) {
	statements := make([]sqlparser.Statement, 0, len(queries))
	for _, q := range queries {
		stmt, err := sqlparser.ParseStrictDDL(q)
		if err != nil {
			return nil, err
		}
		statements = append(statements, stmt)
	}
	return NewSchemaFromStatements(statements)
}

// NewSchemaFromSQL creates a valid and normalized schema based on a SQL blob that contains
// CREATE statements for various objects (tables, views)
func NewSchemaFromSQL(sql string) (*Schema, error) {
	var statements []sqlparser.Statement
	tokenizer := sqlparser.NewStringTokenizer(sql)
	for {
		stmt, err := sqlparser.ParseNextStrictDDL(tokenizer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("could not parse statement in SQL: %v: %w", sql, err)
		}
		statements = append(statements, stmt)
	}
	return NewSchemaFromStatements(statements)
}

// getViewDependentTableNames analyzes a CREATE VIEW definition and extracts all tables/views read by this view
func getViewDependentTableNames(createView *sqlparser.CreateView) (names []string, err error) {
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
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
	return names, err
}

// normalize is called as part of Schema creation process. The user may only get a hold of normalized schema.
// It validates some cross-entity constraints, and orders entity based on dependencies (e.g. tables, views that read from tables, 2nd level views, etc.)
func (s *Schema) normalize() error {
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
	for _, t := range s.tables {
		s.sorted = append(s.sorted, t)
		dependencyLevels[t.Name()] = 0
	}

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

	// We now iterate all views. We iterate "dependency levels":
	// - first we want all views that only depend on tables. These are 1st level views.
	// - then we only want views that depend on 1st level views or on tables. These are 2nd level views.
	// - etc.
	// we stop when we have been unable to find a view in an iteration.
	for iterationLevel := 1; ; iterationLevel++ {
		handledAnyViewsInIteration := false
		for _, v := range s.views {
			name := v.Name()
			if _, ok := dependencyLevels[name]; ok {
				// already handled; skip
				continue
			}
			// Not handled. Is this view dependent on already handled objects?
			dependentNames, err := getViewDependentTableNames(v.CreateView)
			if err != nil {
				return err
			}
			if allNamesFoundInLowerLevel(dependentNames, iterationLevel) {
				s.sorted = append(s.sorted, v)
				dependencyLevels[v.Name()] = iterationLevel
				handledAnyViewsInIteration = true
			}
		}
		if !handledAnyViewsInIteration {
			break
		}
	}
	if len(s.sorted) != len(s.tables)+len(s.views) {
		// We have leftover views. This can happen if the schema definition is invalid:
		// - a view depends on a nonexistent table
		// - two views have a circular dependency
		for _, v := range s.views {
			if _, ok := dependencyLevels[v.Name()]; !ok {
				// We _know_ that in this iteration, at least one view is found unassigned a dependency level.
				// We return the first one.
				return &ViewDependencyUnresolvedError{View: v.ViewName.Name.String()}
			}
		}
	}
	return nil
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
func (s *Schema) Diff(other *Schema, hints *DiffHints) (diffs []EntityDiff, err error) {
	// dropped entities
	var dropDiffs []EntityDiff
	for _, e := range s.Entities() {
		if _, ok := other.named[e.Name()]; !ok {
			// other schema does not have the entity
			dropDiffs = append(dropDiffs, e.Drop())
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
	var buf bytes.Buffer
	for _, query := range s.ToQueries() {
		buf.WriteString(query)
		buf.WriteString(";\n")
	}
	return buf.String()
}

// copy returns a shallow copy of the schema. This is used when applying changes for example.
// applying changes will ensure we copy new entities themselves separately.
func (s *Schema) copy() *Schema {
	dup := newEmptySchema()
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
	for k, v := range s.named {
		dup.named[k] = v
	}
	if err := dup.apply(diffs); err != nil {
		return nil, err
	}
	return dup, nil
}
