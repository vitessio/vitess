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
	"io"
	"sort"

	"github.com/pkg/errors"

	"vitess.io/vitess/go/vt/sqlparser"
)

//
type Schema struct {
	tables []*CreateTableEntity
	views  []*CreateViewEntity

	named  map[string]Entity
	sorted []Entity
}

func NewSchema() *Schema {
	schema := &Schema{
		tables: []*CreateTableEntity{},
		views:  []*CreateViewEntity{},
		named:  map[string]Entity{},
		sorted: []Entity{},
	}
	return schema
}

func NewSchemaFromEntities(entities []Entity) (*Schema, error) {
	schema := NewSchema()
	for _, e := range entities {
		switch c := e.(type) {
		case *CreateTableEntity:
			schema.tables = append(schema.tables, c)
		case *CreateViewEntity:
			schema.views = append(schema.views, c)
		default:
			return nil, ErrUnsupportedEntity
		}
	}
	if err := schema.normalize(); err != nil {
		return nil, err
	}
	return schema, nil
}

func NewSchemaFromStatements(statements []sqlparser.Statement) (*Schema, error) {
	entities := []Entity{}
	for _, s := range statements {
		switch stmt := s.(type) {
		case *sqlparser.CreateTable:
			entities = append(entities, NewCreateTableEntity(stmt))
		case *sqlparser.CreateView:
			entities = append(entities, NewCreateViewEntity(stmt))
		default:
			return nil, errors.Wrap(ErrUnsupportedStatement, sqlparser.String(s))
		}
	}
	return NewSchemaFromEntities(entities)
}

func NewSchemaFromQueries(queries []string) (*Schema, error) {
	statements := []sqlparser.Statement{}
	for _, q := range queries {
		stmt, err := sqlparser.Parse(q)
		if err != nil {
			return nil, err
		}
		statements = append(statements, stmt)
	}
	return NewSchemaFromStatements(statements)
}

func NewSchemaFromSQL(sql string) (*Schema, error) {
	statements := []sqlparser.Statement{}
	tokenizer := sqlparser.NewStringTokenizer(sql)
	for {
		stmt, err := sqlparser.ParseNext(tokenizer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Wrapf(err, "could not parse statement in schema snapshot")
		}
		statements = append(statements, stmt)
	}
	return NewSchemaFromStatements(statements)
}

func (s *Schema) normalize() error {
	// Verify no two entities share same name
	for _, t := range s.tables {
		name := t.Name()
		if _, ok := s.named[name]; ok {
			return errors.Wrap(ErrDuplicateName, name)
		}
		s.named[name] = t
	}
	for _, v := range s.views {
		name := v.Name()
		if _, ok := s.named[name]; ok {
			return errors.Wrap(ErrDuplicateName, name)
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
	handledNames := map[string]bool{}
	for _, t := range s.tables {
		s.sorted = append(s.sorted, t)
		handledNames[t.Name()] = true
	}

	handledViewsInItration := -1
	for handledViewsInItration != 0 {
		for _, v := range s.views {
			name := v.Name()
			if handledNames[name] {
				// already handled; skip
				continue
			}
			// Not handled. Is this view dependant on already handled objects?
			allDependenciesHandled := true
			for _, fromTable := range v.GetFromTables() {
				if !handledNames[fromTable.Name.String()] {
					// "from" table is not yet handled. This means this view cannot be defined yet.
					allDependenciesHandled = false
					break
				}
			}
			if allDependenciesHandled {
				s.sorted = append(s.sorted, v)
				handledNames[v.Name()] = true
				handledViewsInItration++
			}
		}
	}
	if len(s.sorted) != len(s.tables)+len(s.views) {
		return ErrViewDependencyLoop
	}
	return nil
}

func (s *Schema) Entities() []Entity {
	return s.sorted
}

func (s *Schema) EntityNames() []string {
	names := []string{}
	for _, e := range s.Entities() {
		names = append(names, e.Name())
	}
	return names
}
