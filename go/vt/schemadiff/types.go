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

	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	ErrEntityTypeMismatch                          = errors.New("mismatched entity type")
	ErrStrictIndexOrderingUnsupported              = errors.New("strict index ordering is unsupported")
	ErrRangeRotattionStatementsStrategyUnsupported = errors.New("range rotation statement strategy unsupported")
	ErrUnsupportedTableOption                      = errors.New("unsupported table option")
	ErrUnexpectedDiffAction                        = errors.New("unexpected diff action")
	ErrUnexpectedTableSpec                         = errors.New("unexpected table spec")
	ErrNotFullyParsed                              = errors.New("unable to fully parse statement")
	ErrExpectedCreateTable                         = errors.New("expected a CREATE TABLE statement")
	ErrExpectedCreateView                          = errors.New("expected a CREATE VIEW statement")
	ErrUnsupportedEntity                           = errors.New("Unsupported entity type")
	ErrUnsupportedStatement                        = errors.New("Unsupported statement")
	ErrDuplicateName                               = errors.New("Duplicate name")
	ErrViewDependencyUnresolved                    = errors.New("Views have unresolved/loop dependencies")

	ErrUnsupportedApplyOperation = errors.New("unsupported Apply operation")
	ErrApplyTableNotFound        = errors.New("table not found")
	ErrApplyViewNotFound         = errors.New("view not found")
	ErrApplyKeyNotFound          = errors.New("key not found")
	ErrApplyColumnNotFound       = errors.New("column not found")
	ErrApplyDuplicateTableOrView = errors.New("duplicate table or view")
	ErrApplyDuplicateKey         = errors.New("duplicate key")
	ErrApplyDuplicateColumn      = errors.New("duplicate column")
	ErrApplyDuplicateConstraint  = errors.New("duplicate constraint")

	ErrInvalidColumnInKey = errors.New("invalid column referenced by key")
)

// Entity stands for a database object we can diff:
// - A table
// - A view
type Entity interface {
	// Name of entity, ie table name, view name, etc.
	Name() string
	// Diff returns an entitty diff given another entity. The diff direction is from this entity and to the other entity.
	Diff(other Entity, hints *DiffHints) (diff EntityDiff, err error)
	// Create returns an entity diff that describes how to create this entity
	Create() EntityDiff
	// Create returns an entity diff that describes how to drop this entity
	Drop() EntityDiff
}

// EntityDiff represents the diff between two entities
type EntityDiff interface {
	// IsEmpty returns true when the two entities are considered identical
	IsEmpty() bool
	// Entities returns the two diffed entitied, aka "from" and "to"
	Entities() (from Entity, to Entity)
	// Statement returns a valid SQL statement that applies the diff, e.g. an ALTER TABLE ...
	// It returns nil if the diff is empty
	Statement() sqlparser.Statement
	// StatementString "stringifies" the this diff's Statement(). It returns an empty string if the diff is empty
	StatementString() string
	// CanonicalStatementString "stringifies" the this diff's Statement() to a canonical string. It returns an empty string if the diff is empty
	CanonicalStatementString() string
}

const (
	AutoIncrementIgnore int = iota
	AutoIncrementApplyHigher
	AutoIncrementApplyAlways
)

const (
	RangeRotationFullSpec = iota
	RangeRotationStatements
	RangeRotationIgnore
)

// DiffHints is an assortment of rules for diffing entities
type DiffHints struct {
	StrictIndexOrdering   bool
	AutoIncrementStrategy int
	RangeRotationStrategy int
}
