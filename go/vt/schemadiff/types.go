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
	ErrEntityTypeMismatch             = errors.New("mismatched entity type")
	ErrStrictIndexOrderingUnsupported = errors.New("strict index ordering is unsupported")
	ErrPartitioningUnsupported        = errors.New("partitions are unsupported")
	ErrUnsupportedTableOption         = errors.New("unsupported table option")
	ErrUnexpectedDiffAction           = errors.New("unexpected diff action")
	ErrUnexpectedTableSpec            = errors.New("unexpected table spec")
	ErrNotFullyParsed                 = errors.New("unable to fully parse statement")
	ErrExpectedCreateTable            = errors.New("expected a CREATE TABLE statement")
	ErrExpectedCreateView             = errors.New("expected a CREATE VIEW statement")
	ErrUnsupportedEntity              = errors.New("Unsupported entity type")
	ErrUnsupportedStatement           = errors.New("Unsupported statement")
	ErrDuplicateName                  = errors.New("Duplicate name")
	ErrViewDependencyLoop             = errors.New("Views have dependency loop")
)

type Entity interface {
	Name() string
	Diff(other Entity, hints *DiffHints) (diff EntityDiff, err error)
}

type EntityDiff interface {
	IsEmpty() bool
	Statement() sqlparser.Statement
	StatementString() string
}

const (
	AutoIncrementIgnore int = iota
	AutoIncrementApplyHigher
	AutoIncrementApplyAlways
)

type DiffHints struct {
	StrictIndexOrdering   bool
	AutoIncrementStrategy int
}
