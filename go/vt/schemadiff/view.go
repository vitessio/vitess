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

import "vitess.io/vitess/go/vt/sqlparser"

//
type AlterViewEntityDiff struct {
	alterView *sqlparser.AlterView
}

func (d *AlterViewEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

func (d *AlterViewEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.alterView
}

//
type CreateViewEntityDiff struct {
	createView *sqlparser.CreateView
}

func (d *CreateViewEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

func (d *CreateViewEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.createView
}

//
type DropViewEntityDiff struct {
	dropView *sqlparser.DropView
}

func (d *DropViewEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

func (d *DropViewEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.dropView
}

//
type CreateViewEntity struct {
	sqlparser.CreateView
}

func NewCreateViewEntity(c *sqlparser.CreateView) *CreateViewEntity {
	return &CreateViewEntity{CreateView: *c}
}

func (c *CreateViewEntity) Format() string {
	return sqlparser.String(&c.CreateView)
}

// Diff implements Entity interface function
func (c *CreateViewEntity) Diff(other Entity, hints *DiffHints) (EntityDiff, error) {
	otherCreateView, ok := other.(*CreateViewEntity)
	if !ok {
		return nil, ErrEntityTypeMismatch
	}
	return c.ViewDiff(otherCreateView, hints)
}

// Diff compares this view statement with another view statement, and sees what it takes to
// change this view to look like the other view.
// It returns an AlterView statement if changes are found, or nil if not.
// the other view may be of different name; its name is ignored.
func (c *CreateViewEntity) ViewDiff(other *CreateViewEntity, hints *DiffHints) (*AlterViewEntityDiff, error) {
	otherStmt := other.CreateView
	otherStmt.ViewName = c.CreateView.ViewName

	if !c.CreateView.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	if !otherStmt.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}

	format := sqlparser.String(&c.CreateView)
	otherFormat := sqlparser.String(&otherStmt)
	if format == otherFormat {
		return nil, nil
	}

	alterView := &sqlparser.AlterView{
		ViewName:    otherStmt.ViewName,
		Algorithm:   otherStmt.Algorithm,
		Definer:     otherStmt.Definer,
		Security:    otherStmt.Security,
		Columns:     otherStmt.Columns,
		Select:      otherStmt.Select,
		CheckOption: otherStmt.CheckOption,
	}
	return &AlterViewEntityDiff{alterView: alterView}, nil
}
