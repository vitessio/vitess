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
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type AlterViewEntityDiff struct {
	from      *CreateViewEntity
	to        *CreateViewEntity
	alterView *sqlparser.AlterView
}

// IsEmpty implements EntityDiff
func (d *AlterViewEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *AlterViewEntityDiff) Entities() (from Entity, to Entity) {
	return d.from, d.to
}

// Statement implements EntityDiff
func (d *AlterViewEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.alterView
}

// AlterView returns the underlying sqlparser.AlterView that was generated for the diff.
func (d *AlterViewEntityDiff) AlterView() *sqlparser.AlterView {
	if d == nil {
		return nil
	}
	return d.alterView
}

// StatementString implements EntityDiff
func (d *AlterViewEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// CanonicalStatementString implements EntityDiff
func (d *AlterViewEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *AlterViewEntityDiff) SubsequentDiff() EntityDiff {
	return nil
}

// SetSubsequentDiff implements EntityDiff
func (d *AlterViewEntityDiff) SetSubsequentDiff(EntityDiff) {
}

type CreateViewEntityDiff struct {
	createView *sqlparser.CreateView
}

// IsEmpty implements EntityDiff
func (d *CreateViewEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *CreateViewEntityDiff) Entities() (from Entity, to Entity) {
	return nil, &CreateViewEntity{CreateView: d.createView}
}

// Statement implements EntityDiff
func (d *CreateViewEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.createView
}

// CreateView returns the underlying sqlparser.CreateView that was generated for the diff.
func (d *CreateViewEntityDiff) CreateView() *sqlparser.CreateView {
	if d == nil {
		return nil
	}
	return d.createView
}

// StatementString implements EntityDiff
func (d *CreateViewEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// CanonicalStatementString implements EntityDiff
func (d *CreateViewEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *CreateViewEntityDiff) SubsequentDiff() EntityDiff {
	return nil
}

// SetSubsequentDiff implements EntityDiff
func (d *CreateViewEntityDiff) SetSubsequentDiff(EntityDiff) {
}

type DropViewEntityDiff struct {
	from     *CreateViewEntity
	dropView *sqlparser.DropView
}

// IsEmpty implements EntityDiff
func (d *DropViewEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *DropViewEntityDiff) Entities() (from Entity, to Entity) {
	return d.from, nil
}

// Statement implements EntityDiff
func (d *DropViewEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.dropView
}

// DropView returns the underlying sqlparser.DropView that was generated for the diff.
func (d *DropViewEntityDiff) DropView() *sqlparser.DropView {
	if d == nil {
		return nil
	}
	return d.dropView
}

// CanonicalStatementString implements EntityDiff
func (d *DropViewEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// StatementString implements EntityDiff
func (d *DropViewEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *DropViewEntityDiff) SubsequentDiff() EntityDiff {
	return nil
}

// SetSubsequentDiff implements EntityDiff
func (d *DropViewEntityDiff) SetSubsequentDiff(EntityDiff) {
}

// CreateViewEntity stands for a VIEW construct. It contains the view's CREATE statement.
type CreateViewEntity struct {
	*sqlparser.CreateView
}

func NewCreateViewEntity(c *sqlparser.CreateView) (*CreateViewEntity, error) {
	if !c.IsFullyParsed() {
		return nil, &NotFullyParsedError{Entity: c.ViewName.Name.String(), Statement: sqlparser.CanonicalString(c)}
	}
	entity := &CreateViewEntity{CreateView: c}
	entity.normalize()
	return entity, nil
}

func (c *CreateViewEntity) normalize() {
	// Drop the default algorithm
	if strings.EqualFold(c.CreateView.Algorithm, "undefined") {
		c.CreateView.Algorithm = ""
	}
	// Drop the default security model
	if strings.EqualFold(c.CreateView.Security, "definer") {
		c.CreateView.Security = ""
	}
}

// Name implements Entity interface
func (c *CreateViewEntity) Name() string {
	return c.CreateView.GetTable().Name.String()
}

// Diff implements Entity interface function
func (c *CreateViewEntity) Diff(other Entity, hints *DiffHints) (EntityDiff, error) {
	otherCreateView, ok := other.(*CreateViewEntity)
	if !ok {
		return nil, ErrEntityTypeMismatch
	}
	return c.ViewDiff(otherCreateView, hints)
}

// ViewDiff compares this view statement with another view statement, and sees what it takes to
// change this view to look like the other view.
// It returns an AlterView statement if changes are found, or nil if not.
// the other view may be of different name; its name is ignored.
func (c *CreateViewEntity) ViewDiff(other *CreateViewEntity, _ *DiffHints) (*AlterViewEntityDiff, error) {
	if !c.IsFullyParsed() {
		return nil, &NotFullyParsedError{Entity: c.Name(), Statement: sqlparser.CanonicalString(c.CreateView)}
	}
	if !other.CreateView.IsFullyParsed() {
		return nil, &NotFullyParsedError{Entity: c.Name(), Statement: sqlparser.CanonicalString(other.CreateView)}
	}

	if c.identicalOtherThanName(other) {
		return nil, nil
	}

	alterView := &sqlparser.AlterView{
		ViewName:    c.CreateView.ViewName,
		Algorithm:   other.CreateView.Algorithm,
		Definer:     other.CreateView.Definer,
		Security:    other.CreateView.Security,
		Columns:     other.CreateView.Columns,
		Select:      other.CreateView.Select,
		CheckOption: other.CreateView.CheckOption,
	}
	return &AlterViewEntityDiff{alterView: alterView, from: c, to: other}, nil
}

// Create implements Entity interface
func (c *CreateViewEntity) Create() EntityDiff {
	return &CreateViewEntityDiff{createView: c.CreateView}
}

// Drop implements Entity interface
func (c *CreateViewEntity) Drop() EntityDiff {
	dropView := &sqlparser.DropView{
		FromTables: []sqlparser.TableName{c.ViewName},
	}
	return &DropViewEntityDiff{from: c, dropView: dropView}
}

// apply attempts to apply an ALTER VIEW diff onto this entity's view definition.
// supported modifications are only those created by schemadiff's Diff() function.
func (c *CreateViewEntity) apply(diff *AlterViewEntityDiff) error {
	c.CreateView = diff.to.CreateView
	return nil
}

// Apply attempts to apply given ALTER VIEW diff onto the view defined by this entity.
// This entity is unmodified. If successful, a new CREATE VIEW entity is returned.
func (c *CreateViewEntity) Apply(diff EntityDiff) (Entity, error) {
	alterDiff, ok := diff.(*AlterViewEntityDiff)
	if !ok {
		return nil, ErrEntityTypeMismatch
	}
	dup := c.Clone().(*CreateViewEntity)
	if err := dup.apply(alterDiff); err != nil {
		return nil, err
	}
	dup.normalize()
	return dup, nil
}

func (c *CreateViewEntity) Clone() Entity {
	return &CreateViewEntity{CreateView: sqlparser.CloneRefOfCreateView(c.CreateView)}
}

func (c *CreateViewEntity) identicalOtherThanName(other *CreateViewEntity) bool {
	if other == nil {
		return false
	}
	return c.Algorithm == other.Algorithm &&
		c.Security == other.Security &&
		c.CheckOption == other.CheckOption &&
		c.IsReplace == other.IsReplace &&
		sqlparser.EqualsRefOfDefiner(c.Definer, other.Definer) &&
		sqlparser.EqualsColumns(c.Columns, other.Columns) &&
		sqlparser.EqualsSelectStatement(c.Select, other.Select) &&
		sqlparser.EqualsRefOfParsedComments(c.Comments, other.Comments)
}
