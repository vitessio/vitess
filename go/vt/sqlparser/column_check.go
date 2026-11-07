/*
Copyright 2026 The Vitess Authors.

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

package sqlparser

import "fmt"

// A CHECK constraint can be written as an attribute of a column, as in
// `c1 INT CHECK (c1 > 0)`. MySQL treats it as a constraint of the table, as if
// it was written as a separate element at the column's place in the table
// definition. The parser does the same: it holds the constraints of each column
// definition here until the statement adds the column to its table, and then
// adds them to the table in the order in which they were written.
//
// The order matters, because MySQL names an unnamed CHECK constraint after its
// position among the table's CHECK constraints (t1_chk_1, t1_chk_2, ...). The
// formatted statement lists all columns before the table's constraints, so a
// constraint that stayed with its column could change places with a table
// constraint that was written before it, and the two would swap names.

// addColumnCheck records a CHECK constraint among the attributes of the column
// definition that is being parsed. end is where the constraint ends.
func (tkn *Tokenizer) addColumnCheck(name IdentifierCI, expr Expr, end int) {
	tkn.pendingColumnChecks = append(tkn.pendingColumnChecks, &ConstraintDefinition{
		Name:    name,
		Details: &CheckConstraintDefinition{Expr: expr, Enforced: true},
	})
	tkn.columnCheckEnd = end
}

// enforceColumnCheck applies an ENFORCED or NOT ENFORCED attribute to the
// CHECK constraint right before it. attributesEnd is where the attributes that
// precede it end. It reports false when the attribute right before it is not a
// CHECK constraint, which MySQL rejects.
func (tkn *Tokenizer) enforceColumnCheck(enforced bool, attributesEnd int) bool {
	if len(tkn.pendingColumnChecks) == 0 {
		return false
	}
	if tkn.columnCheckEnd != attributesEnd {
		return false
	}
	last := tkn.pendingColumnChecks[len(tkn.pendingColumnChecks)-1]
	last.Details.(*CheckConstraintDefinition).Enforced = enforced
	return true
}

// columnDefaults are the DEFAULT attributes of a column. MySQL keeps a default
// expression, as in DEFAULT (uuid()), apart from a default constant, as in
// DEFAULT 4 or DEFAULT CURRENT_TIMESTAMP, and a later one of either kind
// replaces an earlier one of the same kind. It rejects a column that has both,
// unless the constant is NULL, and the expression wins over NULL.
type columnDefaults struct {
	expression Expr
	literal    Expr
}

// resolve sets the default of opts, or reports false for a column that MySQL
// rejects.
func (defaults *columnDefaults) resolve(opts *ColumnTypeOptions) bool {
	_, literalIsNull := defaults.literal.(*NullVal)
	switch {
	case defaults.expression != nil && defaults.literal != nil && !literalIsNull:
		return false
	case defaults.expression != nil:
		opts.Default = defaults.expression
		opts.DefaultLiteral = false
	case defaults.literal != nil:
		opts.Default = defaults.literal
		opts.DefaultLiteral = true
	}
	return true
}

// endColumnDefinition takes the DEFAULT attributes and the CHECK constraints
// of the column definition that has just been parsed, and holds the
// constraints until the column is added to its table. It returns an error
// message for a default or a constraint that MySQL rejects.
func (tkn *Tokenizer) endColumnDefinition(column *ColumnDefinition) string {
	defaults := tkn.columnDefaults
	tkn.columnDefaults = columnDefaults{}
	if column.Type.Options != nil && !defaults.resolve(column.Type.Options) {
		// MySQL reports ER_INVALID_DEFAULT.
		return fmt.Sprintf("invalid default value for '%s'", column.Name.String())
	}
	checks := tkn.pendingColumnChecks
	tkn.pendingColumnChecks = nil
	if len(checks) == 0 {
		return ""
	}
	for _, check := range checks {
		if referencesOtherColumn(check.Details.(*CheckConstraintDefinition).Expr, column.Name) {
			// MySQL reports ER_COLUMN_CHECK_CONSTRAINT_REFERENCES_OTHER_COLUMN.
			// Moved to the table, the constraint would be accepted.
			return "a column's CHECK constraint can only refer to that column"
		}
	}
	if tkn.columnChecks == nil {
		tkn.columnChecks = make(map[*ColumnDefinition][]*ConstraintDefinition)
	}
	tkn.columnChecks[column] = checks
	return ""
}

func referencesOtherColumn(expr Expr, column IdentifierCI) bool {
	other := false
	_ = Walk(func(node SQLNode) (bool, error) {
		if name, ok := node.(*ColName); ok && !name.Name.Equal(column) {
			other = true
		}
		return !other, nil
	}, expr)
	return other
}

// takeColumnChecks returns the CHECK constraints of a column, and forgets them,
// so that the statement can check that it added the constraints of every
// column.
func (tkn *Tokenizer) takeColumnChecks(column *ColumnDefinition) []*ConstraintDefinition {
	checks := tkn.columnChecks[column]
	delete(tkn.columnChecks, column)
	return checks
}

// addColumnChecksToTable adds the CHECK constraints of a column to the table
// that the column has just been added to.
func (tkn *Tokenizer) addColumnChecksToTable(spec *TableSpec, column *ColumnDefinition) {
	for _, check := range tkn.takeColumnChecks(column) {
		spec.AddConstraint(check)
	}
}

// appendAlterOption appends an ALTER TABLE option, followed by an ADD
// CONSTRAINT option for each CHECK constraint of a column that the option
// defines.
func (tkn *Tokenizer) appendAlterOption(options []AlterOption, option AlterOption) []AlterOption {
	options = append(options, option)
	var columns []*ColumnDefinition
	switch option := option.(type) {
	case *AddColumns:
		columns = option.Columns
	case *ChangeColumn:
		columns = []*ColumnDefinition{option.NewColDefinition}
	case *ModifyColumn:
		columns = []*ColumnDefinition{option.NewColDefinition}
	}
	for _, column := range columns {
		for _, check := range tkn.takeColumnChecks(column) {
			options = append(options, &AddConstraintDefinition{ConstraintDefinition: check})
		}
	}
	return options
}
