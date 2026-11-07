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

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MySQL names a column that a query makes from a select expression without an
// alias after the expression: after the column for a column, after the value
// for a string or NULL, and after the expression's text as written for
// anything else, so `a+b` and `a + b` make different columns. Vitess sends
// MySQL the statement that it formats, which can write an expression
// differently. Where the name is kept, by the table of CREATE TABLE ...
// SELECT, a view, or a derived table or common table expression whose columns
// those refer to, the parser gives such an expression the name that MySQL
// gives it as written, as an alias.

// maxAliasLength is the longest alias that MySQL takes in a select list.
const maxAliasLength = 256

// maxColumnNameLength is the longest name that MySQL takes for a column of a
// table or a view, in characters.
const maxColumnNameLength = 64

// queryColumns says what the columns of a query name.
type queryColumns int

const (
	// tableColumns are the columns of the table that CREATE TABLE ... SELECT
	// creates.
	tableColumns queryColumns = iota
	// viewColumns are the columns of a view.
	viewColumns
	// namedColumns are columns that a list of names names, as in
	// CREATE VIEW v (a, b) AS SELECT ..., so only the names inside the query
	// are kept.
	namedColumns
)

// preserveColumnNames adds the aliases that keep the names of the columns
// that a CREATE TABLE ... SELECT or a view makes from query, and of the
// derived tables and common table expressions inside it.
func preserveColumnNames(query TableStatement, columns queryColumns) {
	_ = Walk(func(node SQLNode) (bool, error) {
		switch node := node.(type) {
		case *AliasedTableExpr:
			if derived, ok := node.Expr.(*DerivedTable); ok && len(node.Columns) == 0 {
				preserveQueryColumnNames(firstSelect(derived.Select), false)
			}
		case *CommonTableExpr:
			if len(node.Columns) == 0 {
				preserveQueryColumnNames(firstSelect(node.Subquery), false)
			}
		}
		return true, nil
	}, query)
	switch columns {
	case tableColumns:
		preserveQueryColumnNames(firstSelect(query), true)
	case viewColumns:
		preserveViewColumnNames(firstSelect(query))
	case namedColumns:
	}
}

// firstSelect returns the query block that names the columns of query: its
// first SELECT.
func firstSelect(query TableStatement) *Select {
	switch query := query.(type) {
	case *Select:
		return query
	case *Union:
		return firstSelect(query.Left)
	}
	return nil
}

// preserveQueryColumnNames gives each expression of sel without an alias the
// name that MySQL gives it as written, when MySQL would name the formatted
// expression differently. table is set for the columns of a table, which MySQL
// limits to fewer characters than an alias.
func preserveQueryColumnNames(sel *Select, table bool) {
	if sel == nil || sel.SelectExprs == nil {
		return
	}
	for _, expr := range sel.SelectExprs.Exprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok || aliased.As.NotEmpty() {
			continue
		}
		name := aliased.ColumnName()
		if name == formattedColumnName(aliased) {
			continue
		}
		// MySQL rejects a name that is too long for the column of a table
		// as written, and an alias that is too long, so either way the
		// statement fails. A name that is too long for an alias but not for
		// a derived table's column is left to fail differently.
		if !table && utf8.RuneCountInString(name) > maxAliasLength {
			continue
		}
		aliased.As = NewIdentifierCI(name)
	}
}

// formattedColumnName returns the name that MySQL gives the column of a select
// expression without an alias that Vitess formats.
func formattedColumnName(aliased *AliasedExpr) string {
	return (&AliasedExpr{Expr: aliased.Expr}).ColumnName()
}

// viewColumn is a column of a view, as MySQL names it.
type viewColumn struct {
	name string
	// original is the name before MySQL replaced it with a synthetic one.
	original string
	// generated is set for a name that MySQL derives from the expression,
	// rather than from an alias or a column.
	generated bool
}

// preserveViewColumnNames gives each expression of sel without an alias the
// name of the view's column that MySQL makes from it as written. MySQL
// replaces a generated name that is not a valid column name, and a generated
// name that two columns share, with a synthetic one, which an alias would
// otherwise turn into an error.
func preserveViewColumnNames(sel *Select) {
	if sel == nil || sel.SelectExprs == nil {
		return
	}
	var aliases []*AliasedExpr
	var columns []viewColumn
	for _, expr := range sel.SelectExprs.Exprs {
		aliased, ok := expr.(*AliasedExpr)
		if !ok {
			// A * is expanded into the columns it stands for, whose names
			// this cannot know, so the names cannot be made unique the way
			// MySQL does. Each expression keeps its name, as for a table.
			preserveQueryColumnNames(sel, true)
			return
		}
		_, isColumn := aliased.Expr.(*ColName)
		aliases = append(aliases, aliased)
		columns = append(columns, viewColumn{name: aliased.ColumnName(), generated: aliased.As.IsEmpty() && !isColumn})
	}
	if !makeViewColumnNames(columns) {
		// MySQL rejects the view, and the formatted view the same way.
		return
	}
	for i, aliased := range aliases {
		if aliased.As.NotEmpty() {
			continue
		}
		if columns[i].name != formattedColumnName(aliased) {
			aliased.As = NewIdentifierCI(columns[i].name)
		}
	}
}

// syntheticColumnName is the prefix of the names that MySQL makes up for the
// columns of a view.
const syntheticColumnName = "Name_exp_"

// makeViewColumnNames names the columns of a view the way MySQL's
// make_valid_column_names and check_duplicate_names do. It reports false when
// MySQL rejects the names, because two names that it does not generate are the
// same.
func makeViewColumnNames(columns []viewColumn) bool {
	for i := range columns {
		if columns[i].generated && !isValidColumnName(columns[i].name) {
			columns[i].original = columns[i].name
			columns[i].name = fmt.Sprintf("%s%d", syntheticColumnName, i+1)
		}
	}
	for i := range columns {
		for j := range i {
			if !strings.EqualFold(columns[i].name, columns[j].name) {
				continue
			}
			switch {
			case columns[i].generated:
				makeUniqueViewColumnName(columns, i, i)
			case columns[j].generated:
				makeUniqueViewColumnName(columns, j, i)
			default:
				return false
			}
		}
	}
	return true
}

// makeUniqueViewColumnName gives the column at target a synthetic name that no
// column up to last has, the way MySQL's make_unique_view_field_name does.
func makeUniqueViewColumnName(columns []viewColumn, target, last int) {
	name := columns[target].name
	if columns[target].original != "" {
		name = columns[target].original
	}
	for attempt := 0; ; attempt++ {
		candidate := syntheticColumnName + name
		if attempt > 0 {
			candidate = fmt.Sprintf("%s%d_%s", syntheticColumnName, attempt, name)
		}
		candidate = truncateRunes(candidate, maxColumnNameLength)
		unique := true
		for i := 0; i <= last; i++ {
			if i != target && strings.EqualFold(columns[i].name, candidate) {
				unique = false
				break
			}
		}
		if unique {
			columns[target].original = columns[target].name
			columns[target].name = candidate
			return
		}
	}
}

// isValidColumnName reports whether MySQL takes name as the name of a column:
// it is valid UTF-8, it is not empty, it does not end with a space and it is
// not too long.
func isValidColumnName(name string) bool {
	if name == "" || !utf8.ValidString(name) {
		return false
	}
	last, _ := utf8.DecodeLastRuneInString(name)
	if unicode.IsSpace(last) {
		return false
	}
	return utf8.RuneCountInString(name) <= maxColumnNameLength
}

func truncateRunes(s string, limit int) string {
	count := 0
	for index := range s {
		if count == limit {
			return s[:index]
		}
		count++
	}
	return s
}

// stringColumnName returns the name of the column that MySQL makes from a
// string literal. It is the literal's value, but MySQL names a literal that
// is written as several strings in a row, as in 'a' 'b', after the first one.
// text is the literal as written, if the parser kept it.
func stringColumnName(literal *Literal, text string) string {
	if text == "" {
		return literal.Val
	}
	tkn := &Tokenizer{buf: text}
	typ, val := tkn.Scan()
	if strings.HasPrefix(val, "_") || typ == ID {
		// A character set introducer, as in _utf8mb4'a' 'b'.
		typ, val = tkn.Scan()
	}
	if typ != STRING && typ != NCHAR_STRING {
		return literal.Val
	}
	return val
}

// isConcatenatedString reports whether text, a string literal as written,
// is several strings in a row rather than one string whose value is value.
func isConcatenatedString(text, value string) bool {
	tkn := &Tokenizer{buf: text}
	typ, val := tkn.Scan()
	if typ != STRING {
		return false
	}
	return val != value
}
