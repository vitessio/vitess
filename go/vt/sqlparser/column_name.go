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
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/eightbit"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file implements the rules MySQL uses to name the result-set column of
// a select expression. See doc/design-docs/MySQLCompatibleColumnNames.md for
// the rules and where they come from in the MySQL source.

// columnNameKind says how MySQL names an unaliased select expression.
type columnNameKind uint8

const (
	// nameRaw names the column after the expression's query text.
	nameRaw columnNameKind = iota
	// nameColumn names the column after the column identifier, as typed.
	nameColumn
	// nameText names the column after the value of the first fragment of a
	// text literal.
	nameText
	// nameNull names the column "NULL".
	nameNull
	// nameParam names the column "?".
	nameParam
	// nameInt names the column after an integer token that fits in a signed
	// 64-bit integer. The name is never truncated.
	nameInt
	// nameUint names the column after an integer token that only fits in an
	// unsigned 64-bit integer. The name is truncated to 256 bytes.
	nameUint
	// nameDecimal names the column after a decimal token, or an integer token
	// too large for 64 bits. The name is never truncated.
	nameDecimal
	// nameFloat names the column after a floating-point token. The name is
	// truncated to 256 bytes.
	nameFloat
)

// cppEditKind is a change that MySQL's lexer makes to the query text while it
// copies it into its pre-processed buffer, which is where column names are
// taken from.
type cppEditKind uint8

const (
	// cppOpen is the opening "/*!" or "/*!NNNNN" of an executed versioned
	// comment. It is removed.
	cppOpen cppEditKind = iota
	// cppClose is the closing "*/" of an executed versioned comment. It is
	// removed, and a space may be inserted in its place.
	cppClose
	// cppSkip is a versioned comment that is not executed. It is removed.
	cppSkip
)

// cppEdit is a cppEditKind at the byte range [start, end) of the query.
type cppEdit struct {
	start, end int
	kind       cppEditKind
}

// columnNameInput is what the parser records about a select expression so
// that the column can later be named the way MySQL names it.
type columnNameInput struct {
	// text is the query text of the expression, from the first byte of its
	// first token to the last byte of its last token.
	text string
	// token is the identifier of a column reference, or the token of a number.
	token string
	// edits are the versioned-comment edits inside text, with offsets
	// relative to text.
	edits []cppEdit
	kind  columnNameKind
	// parsed is set for every select expression the parser creates.
	parsed bool
	// aliased is set when the query gave the expression an alias. The alias
	// can be empty (AS ''), which is different from having no alias.
	aliased bool
}

// ColumnNameEnv holds the session settings that MySQL's column names depend on.
type ColumnNameEnv struct {
	// ClientCharset is the value of character_set_client. Empty means utf8mb4.
	ClientCharset string
	// ConnectionCharset is the charset of collation_connection. Empty means
	// utf8mb4.
	ConnectionCharset string
}

// setColumnNameInput records the naming input of a select expression. start and
// end are the byte offsets of the expression in the query, and aliased says
// whether the query gave the expression an alias.
func (ae *AliasedExpr) setColumnNameInput(tkn *Tokenizer, start, end int, aliased bool) {
	in := &ae._name
	in.parsed = true
	in.aliased = aliased
	if aliased {
		return
	}
	if start < 0 || end > len(tkn.buf) || start > end {
		return
	}
	in.text = tkn.buf[start:end]
	for _, e := range tkn.cppEdits {
		if e.start >= start && e.end <= end {
			in.edits = append(in.edits, cppEdit{start: e.start - start, end: e.end - start, kind: e.kind})
		}
	}

	switch expr := ae.Expr.(type) {
	case *ColName:
		in.kind = nameColumn
		in.token = expr.Name.String()
	case *NullVal:
		in.kind = nameNull
	case *Argument:
		if unwrapParensAndPlus(in.text) == "?" {
			in.kind = nameParam
		}
	case *Literal:
		in.kind, in.token = literalNameKind(expr)
	case *UnaryExpr:
		if lit, ok := expr.Expr.(*Literal); ok && expr.Operator == NStringOp && lit.Type == StrVal {
			in.kind = nameText
		}
	case *IntroducerExpr:
		if lit, ok := expr.Expr.(*Literal); ok && lit.Type == StrVal {
			in.kind = nameText
		}
	}
}

// literalNameKind returns how MySQL names a literal, and the token the name is
// taken from, if any.
func literalNameKind(lit *Literal) (columnNameKind, string) {
	switch lit.Type {
	case StrVal:
		return nameText, ""
	case IntVal:
		return integerNameKind(lit.Val), lit.Val
	case DecimalVal:
		return nameDecimal, lit.Val
	case FloatVal:
		return nameFloat, lit.Val
	}
	// Hexadecimal, bit and temporal literals are named after their text.
	return nameRaw, ""
}

// integerNameKind classifies an integer token the way MySQL's lexer does,
// which decides whether its name is truncated.
func integerNameKind(token string) columnNameKind {
	digits := strings.TrimLeft(token, "0")
	const maxInt64 = "9223372036854775807"
	const maxUint64 = "18446744073709551615"
	switch {
	case len(digits) < len(maxInt64) || (len(digits) == len(maxInt64) && digits <= maxInt64):
		return nameInt
	case len(digits) < len(maxUint64) || (len(digits) == len(maxUint64) && digits <= maxUint64):
		return nameUint
	}
	return nameDecimal
}

// unwrapParensAndPlus strips the parentheses and unary plus signs around an
// expression's text, which MySQL ignores when it names the expression.
func unwrapParensAndPlus(text string) string {
	for {
		trimmed := strings.TrimSpace(text)
		switch {
		case strings.HasPrefix(trimmed, "+"):
			text = trimmed[1:]
		case len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')':
			text = trimmed[1 : len(trimmed)-1]
		default:
			return trimmed
		}
	}
}

// MySQLColumnName returns the name MySQL gives the result-set column of this
// select expression.
//
// Expressions that the parser did not create, for example ones that the planner
// adds, are named by ColumnName.
func (ae *AliasedExpr) MySQLColumnName(env ColumnNameEnv) string {
	in := &ae._name
	if !in.parsed {
		return ae.ColumnName()
	}
	if in.aliased {
		return aliasName(ae.As.String(), env)
	}
	switch in.kind {
	case nameColumn:
		if env.ClientCharset == "latin1" {
			return cutAtNUL(identifierName(in.token, env))
		}
		return cutAtNUL(in.token)
	case nameInt, nameDecimal:
		return cutAtNUL(in.token)
	case nameNull:
		return "NULL"
	case nameParam:
		return "?"
	case nameUint, nameFloat:
		return truncateUTF8(in.token, maxAliasName)
	case nameText:
		value, cs := firstTextFragment(in.text)
		if cs == "utf8mb3" {
			// N'...' is converted to the national charset, utf8mb3.
			value = toUTF8MB3(value, len(value))
		} else if cs == "" {
			cs = env.ConnectionCharset
		}
		return copyName(value, cs)
	}
	return copyName(applyCppEdits(in.text, in.edits), env.ClientCharset)
}

// AliasColumnNames gives an explicit alias to each select expression whose
// column MySQL names differently from the SQL that vtgate sends for it, so
// that the column keeps the name MySQL gives the expression as the query
// spells it. This covers the result columns, and the columns of derived
// tables, CTEs and views, which queries refer to by name. It is called after
// the statement is normalized, and before the plan cache key is taken from
// it: the aliases distinguish statements whose columns have different names.
//
// A result column whose name contains the values of bind variables in
// bindVars, such as the literals that normalizing replaced with bind
// variables, is not aliased, so that statements that differ only in those
// values share a plan. Nor is one with an empty name, which cannot be an
// alias. The returned renames name these columns in each result.
func AliasColumnNames(stmt Statement, env ColumnNameEnv, bindVars map[string]*querypb.BindVariable) *ColumnRenames {
	var renames *ColumnRenames
	ts, isTableStatement := stmt.(TableStatement)
	if isTableStatement {
		renames = aliasSelectExprs(ts, env, resultColumns, bindVars)
	}
	var unnamed []string
	aliasTableColumns := func(table TableStatement) {
		if generated := aliasSelectExprs(table, env, tableColumns, nil); generated != nil {
			unnamed = append(unnamed, generated.unnamed...)
		}
	}
	_ = Walk(func(node SQLNode) (bool, error) {
		switch node := node.(type) {
		case *AliasedTableExpr:
			if dt, ok := node.Expr.(*DerivedTable); ok && len(node.Columns) == 0 {
				aliasTableColumns(dt.Select)
			}
		case *CommonTableExpr:
			if len(node.Columns) == 0 {
				aliasTableColumns(node.Subquery)
			}
		case *CreateView:
			if len(node.Columns) == 0 {
				aliasSelectExprs(node.Select, env, viewColumns, nil)
			}
		case *AlterView:
			if len(node.Columns) == 0 {
				aliasSelectExprs(node.Select, env, viewColumns, nil)
			}
		}
		return true, nil
	}, stmt)
	if len(unnamed) > 0 && isTableStatement {
		// A star over a derived table or CTE returns its columns under the
		// names that stand in for their empty names.
		if renames == nil {
			renames = newColumnRenames(ts)
		}
		if renames != nil {
			renames.unnamed = unnamed
		}
	}
	return renames
}

// columnsOf says what the select expressions of a query expression name.
type columnsOf int

const (
	// resultColumns are the columns of the result set.
	resultColumns columnsOf = iota
	// tableColumns are the columns of a derived table or CTE.
	tableColumns
	// viewColumns are the columns of a view.
	viewColumns
)

// maxViewColumnName is the number of characters a view column name can
// have. MySQL names a view column whose name would be longer, empty, or end
// with a space Name_exp_<position>.
const maxViewColumnName = 64

// aliasSelectExprs aliases the select expressions of the first query block of
// a query expression, which name its columns. For result columns, it returns
// the columns to rename in each result.
func aliasSelectExprs(ts TableStatement, env ColumnNameEnv, of columnsOf, bindVars map[string]*querypb.BindVariable) *ColumnRenames {
	sel, err := GetFirstSelect(ts)
	if err != nil || sel == nil {
		return nil
	}
	var renames *ColumnRenames
	rename := func(ordinal int, name string) {
		if renames == nil {
			renames = newColumnRenames(ts)
		}
		renames.renames = append(renames.renames, columnRename{ordinal: ordinal, name: name})
	}
	referenced := referencedNames(ts)
	// A referenced name that is not a plain identifier can only be the name
	// of a select expression. The SQL that vtgate sends must not give it to
	// an expression that MySQL names differently.
	refersToExpressions := slices.ContainsFunc(referenced, func(name string) bool {
		return strings.ContainsFunc(name, func(r rune) bool {
			return r != '_' && r != '$' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
		})
	})
	var materialized map[string]TableStatement
	for i, expr := range sel.SelectExprs.Exprs {
		ae, ok := expr.(*AliasedExpr)
		if !ok || !ae._name.parsed {
			continue
		}
		if ae.As.NotEmpty() {
			// MySQL's rules apply to aliases too: for example, leading
			// spaces are removed.
			name := ae.MySQLColumnName(env)
			switch {
			case !ae._name.aliased || name == ae.As.String():
			case name != "":
				ae.As = NewIdentifierCI(name)
			case of == resultColumns:
				rename(i, name)
			}
			continue
		}
		if ae._name.kind == nameColumn {
			// MySQL names a column reference as typed, unless it reads a
			// derived table or view that MySQL merges into the query: then
			// the column takes the name it has there, as it does in vtgate.
			if materialized == nil {
				materialized = materializedTables(sel, ts)
			}
			col, ok := ae.Expr.(*ColName)
			if !ok {
				continue
			}
			if table := readsMaterializedTable(col, sel, materialized); table != nil && definedName(table, col.Name.String()) != col.Name.String() {
				ae.As = NewIdentifierCI(ae.MySQLColumnName(env))
			}
			continue
		}
		name := ae.MySQLColumnName(env)
		isReferenced := len(referenced) > 0 && slices.Contains(referenced, strings.ToLower(name))
		if of == resultColumns && !isReferenced && (name == "" || (!refersToExpressions && containsBindVar(ae.Expr, bindVars))) {
			// MySQL gets the values of bind variables, not their names,
			// and an empty name cannot be an alias.
			rename(i, name)
			continue
		}
		if name == String(ae.Expr) && !isReferenced && !rewrittenByPlanning(ae.Expr) {
			// MySQL names the SQL that vtgate sends the same way. A column
			// that the query refers to by name needs an alias for vtgate to
			// resolve the reference.
			continue
		}
		switch of {
		case tableColumns:
			if name == "" {
				// The column cannot be referenced, but it needs a name
				// that the SQL vtgate sends can use.
				name = "vt_unnamed_" + strconv.Itoa(i)
				if renames == nil {
					renames = &ColumnRenames{}
				}
				renames.unnamed = append(renames.unnamed, name)
			}
		case viewColumns:
			if name == "" || utf8.RuneCountInString(name) > maxViewColumnName || strings.HasSuffix(name, " ") {
				name = "Name_exp_" + strconv.Itoa(i+1)
			}
		}
		if name == "" {
			continue
		}
		ae.As = NewIdentifierCI(name)
	}
	return renames
}

// containsBindVar reports whether an expression contains one of the bind
// variables.
func containsBindVar(expr Expr, bindVars map[string]*querypb.BindVariable) bool {
	if len(bindVars) == 0 {
		return false
	}
	found := false
	_ = Walk(func(node SQLNode) (bool, error) {
		switch node := node.(type) {
		case *Argument:
			_, found = bindVars[node.Name]
		case ListArg:
			_, found = bindVars[string(node)]
		}
		return !found, nil
	}, expr)
	return found
}

// ColumnRenames are the result columns that a statement names in each result
// rather than with aliases.
type ColumnRenames struct {
	renames []columnRename
	// exprs is the number of select expressions, and firstStar and lastStar
	// are the positions of the first and last star among them, or -1.
	exprs, firstStar, lastStar int
	// unnamed are the names that stand in for the empty names of columns of
	// derived tables and CTEs. A star returns them, and they become empty.
	unnamed []string
}

// newColumnRenames returns the renames of the result columns of a query
// expression, or nil if it has no first query block.
func newColumnRenames(ts TableStatement) *ColumnRenames {
	sel, err := GetFirstSelect(ts)
	if err != nil || sel == nil {
		return nil
	}
	renames := &ColumnRenames{exprs: len(sel.SelectExprs.Exprs), firstStar: -1, lastStar: -1}
	for i, expr := range sel.SelectExprs.Exprs {
		if _, ok := expr.(*StarExpr); ok {
			if renames.firstStar < 0 {
				renames.firstStar = i
			}
			renames.lastStar = i
		}
	}
	return renames
}

type columnRename struct {
	ordinal int
	name    string
}

// Apply returns the fields with the names of the renamed columns. It never
// changes fields in place, because they can be shared, for example by cached
// plans: it returns a new slice when a name changes. The columns of a star
// between two others cannot be told apart, so columns between the first and
// the last star keep their names.
func (r *ColumnRenames) Apply(fields []*querypb.Field) []*querypb.Field {
	if r == nil || len(fields) == 0 {
		return fields
	}
	var renamed []*querypb.Field
	setName := func(idx int, name string) {
		if renamed == nil {
			renamed = slices.Clone(fields)
		}
		field := fields[idx].CloneVT()
		field.Name = name
		renamed[idx] = field
	}
	if r.firstStar >= 0 {
		for idx := r.firstStar; idx < len(fields)-(r.exprs-1-r.lastStar); idx++ {
			if slices.Contains(r.unnamed, fields[idx].Name) {
				setName(idx, "")
			}
		}
	}
	if r.firstStar < 0 && len(fields) != r.exprs {
		if renamed == nil {
			return fields
		}
		return renamed
	}
	for _, rn := range r.renames {
		idx := rn.ordinal
		switch {
		case r.firstStar < 0 || rn.ordinal < r.firstStar:
		case rn.ordinal > r.lastStar:
			idx = len(fields) - (r.exprs - rn.ordinal)
		default:
			continue
		}
		if idx < 0 || idx >= len(fields) || fields[idx].Name == rn.name {
			continue
		}
		setName(idx, rn.name)
	}
	if renamed == nil {
		return fields
	}
	return renamed
}

// referencedNames returns the lowercased names that the ORDER BY, GROUP BY and
// HAVING clauses of a query expression refer to without a qualifier. MySQL
// resolves them against the names of the select expressions.
func referencedNames(ts TableStatement) []string {
	var clauses []SQLNode
	switch ts := ts.(type) {
	case *Select:
		clauses = []SQLNode{ts.OrderBy, ts.GroupBy, ts.Having}
	case *Union:
		clauses = []SQLNode{ts.OrderBy}
	}
	var names []string
	for _, clause := range clauses {
		_ = Walk(func(node SQLNode) (bool, error) {
			switch node := node.(type) {
			case *ColName:
				if node.Qualifier.IsEmpty() {
					names = append(names, node.Name.Lowered())
				}
			case *Subquery:
				return false, nil
			}
			return true, nil
		}, clause)
	}
	return names
}

// materializedTables returns the derived tables and CTEs in the FROM clause
// of a query block that MySQL materializes, by their lowercased names.
func materializedTables(sel *Select, ts TableStatement) map[string]TableStatement {
	materialized := map[string]TableStatement{}
	var with *With
	switch ts := ts.(type) {
	case *Select:
		with = ts.With
	case *Union:
		with = ts.With
	}
	visit := func(node SQLNode) (bool, error) {
		switch node := node.(type) {
		case *AliasedTableExpr:
			switch expr := node.Expr.(type) {
			case *DerivedTable:
				if !MySQLMergesDerivedTable(expr.Select) {
					materialized[strings.ToLower(node.As.String())] = expr.Select
				}
			case TableName:
				if cte := findCTE(with, expr); cte != nil && !MySQLMergesDerivedTable(cte.Subquery) {
					name := node.As
					if name.IsEmpty() {
						name = expr.Name
					}
					materialized[strings.ToLower(name.String())] = cte.Subquery
				}
			}
			return false, nil
		case *Subquery:
			return false, nil
		}
		return true, nil
	}
	for _, table := range sel.From {
		_ = Walk(visit, table)
	}
	return materialized
}

// rewrittenByPlanning reports whether planning can rewrite an expression, so
// that the SQL vtgate sends spells it differently: planning rewrites
// subqueries, NOT before a comparison, and comparisons of tuples.
func rewrittenByPlanning(expr Expr) bool {
	found := false
	_ = Walk(func(node SQLNode) (bool, error) {
		switch node := node.(type) {
		case *Subquery:
			found = true
		case *NotExpr:
			_, found = node.Expr.(*ComparisonExpr)
		case *ComparisonExpr:
			_, left := node.Left.(ValTuple)
			_, right := node.Right.(ValTuple)
			found = left && right
		}
		return !found, nil
	}, expr)
	return found
}

// readsMaterializedTable returns the derived table or CTE that MySQL
// materializes and that a column reference reads: the one that its qualifier
// names, or the only table of the query block. It returns nil otherwise.
func readsMaterializedTable(col *ColName, sel *Select, materialized map[string]TableStatement) TableStatement {
	if !col.Qualifier.IsEmpty() {
		if !col.Qualifier.Qualifier.IsEmpty() {
			return nil
		}
		return materialized[strings.ToLower(col.Qualifier.Name.String())]
	}
	if len(sel.From) != 1 || len(materialized) != 1 {
		return nil
	}
	if _, single := sel.From[0].(*AliasedTableExpr); !single {
		return nil
	}
	for _, table := range materialized {
		return table
	}
	return nil
}

// definedName returns the name of the column that a derived table or CTE
// defines under the given name, which matches it case-insensitively, or the
// name itself when it cannot tell.
func definedName(table TableStatement, name string) string {
	sel, err := GetFirstSelect(table)
	if err != nil || sel == nil {
		return name
	}
	for _, expr := range sel.SelectExprs.Exprs {
		ae, ok := expr.(*AliasedExpr)
		if !ok {
			continue
		}
		if defined := ae.ColumnName(); strings.EqualFold(defined, name) {
			return defined
		}
	}
	return name
}

// findCTE returns the CTE that an unqualified table name refers to.
func findCTE(with *With, name TableName) *CommonTableExpr {
	if with == nil || !name.Qualifier.IsEmpty() {
		return nil
	}
	for _, cte := range with.CTEs {
		if cte.ID.String() == name.Name.String() {
			return cte
		}
	}
	return nil
}

// MySQLMergesDerivedTable reports whether MySQL can merge a derived table, CTE
// or view with this query expression into the query that reads from it,
// rather than materialize it. MySQL also materializes every derived table
// when the optimizer switch derived_merge is off, and ones named by a
// NO_MERGE hint.
func MySQLMergesDerivedTable(stmt TableStatement) bool {
	sel, ok := stmt.(*Select)
	if !ok {
		// Set operations are materialized.
		return false
	}
	if sel.Distinct || sel.GroupBy != nil || sel.Having != nil || sel.Limit != nil || len(sel.Windows) > 0 {
		return false
	}
	if len(sel.From) == 0 {
		return false
	}
	if len(sel.From) == 1 {
		if aliased, ok := sel.From[0].(*AliasedTableExpr); ok {
			if name, ok := aliased.Expr.(TableName); ok && strings.EqualFold(name.Name.String(), "dual") && name.Qualifier.IsEmpty() {
				return false
			}
		}
	}
	merges := true
	for _, se := range sel.SelectExprs.Exprs {
		_ = Walk(func(node SQLNode) (bool, error) {
			switch node.(type) {
			case AggrFunc, WindowFunc, *AssignmentExpr:
				merges = false
			case *Subquery:
				// Subqueries have their own scope.
				return false, nil
			}
			return merges, nil
		}, se)
	}
	return merges
}

// maxAliasName is MAX_ALIAS_NAME in MySQL: the longest a column name can be,
// in bytes.
const maxAliasName = 256

// aliasName applies MySQL's rules to an explicit alias: it is converted from
// the client charset to utf8mb3, leading non-graphic characters are removed,
// and the name is truncated to 256 bytes.
func aliasName(alias string, env ColumnNameEnv) string {
	return copyName(identifierName(alias, env), "utf8mb3")
}

// identifierName converts an identifier from the client charset to utf8mb3,
// as MySQL's lexer does: for a utf8mb4 client, characters outside the Basic
// Multilingual Plane become '?'.
func identifierName(name string, env ColumnNameEnv) string {
	switch env.ClientCharset {
	case "latin1":
		return latin1ToUTF8MB3(name, 3*len(name))
	case "utf8mb3", "utf8":
		return name
	}
	return toUTF8MB3(name, len(name))
}

// copyName is MySQL's Name_string::copy, followed by the NUL cut that happens
// when the name is sent to the client. The name is always returned as valid
// UTF-8.
func copyName(name string, cs string) string {
	name = stripLeadingNonGraphic(name, cs)
	switch cs {
	case "utf8mb3", "utf8":
		// MySQL copies at most 256 bytes as they are. It can cut a character in
		// half, which clients that receive utf8mb4 never see: the conversion
		// drops it.
		name = truncateUTF8(name, maxAliasName)
	case "latin1":
		name = latin1ToUTF8MB3(name, maxAliasName-1)
	case "binary":
		if len(name) > maxAliasName-1 {
			name = name[:maxAliasName-1]
		}
		name = strings.ToValidUTF8(name, "?")
	default:
		name = toUTF8MB3(name, maxAliasName-1)
	}
	return cutAtNUL(name)
}

// stripLeadingNonGraphic removes the leading bytes that are not graphic in the
// given charset, the way Name_string::copy does.
func stripLeadingNonGraphic(name string, cs string) string {
	for i := 0; i < len(name); i++ {
		b := name[i]
		graphic := b > 0x20 && b != 0x7f
		switch cs {
		case "binary":
			graphic = graphic && b < 0x80
		case "latin1":
			switch b {
			case 0x81, 0x8d, 0x8f, 0x90, 0x9d, 0xa0:
				graphic = false
			}
		}
		if graphic {
			return name[i:]
		}
	}
	return ""
}

// toUTF8MB3 converts UTF-8 text to utf8mb3 the way MySQL does: characters
// outside the Basic Multilingual Plane and invalid bytes become '?', and an
// incomplete character at the end ends the text. It stops before the result
// would exceed limit bytes.
func toUTF8MB3(s string, limit int) string {
	clean := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			clean = false
			break
		}
	}
	if clean {
		if len(s) > limit {
			return s[:limit]
		}
		return s
	}

	var b strings.Builder
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size <= 1 && !utf8.FullRuneInString(s) {
			break
		}
		width := size
		if r == utf8.RuneError && size <= 1 || r > 0xFFFF {
			r, width = '?', 1
		}
		if b.Len()+width > limit {
			break
		}
		if r == '?' && width == 1 {
			b.WriteByte('?')
		} else {
			b.WriteString(s[:size])
		}
		s = s[size:]
	}
	return b.String()
}

// latin1ToUTF8MB3 converts latin1 bytes to UTF-8, stopping before the result
// would exceed limit bytes.
func latin1ToUTF8MB3(s string, limit int) string {
	var cs eightbit.Charset_latin1
	var b strings.Builder
	var buf [utf8.UTFMax]byte
	for i := 0; i < len(s); i++ {
		r, _, _ := cs.DecodeRune([]byte{s[i]})
		n := utf8.EncodeRune(buf[:], r)
		if b.Len()+n > limit {
			break
		}
		b.Write(buf[:n])
	}
	return b.String()
}

// truncateUTF8 truncates s to at most limit bytes, without cutting a character
// in half.
func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}

// cutAtNUL returns s up to its first NUL byte. MySQL sends column names as
// NUL-terminated strings, so a name ends at its first NUL.
func cutAtNUL(s string) string {
	if i := strings.IndexByte(s, 0); i >= 0 {
		return s[:i]
	}
	return s
}

// applyCppEdits applies the versioned-comment edits to an expression's text,
// giving the text as it is in MySQL's pre-processed buffer.
func applyCppEdits(text string, edits []cppEdit) string {
	if len(edits) == 0 {
		return text
	}
	var b strings.Builder
	pos := 0
	for _, e := range edits {
		b.WriteString(text[pos:e.start])
		pos = e.end
		if e.kind != cppClose || pos >= len(text) || b.Len() == 0 {
			continue
		}
		// MySQL inserts a space after an executed versioned comment when
		// neither the text before nor the text after it is whitespace.
		out := b.String()
		if !isCppSpace(text[pos]) && !isCppSpace(out[len(out)-1]) {
			b.WriteByte(' ')
		}
	}
	b.WriteString(text[pos:])
	return b.String()
}

func isCppSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// firstTextFragment returns the value of the first string of a text literal,
// such as a for 'a' 'b', N'a' or _utf8mb4'a', and the charset that an
// introducer or N gives it, if any. MySQL names a text literal after its first
// string only.
func firstTextFragment(text string) (value, charset string) {
	tkn := &Tokenizer{buf: text, parser: &Parser{}}
	for {
		typ, val := tkn.Scan()
		switch typ {
		case STRING:
			return val, charset
		case NCHAR_STRING:
			return val, "utf8mb3"
		case 0, LEX_ERROR:
			return "", charset
		default:
			if strings.HasPrefix(val, "_") {
				charset = strings.ToLower(val[1:])
			}
		}
	}
}
