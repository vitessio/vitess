/*
Copyright 2019 The Vitess Authors.

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

// analyzer.go contains utility analysis functions.

import (
	"fmt"
	"strings"
	"unicode"
)

// StatementType encodes the type of a SQL statement
type StatementType int

// These constants are used to identify the SQL statement type.
// Changing this list will require reviewing all calls to Preview.
const (
	StmtSelect StatementType = iota
	StmtStream
	StmtInsert
	StmtReplace
	StmtUpdate
	StmtDelete
	StmtDDL
	StmtBegin
	StmtCommit
	StmtRollback
	StmtSet
	StmtShow
	StmtUse
	StmtOther
	StmtUnknown
	StmtComment
	StmtPriv
	StmtExplain
	StmtSavepoint
	StmtSRollback
	StmtRelease
	StmtVStream
	StmtLockTables
	StmtUnlockTables
	StmtFlush
	StmtCallProc
	StmtRevert
	StmtShowMigrationLogs
	StmtCommentOnly
)

//ASTToStatementType returns a StatementType from an AST stmt
func ASTToStatementType(stmt Statement) StatementType {
	switch stmt.(type) {
	case *Select, *Union:
		return StmtSelect
	case *Insert:
		return StmtInsert
	case *Update:
		return StmtUpdate
	case *Delete:
		return StmtDelete
	case *Set, *SetTransaction:
		return StmtSet
	case *Show:
		return StmtShow
	case DDLStatement, DBDDLStatement, *AlterVschema:
		return StmtDDL
	case *RevertMigration:
		return StmtRevert
	case *ShowMigrationLogs:
		return StmtShowMigrationLogs
	case *Use:
		return StmtUse
	case *OtherRead, *OtherAdmin, *Load:
		return StmtOther
	case Explain:
		return StmtExplain
	case *Begin:
		return StmtBegin
	case *Commit:
		return StmtCommit
	case *Rollback:
		return StmtRollback
	case *Savepoint:
		return StmtSavepoint
	case *SRollback:
		return StmtSRollback
	case *Release:
		return StmtRelease
	case *LockTables:
		return StmtLockTables
	case *UnlockTables:
		return StmtUnlockTables
	case *Flush:
		return StmtFlush
	case *CallProc:
		return StmtCallProc
	case *Stream:
		return StmtStream
	case *VStream:
		return StmtVStream
	case *CommentOnly:
		return StmtCommentOnly
	default:
		return StmtUnknown
	}
}

//CanNormalize takes Statement and returns if the statement can be normalized.
func CanNormalize(stmt Statement) bool {
	switch stmt.(type) {
	case *Select, *Union, *Insert, *Update, *Delete, *Set, *CallProc, *Stream: // TODO: we could merge this logic into ASTrewriter
		return true
	}
	return false
}

// CachePlan takes Statement and returns true if the query plan should be cached
func CachePlan(stmt Statement) bool {
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	case *Select:
		comments = stmt.Comments
	case *Insert:
		comments = stmt.Comments
	case *Update:
		comments = stmt.Comments
	case *Delete:
		comments = stmt.Comments
	case *Union, *Stream:
		return true
	default:
		return false
	}
	return !comments.Directives().IsSet(DirectiveSkipQueryPlanCache)
}

// MustRewriteAST takes Statement and returns true if RewriteAST must run on it for correct execution irrespective of user flags.
func MustRewriteAST(stmt Statement, hasSelectLimit bool) bool {
	switch node := stmt.(type) {
	case *Set:
		return true
	case *Show:
		switch node.Internal.(type) {
		case *ShowBasic:
			return true
		}
		return false
	case SelectStatement:
		return hasSelectLimit
	}
	return false
}

// Preview analyzes the beginning of the query using a simpler and faster
// textual comparison to identify the statement type.
func Preview(sql string) StatementType {
	trimmed := StripLeadingComments(sql)

	if strings.Index(trimmed, "/*!") == 0 {
		return StmtComment
	}

	isNotLetter := func(r rune) bool { return !unicode.IsLetter(r) }
	firstWord := strings.TrimLeftFunc(trimmed, isNotLetter)

	if end := strings.IndexFunc(firstWord, unicode.IsSpace); end != -1 {
		firstWord = firstWord[:end]
	}
	// Comparison is done in order of priority.
	loweredFirstWord := strings.ToLower(firstWord)
	switch loweredFirstWord {
	case "select":
		return StmtSelect
	case "stream":
		return StmtStream
	case "vstream":
		return StmtVStream
	case "revert":
		return StmtRevert
	case "insert":
		return StmtInsert
	case "replace":
		return StmtReplace
	case "update":
		return StmtUpdate
	case "delete":
		return StmtDelete
	case "savepoint":
		return StmtSavepoint
	case "lock":
		return StmtLockTables
	case "unlock":
		return StmtUnlockTables
	}
	// For the following statements it is not sufficient to rely
	// on loweredFirstWord. This is because they are not statements
	// in the grammar and we are relying on Preview to parse them.
	// For instance, we don't want: "BEGIN JUNK" to be parsed
	// as StmtBegin.
	trimmedNoComments, _ := SplitMarginComments(trimmed)
	switch strings.ToLower(trimmedNoComments) {
	case "begin", "start transaction":
		return StmtBegin
	case "commit":
		return StmtCommit
	case "rollback":
		return StmtRollback
	}
	switch loweredFirstWord {
	case "create", "alter", "rename", "drop", "truncate":
		return StmtDDL
	case "flush":
		return StmtFlush
	case "set":
		return StmtSet
	case "show":
		return StmtShow
	case "use":
		return StmtUse
	case "describe", "desc", "explain":
		return StmtExplain
	case "analyze", "repair", "optimize":
		return StmtOther
	case "grant", "revoke":
		return StmtPriv
	case "release":
		return StmtRelease
	case "rollback":
		return StmtSRollback
	}
	return StmtUnknown
}

func (s StatementType) String() string {
	switch s {
	case StmtSelect:
		return "SELECT"
	case StmtStream:
		return "STREAM"
	case StmtVStream:
		return "VSTREAM"
	case StmtRevert:
		return "REVERT"
	case StmtInsert:
		return "INSERT"
	case StmtReplace:
		return "REPLACE"
	case StmtUpdate:
		return "UPDATE"
	case StmtDelete:
		return "DELETE"
	case StmtDDL:
		return "DDL"
	case StmtBegin:
		return "BEGIN"
	case StmtCommit:
		return "COMMIT"
	case StmtRollback:
		return "ROLLBACK"
	case StmtSet:
		return "SET"
	case StmtShow:
		return "SHOW"
	case StmtUse:
		return "USE"
	case StmtOther:
		return "OTHER"
	case StmtPriv:
		return "PRIV"
	case StmtExplain:
		return "EXPLAIN"
	case StmtSavepoint:
		return "SAVEPOINT"
	case StmtSRollback:
		return "SAVEPOINT_ROLLBACK"
	case StmtRelease:
		return "RELEASE"
	case StmtLockTables:
		return "LOCK_TABLES"
	case StmtUnlockTables:
		return "UNLOCK_TABLES"
	case StmtFlush:
		return "FLUSH"
	case StmtCallProc:
		return "CALL_PROC"
	case StmtCommentOnly:
		return "COMMENT_ONLY"
	default:
		return "UNKNOWN"
	}
}

// IsDML returns true if the query is an INSERT, UPDATE or DELETE statement.
func IsDML(sql string) bool {
	switch Preview(sql) {
	case StmtInsert, StmtReplace, StmtUpdate, StmtDelete:
		return true
	}
	return false
}

//IsDMLStatement returns true if the query is an INSERT, UPDATE or DELETE statement.
func IsDMLStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *Insert, *Update, *Delete:
		return true
	}

	return false
}

// SplitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters. Outer parenthesis are removed. Precedence
// should be taken into account if expressions are recombined.
func SplitAndExpression(filters []Expr, node Expr) []Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *AndExpr:
		filters = SplitAndExpression(filters, node.Left)
		return SplitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}

// AndExpressions ands together two or more expressions, minimising the expr when possible
func AndExpressions(exprs ...Expr) Expr {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		result := (Expr)(nil)
	outer:
		// we'll loop and remove any duplicates
		for i, expr := range exprs {
			if expr == nil {
				continue
			}
			if result == nil {
				result = expr
				continue outer
			}

			for j := 0; j < i; j++ {
				if EqualsExpr(expr, exprs[j]) {
					continue outer
				}
			}
			result = &AndExpr{Left: result, Right: expr}
		}
		return result
	}
}

// TableFromStatement returns the qualified table name for the query.
// This works only for select statements.
func TableFromStatement(sql string) (TableName, error) {
	stmt, err := Parse(sql)
	if err != nil {
		return TableName{}, err
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return TableName{}, fmt.Errorf("unrecognized statement: %s", sql)
	}
	if len(sel.From) != 1 {
		return TableName{}, fmt.Errorf("table expression is complex")
	}
	aliased, ok := sel.From[0].(*AliasedTableExpr)
	if !ok {
		return TableName{}, fmt.Errorf("table expression is complex")
	}
	tableName, ok := aliased.Expr.(TableName)
	if !ok {
		return TableName{}, fmt.Errorf("table expression is complex")
	}
	return tableName, nil
}

// GetTableName returns the table name from the SimpleTableExpr
// only if it's a simple expression. Otherwise, it returns "".
func GetTableName(node SimpleTableExpr) IdentifierCS {
	if n, ok := node.(TableName); ok && n.Qualifier.IsEmpty() {
		return n.Name
	}
	// sub-select or '.' expression
	return NewIdentifierCS("")
}

// IsColName returns true if the Expr is a *ColName.
func IsColName(node Expr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsValue returns true if the Expr is a string, integral or value arg.
// NULL is not considered to be a value.
func IsValue(node Expr) bool {
	switch v := node.(type) {
	case Argument:
		return true
	case *Literal:
		switch v.Type {
		case StrVal, HexVal, IntVal:
			return true
		}
	}
	return false
}

// IsNull returns true if the Expr is SQL NULL
func IsNull(node Expr) bool {
	switch node.(type) {
	case *NullVal:
		return true
	}
	return false
}

// IsSimpleTuple returns true if the Expr is a ValTuple that
// contains simple values or if it's a list arg.
func IsSimpleTuple(node Expr) bool {
	switch vals := node.(type) {
	case ValTuple:
		for _, n := range vals {
			if !IsValue(n) {
				return false
			}
		}
		return true
	case ListArg:
		return true
	}
	// It's a subquery
	return false
}

//IsLockingFunc returns true for all functions that are used to work with mysql advisory locks
func IsLockingFunc(node Expr) bool {
	switch node.(type) {
	case *LockingFunc:
		return true
	}
	return false
}
