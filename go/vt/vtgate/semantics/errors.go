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

package semantics

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	unsupportedError interface {
		error
		unsupported()
	}

	bugError interface {
		error
		bug()
	}

	SQLCalcFoundRowsUsageError     struct{}
	UnionWithSQLCalcFoundRowsError struct{}
	MissingInVSchemaError          struct{ Table TableInfo }
	CantUseOptionHereError         struct{ Msg string }
	TableNotUpdatableError         struct{ Table string }
	UnsupportedNaturalJoinError    struct{ JoinExpr *sqlparser.JoinTableExpr }
	NotSequenceTableError          struct{ Table string }
	NextWithMultipleTablesError    struct{ CountTables int }
	LockOnlyWithDualError          struct{ Node *sqlparser.LockingFunc }
	JSONTablesError                struct{ Table string }
	QualifiedOrderInUnionError     struct{ Table string }
	BuggyError                     struct{ Msg string }
	UnsupportedConstruct           struct{ errString string }
	AmbiguousColumnError           struct{ Column string }
	SubqueryColumnCountError       struct{ Expected int }
	ColumnsMissingInSchemaError    struct{}
	CantUseMultipleVindexHints     struct{ Table string }
	InvalidUseOfGroupFunction      struct{}
	CantGroupOn                    struct{ Column string }

	NoSuchVindexFound struct {
		Table      string
		VindexName string
	}

	UnsupportedMultiTablesInUpdateError struct {
		ExprCount int
		NotAlias  bool
	}
	UnionColumnsDoNotMatchError struct {
		FirstProj  int
		SecondProj int
	}
	ColumnNotFoundError struct {
		Column *sqlparser.ColName
		Table  *sqlparser.TableName
	}
	ColumnNotFoundClauseError struct {
		Column string
		Clause string
	}
)

func eprintf(e error, format string, args ...any) string {
	switch e.(type) {
	case unsupportedError:
		format = "VT12001: unsupported: " + format
	case bugError:
		format = "VT13001: [BUG] " + format
	}
	return fmt.Sprintf(format, args...)
}

func newAmbiguousColumnError(name *sqlparser.ColName) error {
	return &AmbiguousColumnError{Column: sqlparser.String(name)}
}

// Specific error implementations follow

// UnionColumnsDoNotMatchError
func (e *UnionColumnsDoNotMatchError) ErrorState() vterrors.State {
	return vterrors.WrongNumberOfColumnsInSelect
}

func (e *UnionColumnsDoNotMatchError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_FAILED_PRECONDITION
}

func (e *UnionColumnsDoNotMatchError) Error() string {
	return eprintf(e, "The used SELECT statements have a different number of columns: %v, %v", e.FirstProj, e.SecondProj)
}

// UnsupportedMultiTablesInUpdateError
func (e *UnsupportedMultiTablesInUpdateError) Error() string {
	switch {
	case e.NotAlias:
		return eprintf(e, "unaliased multiple tables in update")
	default:
		return eprintf(e, "multiple (%d) tables in update", e.ExprCount)
	}
}

func (e *UnsupportedMultiTablesInUpdateError) unsupported() {}

// UnsupportedNaturalJoinError
func (e *UnsupportedNaturalJoinError) Error() string {
	return eprintf(e, "%s", e.JoinExpr.Join.ToString())
}

func (e *UnsupportedNaturalJoinError) unsupported() {}

// UnionWithSQLCalcFoundRowsError
func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return eprintf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) unsupported() {}

// TableNotUpdatableError
func (e *TableNotUpdatableError) Error() string {
	return eprintf(e, "The target table %s of the UPDATE is not updatable", e.Table)
}

func (e *TableNotUpdatableError) ErrorState() vterrors.State {
	return vterrors.NonUpdateableTable
}

func (e *TableNotUpdatableError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// SQLCalcFoundRowsUsageError
func (e *SQLCalcFoundRowsUsageError) Error() string {
	return eprintf(e, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
}

func (e *SQLCalcFoundRowsUsageError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// CantUseOptionHereError
func (e *CantUseOptionHereError) Error() string {
	return eprintf(e, "Incorrect usage/placement of '%s'", e.Msg)
}

func (e *CantUseOptionHereError) ErrorState() vterrors.State {
	return vterrors.CantUseOptionHere
}

func (e *CantUseOptionHereError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// MissingInVSchemaError
func (e *MissingInVSchemaError) Error() string {
	tableName, _ := e.Table.Name()
	return eprintf(e, "Table information is not provided in vschema for table `%s`", sqlparser.String(tableName))
}

func (e *MissingInVSchemaError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// NotSequenceTableError
func (e *NotSequenceTableError) Error() string {
	return eprintf(e, "NEXT used on a non-sequence table `%s`", e.Table)
}

func (e *NotSequenceTableError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// NextWithMultipleTablesError
func (e *NextWithMultipleTablesError) Error() string {
	return eprintf(e, "Next statement should not contain multiple tables: found %d tables", e.CountTables)
}

func (e *NextWithMultipleTablesError) bug() {}

// LockOnlyWithDualError
func (e *LockOnlyWithDualError) Error() string {
	return eprintf(e, "%v allowed only with dual", sqlparser.String(e.Node))
}

func (e *LockOnlyWithDualError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_UNIMPLEMENTED
}

// QualifiedOrderInUnionError
func (e *QualifiedOrderInUnionError) Error() string {
	return eprintf(e, "Table `%s` from one of the SELECTs cannot be used in global ORDER clause", e.Table)
}

// JSONTablesError
func (e *JSONTablesError) Error() string {
	return eprintf(e, "json_table expressions")
}

func (e *JSONTablesError) unsupported() {}

// BuggyError is used for checking conditions that should never occur
func (e *BuggyError) Error() string {
	return eprintf(e, e.Msg)
}

func (e *BuggyError) bug() {}

// ColumnNotFoundError
func (e ColumnNotFoundError) Error() string {
	if e.Table == nil {
		return eprintf(e, "column '%s' not found", sqlparser.String(e.Column))
	}
	return eprintf(e, "column '%s' not found in table '%s'", sqlparser.String(e.Column), sqlparser.String(e.Table))
}

func (e ColumnNotFoundError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

func (e ColumnNotFoundError) ErrorState() vterrors.State {
	return vterrors.BadFieldError
}

// AmbiguousColumnError
func (e *AmbiguousColumnError) Error() string {
	return eprintf(e, "Column '%s' in field list is ambiguous", e.Column)
}

func (e *AmbiguousColumnError) ErrorState() vterrors.State {
	return vterrors.BadFieldError
}

func (e *AmbiguousColumnError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// UnsupportedConstruct
func (e *UnsupportedConstruct) unsupported() {}

func (e *UnsupportedConstruct) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_UNIMPLEMENTED
}

func (e *UnsupportedConstruct) Error() string {
	return eprintf(e, e.errString)
}

// SubqueryColumnCountError
func (e *SubqueryColumnCountError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

func (e *SubqueryColumnCountError) Error() string {
	return fmt.Sprintf("Operand should contain %d column(s)", e.Expected)
}

// ColumnsMissingInSchemaError
func (e *ColumnsMissingInSchemaError) Error() string {
	return "VT09015: schema tracking required"
}

func (e *ColumnsMissingInSchemaError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// CantUseMultipleVindexHints
func (c *CantUseMultipleVindexHints) Error() string {
	return vterrors.VT09020(c.Table).Error()
}

func (c *CantUseMultipleVindexHints) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_FAILED_PRECONDITION
}

// NoSuchVindexFound
func (c *NoSuchVindexFound) Error() string {
	return vterrors.VT09021(c.VindexName, c.Table).Error()
}

func (c *NoSuchVindexFound) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_FAILED_PRECONDITION
}

// InvalidUseOfGroupFunction
func (*InvalidUseOfGroupFunction) Error() string {
	return "Invalid use of group function"
}

func (*InvalidUseOfGroupFunction) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

func (*InvalidUseOfGroupFunction) ErrorState() vterrors.State {
	return vterrors.InvalidGroupFuncUse
}

// CantGroupOn
func (e *CantGroupOn) Error() string {
	return vterrors.VT03005(e.Column).Error()
}

func (*CantGroupOn) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

func (e *CantGroupOn) ErrorState() vterrors.State {
	return vterrors.VT03005(e.Column).State
}

// ColumnNotFoundInGroupByError
func (e *ColumnNotFoundClauseError) Error() string {
	return fmt.Sprintf("Unknown column '%s' in '%s'", e.Column, e.Clause)
}

func (*ColumnNotFoundClauseError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

func (e *ColumnNotFoundClauseError) ErrorState() vterrors.State {
	return vterrors.BadFieldError
}
