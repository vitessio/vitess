package schemadiff

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/sqlescape"
)

var (
	ErrEntityTypeMismatch             = errors.New("mismatched entity type")
	ErrStrictIndexOrderingUnsupported = errors.New("strict index ordering is unsupported")
	ErrUnexpectedDiffAction           = errors.New("unexpected diff action")
	ErrUnexpectedTableSpec            = errors.New("unexpected table spec")
	ErrExpectedCreateTable            = errors.New("expected a CREATE TABLE statement")
	ErrExpectedCreateView             = errors.New("expected a CREATE VIEW statement")
)

type UnsupportedEntityError struct {
	Entity    string
	Statement string
}

func (e *UnsupportedEntityError) Error() string {
	return fmt.Sprintf("entity %s is not supported: %s", sqlescape.EscapeID(e.Entity), e.Statement)
}

type NotFullyParsedError struct {
	Entity    string
	Statement string
}

func (e *NotFullyParsedError) Error() string {
	return fmt.Sprintf("entity %s is not fully parsed: %s", sqlescape.EscapeID(e.Entity), e.Statement)
}

type UnsupportedTableOptionError struct {
	Table  string
	Option string
}

func (e *UnsupportedTableOptionError) Error() string {
	return fmt.Sprintf("unsupported option %s on table %s", e.Option, sqlescape.EscapeID(e.Table))
}

type UnsupportedStatementError struct {
	Statement string
}

func (e *UnsupportedStatementError) Error() string {
	return fmt.Sprintf("unsupported statement: %s", e.Statement)
}

type UnsupportedApplyOperationError struct {
	Statement string
}

func (e *UnsupportedApplyOperationError) Error() string {
	return fmt.Sprintf("unsupported operation: %s", e.Statement)
}

type ApplyTableNotFoundError struct {
	Table string
}

func (e *ApplyTableNotFoundError) Error() string {
	return fmt.Sprintf("table %s not found", sqlescape.EscapeID(e.Table))
}

type ApplyViewNotFoundError struct {
	View string
}

func (e *ApplyViewNotFoundError) Error() string {
	return fmt.Sprintf("view %s not found", sqlescape.EscapeID(e.View))
}

type ApplyKeyNotFoundError struct {
	Table string
	Key   string
}

func (e *ApplyKeyNotFoundError) Error() string {
	return fmt.Sprintf("key %s not found in table %s", sqlescape.EscapeID(e.Key), sqlescape.EscapeID(e.Table))
}

type ApplyColumnNotFoundError struct {
	Table  string
	Column string
}

func (e *ApplyColumnNotFoundError) Error() string {
	return fmt.Sprintf("column %s not found in table %s", sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.Table))
}

type ApplyColumnAfterNotFoundError struct {
	Table       string
	Column      string
	AfterColumn string
}

func (e *ApplyColumnAfterNotFoundError) Error() string {
	return fmt.Sprintf("column %s can't be after non-existing column %s in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.AfterColumn), sqlescape.EscapeID(e.Table))
}

type ApplyDuplicateEntityError struct {
	Entity string
}

func (e *ApplyDuplicateEntityError) Error() string {
	return fmt.Sprintf("duplicate entity %s", sqlescape.EscapeID(e.Entity))
}

type ApplyDuplicateKeyError struct {
	Table string
	Key   string
}

func (e *ApplyDuplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key %s in table %s", sqlescape.EscapeID(e.Key), sqlescape.EscapeID(e.Table))
}

type ApplyDuplicateColumnError struct {
	Table  string
	Column string
}

func (e *ApplyDuplicateColumnError) Error() string {
	return fmt.Sprintf("duplicate column %s in table %s", sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.Table))
}

type ApplyConstraintNotFoundError struct {
	Table      string
	Constraint string
}

func (e *ApplyConstraintNotFoundError) Error() string {
	return fmt.Sprintf("constraint %s not found in table %s", sqlescape.EscapeID(e.Constraint), sqlescape.EscapeID(e.Table))
}

type ApplyDuplicateConstraintError struct {
	Table      string
	Constraint string
}

func (e *ApplyDuplicateConstraintError) Error() string {
	return fmt.Sprintf("duplicate constraint %s in table %s", sqlescape.EscapeID(e.Constraint), sqlescape.EscapeID(e.Table))
}

type ApplyPartitionNotFoundError struct {
	Table     string
	Partition string
}

func (e *ApplyPartitionNotFoundError) Error() string {
	return fmt.Sprintf("partition %s not found in table %s", sqlescape.EscapeID(e.Partition), sqlescape.EscapeID(e.Table))
}

type ApplyDuplicatePartitionError struct {
	Table     string
	Partition string
}

func (e *ApplyDuplicatePartitionError) Error() string {
	return fmt.Sprintf("duplicate partition %s in table %s", sqlescape.EscapeID(e.Partition), sqlescape.EscapeID(e.Table))
}

type ApplyNoPartitionsError struct {
	Table string
}

func (e *ApplyNoPartitionsError) Error() string {
	return fmt.Sprintf("no partitions in table %s", sqlescape.EscapeID(e.Table))
}

type InvalidColumnInKeyError struct {
	Table  string
	Column string
	Key    string
}

type DuplicateKeyNameError struct {
	Table string
	Key   string
}

func (e *DuplicateKeyNameError) Error() string {
	return fmt.Sprintf("duplicate key %s in table %s", sqlescape.EscapeID(e.Key), sqlescape.EscapeID(e.Table))
}

func (e *InvalidColumnInKeyError) Error() string {
	return fmt.Sprintf("invalid column %s referenced by key %s in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.Key), sqlescape.EscapeID(e.Table))
}

type InvalidColumnInGeneratedColumnError struct {
	Table           string
	Column          string
	GeneratedColumn string
}

func (e *InvalidColumnInGeneratedColumnError) Error() string {
	return fmt.Sprintf("invalid column %s referenced by generated column %s in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.GeneratedColumn), sqlescape.EscapeID(e.Table))
}

type InvalidColumnInPartitionError struct {
	Table  string
	Column string
}

func (e *InvalidColumnInPartitionError) Error() string {
	return fmt.Sprintf("invalid column %s referenced by partition in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.Table))
}

type MissingPartitionColumnInUniqueKeyError struct {
	Table     string
	Column    string
	UniqueKey string
}

func (e *MissingPartitionColumnInUniqueKeyError) Error() string {
	return fmt.Sprintf("invalid column %s referenced by unique key %s in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.UniqueKey), sqlescape.EscapeID(e.Table))
}

type InvalidColumnInCheckConstraintError struct {
	Table      string
	Constraint string
	Column     string
}

func (e *InvalidColumnInCheckConstraintError) Error() string {
	return fmt.Sprintf("invalid column %s referenced by check constraint %s in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.Constraint), sqlescape.EscapeID(e.Table))
}

type ForeignKeyDependencyUnresolvedError struct {
	Table string
}

func (e *ForeignKeyDependencyUnresolvedError) Error() string {
	return fmt.Sprintf("table %s has unresolved/loop foreign key dependencies",
		sqlescape.EscapeID(e.Table))
}

type InvalidColumnInForeignKeyConstraintError struct {
	Table      string
	Constraint string
	Column     string
}

func (e *InvalidColumnInForeignKeyConstraintError) Error() string {
	return fmt.Sprintf("invalid column %s covered by foreign key constraint %s in table %s",
		sqlescape.EscapeID(e.Column), sqlescape.EscapeID(e.Constraint), sqlescape.EscapeID(e.Table))
}

type InvalidReferencedColumnInForeignKeyConstraintError struct {
	Table            string
	Constraint       string
	ReferencedTable  string
	ReferencedColumn string
}

func (e *InvalidReferencedColumnInForeignKeyConstraintError) Error() string {
	return fmt.Sprintf("invalid column %s.%s referenced by foreign key constraint %s in table %s",
		sqlescape.EscapeID(e.ReferencedTable), sqlescape.EscapeID(e.ReferencedColumn), sqlescape.EscapeID(e.Constraint), sqlescape.EscapeID(e.Table))
}

type ForeignKeyColumnCountMismatchError struct {
	Table                 string
	Constraint            string
	ColumnCount           int
	ReferencedTable       string
	ReferencedColumnCount int
}

func (e *ForeignKeyColumnCountMismatchError) Error() string {
	return fmt.Sprintf("mismatching column count %d referenced by foreign key constraint %s in table %s. Expected %d",
		e.ReferencedColumnCount, sqlescape.EscapeID(e.Constraint), sqlescape.EscapeID(e.Table), e.ColumnCount)
}

type ForeignKeyColumnTypeMismatchError struct {
	Table            string
	Constraint       string
	Column           string
	ReferencedTable  string
	ReferencedColumn string
}

func (e *ForeignKeyColumnTypeMismatchError) Error() string {
	return fmt.Sprintf("mismatching column type %s.%s and %s.%s referenced by foreign key constraint %s in table %s",
		sqlescape.EscapeID(e.ReferencedTable),
		sqlescape.EscapeID(e.ReferencedColumn),
		sqlescape.EscapeID(e.Table),
		sqlescape.EscapeID(e.Column),
		sqlescape.EscapeID(e.Constraint),
		sqlescape.EscapeID(e.Table),
	)
}

type MissingForeignKeyReferencedIndexError struct {
	Table           string
	Constraint      string
	ReferencedTable string
}

func (e *MissingForeignKeyReferencedIndexError) Error() string {
	return fmt.Sprintf("missing index in referenced table %s for foreign key constraint %s in table %s",
		sqlescape.EscapeID(e.ReferencedTable),
		sqlescape.EscapeID(e.Constraint),
		sqlescape.EscapeID(e.Table),
	)
}

type IndexNeededByForeignKeyError struct {
	Table string
	Key   string
}

func (e *IndexNeededByForeignKeyError) Error() string {
	return fmt.Sprintf("key %s needed by a foreign key constraint in table %s",
		sqlescape.EscapeID(e.Key),
		sqlescape.EscapeID(e.Table),
	)
}

type ViewDependencyUnresolvedError struct {
	View string
}

func (e *ViewDependencyUnresolvedError) Error() string {
	return fmt.Sprintf("view %s has unresolved/loop dependencies", sqlescape.EscapeID(e.View))
}
