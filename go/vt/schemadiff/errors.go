package schemadiff

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/sqlescape"
)

var (
	ErrEntityTypeMismatch             = errors.New("mismatched entity type")
	ErrStrictIndexOrderingUnsupported = errors.New("strict index ordering is unsupported")
	ErrUnsupportedTableOption         = errors.New("unsupported table option")
	ErrUnexpectedDiffAction           = errors.New("unexpected diff action")
	ErrUnexpectedTableSpec            = errors.New("unexpected table spec")
	ErrNotFullyParsed                 = errors.New("unable to fully parse statement")
	ErrExpectedCreateTable            = errors.New("expected a CREATE TABLE statement")
	ErrExpectedCreateView             = errors.New("expected a CREATE VIEW statement")
	ErrUnsupportedEntity              = errors.New("unsupported entity type")
	ErrUnsupportedStatement           = errors.New("unsupported statement")
	ErrDuplicateName                  = errors.New("duplicate name")
	ErrViewDependencyUnresolved       = errors.New("views have unresolved/loop dependencies")

	ErrUnsupportedApplyOperation = errors.New("unsupported Apply operation")
	ErrApplyTableNotFound        = errors.New("table not found")
	ErrApplyViewNotFound         = errors.New("view not found")
	ErrApplyKeyNotFound          = errors.New("key not found")
	ErrApplyColumnNotFound       = errors.New("column not found")
	ErrApplyDuplicateTableOrView = errors.New("duplicate table or view")
	ErrApplyDuplicateKey         = errors.New("duplicate key")
	ErrApplyDuplicateColumn      = errors.New("duplicate column")
	ErrApplyConstraintNotFound   = errors.New("constraint not found")
	ErrApplyDuplicateConstraint  = errors.New("duplicate constraint")
	ErrApplyPartitionNotFound    = errors.New("partition not found")
	ErrApplyDuplicatePartition   = errors.New("duplicate partition")
	ErrApplyNoPartitions         = errors.New("no partitions found")
)

type InvalidColumnInKeyError struct {
	Table  string
	Column string
	Key    string
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
