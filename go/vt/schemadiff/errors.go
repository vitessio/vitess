package schemadiff

import "errors"

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

	ErrInvalidColumnInKey                = errors.New("invalid column referenced by key")
	ErrInvalidColumnInGeneratedColumn    = errors.New("invalid column referenced by generated column")
	ErrInvalidColumnInPartition          = errors.New("invalid column referenced by partition")
	ErrMissingPartitionColumnInUniqueKey = errors.New("unique key must include all columns in a partitioning function")
)
