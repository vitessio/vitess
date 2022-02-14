package vterrors

// State is error state
type State int

// All the error states
const (
	Undefined State = iota

	// invalid argument
	BadFieldError
	BadTableError
	CantUseOptionHere
	DataOutOfRange
	EmptyQuery
	ForbidSchemaChange
	IncorrectGlobalLocalVar
	NonUniqError
	NonUniqTable
	NonUpdateableTable
	SyntaxError
	WrongFieldWithGroup
	WrongGroupField
	WrongTypeForVar
	WrongValueForVar
	LockOrActiveTransaction
	MixOfGroupFuncAndFields
	DupFieldName

	// failed precondition
	NoDB
	InnodbReadOnly
	WrongNumberOfColumnsInSelect
	CantDoThisInTransaction
	RequiresPrimaryKey
	OperandColumns

	// not found
	BadDb
	DbDropExists
	NoSuchTable
	SPDoesNotExist
	UnknownSystemVariable
	UnknownTable
	NoSuchSession

	// already exists
	DbCreateExists

	// resource exhausted
	NetPacketTooLarge

	// cancelled
	QueryInterrupted

	// unimplemented
	NotSupportedYet
	UnsupportedPS

	// permission denied
	AccessDeniedError

	// server not available
	ServerNotAvailable

	// No state should be added below NumOfStates
	NumOfStates
)
