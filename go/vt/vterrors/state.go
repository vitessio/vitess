package vterrors

// State is error state
type State int

// All the error states
const (
	Undefined State = iota

	// invalid argument
	BadFieldError
	CantUseOptionHere
	DataOutOfRange
	ForbidSchemaChange
	NonUniqTable
	SyntaxError
	WrongValueForVar

	// failed precondition
	NoDB
	InnodbReadOnly
	WrongNumberOfColumnsInSelect

	// not found
	BadDb
	DbDropExists
	NoSuchTable
	SPDoesNotExist
	UnknownTable

	// already exists
	DbCreateExists

	// resource exhausted
	NetPacketTooLarge

	// cancelled
	QueryInterrupted

	// unimplemented
	NotSupportedYet

	// permission denied
	AccessDeniedError

	// No state should be added below NumOfStates
	NumOfStates
)
