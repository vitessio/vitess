package vterrors

// State is error state
type State int

// All the error states
const (
	Undefined State = iota
	DataOutOfRange
	NoDB
	WrongNumberOfColumnsInSelect
	BadFieldError
	DbDropExists
	DbCreateExists
	ForbidSchemaChange
	NetPacketTooLarge
	SPDoesNotExist
	QueryInterrupted
	CantUseOptionHere
	NonUniqTable
	BadDb

	// No state should be added below NumOfStates
	NumOfStates
)
