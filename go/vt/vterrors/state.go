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
)
