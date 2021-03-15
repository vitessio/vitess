/*
Copyright 2021 The Vitess Authors.

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
	EmptyQuery
	ForbidSchemaChange
	IncorrectGlobalLocalVar
	NonUniqTable
	SyntaxError
	WrongGroupField
	WrongTypeForVar
	WrongValueForVar
	LockOrActiveTransaction

	// failed precondition
	NoDB
	InnodbReadOnly
	WrongNumberOfColumnsInSelect

	// not found
	BadDb
	DbDropExists
	NoSuchTable
	SPDoesNotExist
	UnknownSystemVariable
	UnknownTable

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

	// No state should be added below NumOfStates
	NumOfStates
)
