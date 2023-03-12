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

package mysql

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// SQLError is the error structure returned from calling a db library function
type SQLError struct {
	Num     ErrorCode
	State   string
	Message string
	Query   string
}

// NewSQLError creates a new SQLError.
// If sqlState is left empty, it will default to "HY000" (general error).
// TODO: Should be aligned with vterrors, stack traces and wrapping
func NewSQLError(number ErrorCode, sqlState string, format string, args ...any) *SQLError {
	if sqlState == "" {
		sqlState = SSUnknownSQLState
	}
	return &SQLError{
		Num:     number,
		State:   sqlState,
		Message: fmt.Sprintf(format, args...),
	}
}

// newUnknownSQLError creates a new SQLError with no specific error code or state
func newUnknownSQLError(format string, args ...any) *SQLError {
	return &SQLError{
		Num:     ERUnknownError,
		State:   SSUnknownSQLState,
		Message: fmt.Sprintf(format, args...),
	}
}

// Error implements the error interface
func (se *SQLError) Error() string {
	buf := &bytes.Buffer{}
	buf.WriteString(se.Message)

	// Add MySQL errno and SQLSTATE in a format that we can later parse.
	// There's no avoiding string parsing because all errors
	// are converted to strings anyway at RPC boundaries.
	// See NewSQLErrorFromError.
	fmt.Fprintf(buf, " (errno %v) (sqlstate %v)", se.Num, se.State)

	if se.Query != "" {
		fmt.Fprintf(buf, " during query: %s", sqlparser.TruncateForLog(se.Query))
	}

	return buf.String()
}

// Number returns the internal MySQL error code.
func (se *SQLError) Number() ErrorCode {
	return se.Num
}

// SQLState returns the SQLSTATE value.
func (se *SQLError) SQLState() string {
	return se.State
}

var errExtract = regexp.MustCompile(`.*\(errno ([0-9]*)\) \(sqlstate ([0-9a-zA-Z]{5})\).*`)

// NewSQLErrorFromError returns a *SQLError from the provided error.
// If it's not the right type, it still tries to get it from a regexp, or from vitess error code.
// If there's absolutely no path to extracting a sensible SQL error information from the error,
// the function returns (<unknown>, false).
// If the input is `nil` then the result is (nil, false)
func NewSQLErrorFromError(err error) (sqlError *SQLError, ok bool) {
	if err == nil {
		return nil, false
	}

	if serr, ok := err.(*SQLError); ok {
		return serr, true
	}

	unwrapped := vterrors.UnwrapAll(err)
	if serr, ok := unwrapped.(*SQLError); ok {
		// we don't just return 'unwrapped' because the wrapping errors decorate the message with
		// important details. So we use the unwrapped info, and the outer error's message
		return NewSQLError(serr.Num, serr.State, err.Error()), true
	}

	if sqlErr, ok := convertToMysqlError(err); ok {
		return sqlErr, true
	}

	msg := err.Error()
	match := errExtract.FindStringSubmatch(msg)
	if len(match) >= 2 {
		return extractSQLErrorFromMessage(match, msg)
	}

	return mapToSQLErrorFromErrorCode(err, msg)
}

// ForceNewSQLErrorFromError returns a SQLError obejct at all costs. It first attempts to extract
// sensible information from the error, via NewSQLErrorFromError(). But if that's unsuccessful, it
// returns a SQLError with _undefined_ sql num&state.
func ForceNewSQLErrorFromError(err error) *SQLError {
	if err == nil {
		return nil
	}
	if sqlErr, ok := NewSQLErrorFromError(err); ok {
		return sqlErr
	}
	return newUnknownSQLError(err.Error())
}

func extractSQLErrorFromMessage(match []string, msg string) (sqlError *SQLError, ok bool) {
	num, err := strconv.ParseUint(match[1], 10, 16)
	if err != nil {
		return nil, false
	}

	return &SQLError{
		Num:     ErrorCode(num),
		State:   match[2],
		Message: msg,
	}, true
}

func mapToSQLErrorFromErrorCode(err error, msg string) (sqlError *SQLError, ok bool) {
	// Map vitess error codes into the mysql equivalent
	var num ErrorCode
	var ss string

	switch vterrors.Code(err) {
	case vtrpcpb.Code_CANCELED, vtrpcpb.Code_DEADLINE_EXCEEDED, vtrpcpb.Code_ABORTED:
		num = ERQueryInterrupted
		ss = SSQueryInterrupted
	case vtrpcpb.Code_PERMISSION_DENIED, vtrpcpb.Code_UNAUTHENTICATED:
		num = ERAccessDeniedError
		ss = SSAccessDeniedError
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		num = demuxResourceExhaustedErrors(err.Error())
		ss = SSClientError
	case vtrpcpb.Code_UNIMPLEMENTED:
		num = ERNotSupportedYet
		ss = SSClientError
	case vtrpcpb.Code_INTERNAL:
		num = ERInternalError
		ss = SSUnknownSQLState
	default:
		return nil, false
	}

	// Not found, build a generic SQLError.
	return &SQLError{
		Num:     num,
		State:   ss,
		Message: msg,
	}, true
}

type mysqlCode struct {
	num   ErrorCode
	state string
}

var stateToMysqlCode = map[vterrors.State]mysqlCode{
	vterrors.Undefined:                    {num: ERUnknownError, state: SSUnknownSQLState},
	vterrors.AccessDeniedError:            {num: ERAccessDeniedError, state: SSAccessDeniedError},
	vterrors.BadDb:                        {num: ERBadDb, state: SSClientError},
	vterrors.BadFieldError:                {num: ERBadFieldError, state: SSBadFieldError},
	vterrors.BadTableError:                {num: ERBadTable, state: SSUnknownTable},
	vterrors.CantUseOptionHere:            {num: ERCantUseOptionHere, state: SSClientError},
	vterrors.DataOutOfRange:               {num: ERDataOutOfRange, state: SSDataOutOfRange},
	vterrors.DbCreateExists:               {num: ERDbCreateExists, state: SSUnknownSQLState},
	vterrors.DbDropExists:                 {num: ERDbDropExists, state: SSUnknownSQLState},
	vterrors.DupFieldName:                 {num: ERDupFieldName, state: SSDupFieldName},
	vterrors.EmptyQuery:                   {num: EREmptyQuery, state: SSClientError},
	vterrors.IncorrectGlobalLocalVar:      {num: ERIncorrectGlobalLocalVar, state: SSUnknownSQLState},
	vterrors.InnodbReadOnly:               {num: ERInnodbReadOnly, state: SSUnknownSQLState},
	vterrors.LockOrActiveTransaction:      {num: ERLockOrActiveTransaction, state: SSUnknownSQLState},
	vterrors.NoDB:                         {num: ERNoDb, state: SSNoDB},
	vterrors.NoSuchTable:                  {num: ERNoSuchTable, state: SSUnknownTable},
	vterrors.NotSupportedYet:              {num: ERNotSupportedYet, state: SSClientError},
	vterrors.ForbidSchemaChange:           {num: ERForbidSchemaChange, state: SSUnknownSQLState},
	vterrors.MixOfGroupFuncAndFields:      {num: ERMixOfGroupFuncAndFields, state: SSClientError},
	vterrors.NetPacketTooLarge:            {num: ERNetPacketTooLarge, state: SSNetError},
	vterrors.NonUniqError:                 {num: ERNonUniq, state: SSConstraintViolation},
	vterrors.NonUniqTable:                 {num: ERNonUniqTable, state: SSClientError},
	vterrors.NonUpdateableTable:           {num: ERNonUpdateableTable, state: SSUnknownSQLState},
	vterrors.QueryInterrupted:             {num: ERQueryInterrupted, state: SSQueryInterrupted},
	vterrors.SPDoesNotExist:               {num: ERSPDoesNotExist, state: SSClientError},
	vterrors.SyntaxError:                  {num: ERSyntaxError, state: SSClientError},
	vterrors.UnsupportedPS:                {num: ERUnsupportedPS, state: SSUnknownSQLState},
	vterrors.UnknownSystemVariable:        {num: ERUnknownSystemVariable, state: SSUnknownSQLState},
	vterrors.UnknownTable:                 {num: ERUnknownTable, state: SSUnknownTable},
	vterrors.WrongGroupField:              {num: ERWrongGroupField, state: SSClientError},
	vterrors.WrongNumberOfColumnsInSelect: {num: ERWrongNumberOfColumnsInSelect, state: SSWrongNumberOfColumns},
	vterrors.WrongTypeForVar:              {num: ERWrongTypeForVar, state: SSClientError},
	vterrors.WrongValueForVar:             {num: ERWrongValueForVar, state: SSClientError},
	vterrors.WrongValue:                   {num: ERWrongValue, state: SSUnknownSQLState},
	vterrors.WrongFieldWithGroup:          {num: ERWrongFieldWithGroup, state: SSClientError},
	vterrors.ServerNotAvailable:           {num: ERServerIsntAvailable, state: SSNetError},
	vterrors.CantDoThisInTransaction:      {num: ERCantDoThisDuringAnTransaction, state: SSCantDoThisDuringAnTransaction},
	vterrors.RequiresPrimaryKey:           {num: ERRequiresPrimaryKey, state: SSClientError},
	vterrors.NoSuchSession:                {num: ERUnknownComError, state: SSNetError},
	vterrors.OperandColumns:               {num: EROperandColumns, state: SSWrongNumberOfColumns},
	vterrors.WrongValueCountOnRow:         {num: ERWrongValueCountOnRow, state: SSWrongValueCountOnRow},
}

func getStateToMySQLState(state vterrors.State) mysqlCode {
	if state == 0 {
		return mysqlCode{}
	}
	s := stateToMysqlCode[state]
	return s
}

// ConvertStateToMySQLErrorCode returns MySQL error code for the given vterrors.State
// If the state is == 0, an empty string is returned
func ConvertStateToMySQLErrorCode(state vterrors.State) string {
	s := getStateToMySQLState(state)
	return s.num.ToString()
}

// ConvertStateToMySQLState returns MySQL state for the given vterrors.State
// If the state is == 0, an empty string is returned
func ConvertStateToMySQLState(state vterrors.State) string {
	s := getStateToMySQLState(state)
	return s.state
}

func init() {
	if len(stateToMysqlCode) != int(vterrors.NumOfStates) {
		panic("all vterrors states are not mapped to mysql errors")
	}
}

func convertToMysqlError(err error) (sqlError *SQLError, ok bool) {
	errState := vterrors.ErrState(err)
	if errState == vterrors.Undefined {
		return nil, false
	}
	mysqlCode, ok := stateToMysqlCode[errState]
	if !ok {
		return nil, false
	}
	return NewSQLError(mysqlCode.num, mysqlCode.state, err.Error()), true
}

var isGRPCOverflowRE = regexp.MustCompile(`.*?grpc: (received|trying to send) message larger than max \(\d+ vs. \d+\)`)

func demuxResourceExhaustedErrors(msg string) ErrorCode {
	switch {
	case isGRPCOverflowRE.Match([]byte(msg)):
		return ERNetPacketTooLarge
	default:
		return ERTooManyUserConnections
	}
}
