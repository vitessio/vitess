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
	Num     int
	State   string
	Message string
	Query   string
}

// NewSQLError creates a new SQLError.
// If sqlState is left empty, it will default to "HY000" (general error).
// TODO: Should be aligned with vterrors, stack traces and wrapping
func NewSQLError(number int, sqlState string, format string, args ...interface{}) *SQLError {
	if sqlState == "" {
		sqlState = SSUnknownSQLState
	}
	return &SQLError{
		Num:     number,
		State:   sqlState,
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
func (se *SQLError) Number() int {
	return se.Num
}

// SQLState returns the SQLSTATE value.
func (se *SQLError) SQLState() string {
	return se.State
}

var errExtract = regexp.MustCompile(`.*\(errno ([0-9]*)\) \(sqlstate ([0-9a-zA-Z]{5})\).*`)

// NewSQLErrorFromError returns a *SQLError from the provided error.
// If it's not the right type, it still tries to get it from a regexp.
func NewSQLErrorFromError(err error) error {
	if err == nil {
		return nil
	}

	if serr, ok := err.(*SQLError); ok {
		return serr
	}

	sErr := convertToMysqlError(err)
	if _, ok := sErr.(*SQLError); ok {
		return sErr
	}

	msg := err.Error()
	match := errExtract.FindStringSubmatch(msg)
	if len(match) < 2 {
		// Map vitess error codes into the mysql equivalent
		code := vterrors.Code(err)
		num := ERUnknownError
		ss := SSUnknownSQLState
		switch code {
		case vtrpcpb.Code_CANCELED, vtrpcpb.Code_DEADLINE_EXCEEDED, vtrpcpb.Code_ABORTED:
			num = ERQueryInterrupted
			ss = SSQueryInterrupted
		case vtrpcpb.Code_UNKNOWN, vtrpcpb.Code_INVALID_ARGUMENT, vtrpcpb.Code_NOT_FOUND, vtrpcpb.Code_ALREADY_EXISTS,
			vtrpcpb.Code_FAILED_PRECONDITION, vtrpcpb.Code_OUT_OF_RANGE, vtrpcpb.Code_UNAVAILABLE, vtrpcpb.Code_DATA_LOSS:
			num = ERUnknownError
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
		}

		// Not found, build a generic SQLError.
		return &SQLError{
			Num:     num,
			State:   ss,
			Message: msg,
		}
	}

	num, err := strconv.Atoi(match[1])
	if err != nil {
		return &SQLError{
			Num:     ERUnknownError,
			State:   SSUnknownSQLState,
			Message: msg,
		}
	}

	serr := &SQLError{
		Num:     num,
		State:   match[2],
		Message: msg,
	}
	return serr
}

var stateToMysqlCode = map[vterrors.State]struct {
	num   int
	state string
}{
	vterrors.Undefined:                    {num: ERUnknownError, state: SSUnknownSQLState},
	vterrors.AccessDeniedError:            {num: ERAccessDeniedError, state: SSAccessDeniedError},
	vterrors.BadDb:                        {num: ERBadDb, state: SSClientError},
	vterrors.BadFieldError:                {num: ERBadFieldError, state: SSBadFieldError},
	vterrors.CantUseOptionHere:            {num: ERCantUseOptionHere, state: SSClientError},
	vterrors.DataOutOfRange:               {num: ERDataOutOfRange, state: SSDataOutOfRange},
	vterrors.DbCreateExists:               {num: ERDbCreateExists, state: SSUnknownSQLState},
	vterrors.DbDropExists:                 {num: ERDbDropExists, state: SSUnknownSQLState},
	vterrors.EmptyQuery:                   {num: EREmptyQuery, state: SSClientError},
	vterrors.IncorrectGlobalLocalVar:      {num: ERIncorrectGlobalLocalVar, state: SSUnknownSQLState},
	vterrors.InnodbReadOnly:               {num: ERInnodbReadOnly, state: SSUnknownSQLState},
	vterrors.LockOrActiveTransaction:      {num: ERLockOrActiveTransaction, state: SSUnknownSQLState},
	vterrors.NoDB:                         {num: ERNoDb, state: SSNoDB},
	vterrors.NoSuchTable:                  {num: ERNoSuchTable, state: SSUnknownTable},
	vterrors.NotSupportedYet:              {num: ERNotSupportedYet, state: SSClientError},
	vterrors.ForbidSchemaChange:           {num: ERForbidSchemaChange, state: SSUnknownSQLState},
	vterrors.NetPacketTooLarge:            {num: ERNetPacketTooLarge, state: SSNetError},
	vterrors.NonUniqTable:                 {num: ERNonUniqTable, state: SSClientError},
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
}

func init() {
	if len(stateToMysqlCode) != int(vterrors.NumOfStates) {
		panic("all vterrors states are not mapped to mysql errors")
	}
}

func convertToMysqlError(err error) error {
	errState := vterrors.ErrState(err)
	if errState == vterrors.Undefined {
		return err
	}
	mysqlCode, ok := stateToMysqlCode[errState]
	if !ok {
		return err
	}
	return NewSQLError(mysqlCode.num, mysqlCode.state, err.Error())
}

var isGRPCOverflowRE = regexp.MustCompile(`.*grpc: received message larger than max \(\d+ vs. \d+\)`)

func demuxResourceExhaustedErrors(msg string) int {
	switch {
	case isGRPCOverflowRE.Match([]byte(msg)):
		return ERNetPacketTooLarge
	default:
		return ERTooManyUserConnections
	}
}
