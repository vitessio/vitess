// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletenv

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/tb"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	maxErrLen = 5000
)

// ErrConnPoolClosed is returned / panicked when the connection pool is closed.
var ErrConnPoolClosed = NewTabletError(
	// connection pool being closed is not the query's fault, it can be retried on a
	// different VtTablet.
	vtrpcpb.Code_INTERNAL,
	"connection pool is closed")

// TabletError is the error type we use in this library.
// It implements vterrors.VtError interface.
type TabletError struct {
	Message  string
	SQLError int
	SQLState string
	// Code will be used to transmit the error across RPC boundaries
	Code vtrpcpb.Code
}

// NewTabletError returns a TabletError of the given type
func NewTabletError(errCode vtrpcpb.Code, format string, args ...interface{}) *TabletError {
	return &TabletError{
		Message: printable(fmt.Sprintf(format, args...)),
		Code:    errCode,
	}
}

// NewTabletErrorSQL returns a TabletError based on the error
func NewTabletErrorSQL(errCode vtrpcpb.Code, err error) *TabletError {
	var errnum int
	errstr := err.Error()
	sqlState := sqldb.SQLStateGeneral
	if sqlErr, ok := err.(*sqldb.SQLError); ok {
		errnum = sqlErr.Number()
		sqlState = sqlErr.SQLState()
		switch errnum {
		case mysqlconn.EROptionPreventsStatement:
			// Override error type if MySQL is in read-only mode. It's probably because
			// there was a remaster and there are old clients still connected.
			if strings.Contains(errstr, "read-only") {
				errCode = vtrpcpb.Code_FAILED_PRECONDITION
			}
		case mysqlconn.ERDupEntry:
			errCode = vtrpcpb.Code_ALREADY_EXISTS
		case mysqlconn.ERDataTooLong, mysqlconn.ERDataOutOfRange:
			errCode = vtrpcpb.Code_INVALID_ARGUMENT
		default:
		}
	}
	return &TabletError{
		Message:  printable(errstr),
		SQLError: errnum,
		SQLState: sqlState,
		Code:     errCode,
	}
}

// PrefixTabletError attempts to add a string prefix to a TabletError,
// while preserving its Code. If the given error is not a
// TabletError, a new TabletError is returned with the desired Code.
func PrefixTabletError(errCode vtrpcpb.Code, err error, prefix string) error {
	if terr, ok := err.(*TabletError); ok {
		return NewTabletError(terr.Code, "%s%s", prefix, terr.Message)
	}
	return NewTabletError(errCode, "%s%s", prefix, err)
}

func printable(in string) string {
	if len(in) > maxErrLen {
		in = in[:maxErrLen]
	}
	in = fmt.Sprintf("%q", in)
	return in[1 : len(in)-1]
}

var errExtract = regexp.MustCompile(`.*\(errno ([0-9]*)\).*`)

// IsConnErr returns true if the error is a connection error. If
// the error is of type TabletError or hasNumber, it checks the error
// code. Otherwise, it parses the string looking for (errno xxxx)
// and uses the extracted value to determine if it's a conn error.
func IsConnErr(err error) bool {
	var sqlError int
	switch err := err.(type) {
	case *TabletError:
		sqlError = err.SQLError
	case *sqldb.SQLError:
		sqlError = err.Number()
	default:
		match := errExtract.FindStringSubmatch(err.Error())
		if len(match) < 2 {
			return false
		}
		var convErr error
		sqlError, convErr = strconv.Atoi(match[1])
		if convErr != nil {
			return false
		}
	}
	// CRServerLost means that someone sniped the query.
	if sqlError == mysqlconn.CRServerLost {
		return false
	}
	return sqlError >= 2000 && sqlError <= 2018
}

func (te *TabletError) Error() string {
	return te.Prefix() + te.Message
}

// VtErrorCode returns the underlying Vitess error code
func (te *TabletError) VtErrorCode() vtrpcpb.Code {
	return te.Code
}

// Prefix returns the prefix for the error, like error, fatal, etc.
func (te *TabletError) Prefix() string {
	prefix := "error: "
	switch te.Code {
	case vtrpcpb.Code_FAILED_PRECONDITION:
		prefix = "retry: "
	case vtrpcpb.Code_INTERNAL:
		prefix = "fatal: "
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		prefix = "tx_pool_full: "
	case vtrpcpb.Code_ABORTED:
		prefix = "not_in_tx: "
	}
	// Special case for killed queries.
	if te.SQLError == mysqlconn.CRServerLost {
		prefix = prefix + "the query was killed either because it timed out or was canceled: "
	}
	return prefix
}

// RecordStats will record the error in the proper stat bucket
func (te *TabletError) RecordStats() {
	switch te.Code {
	case vtrpcpb.Code_FAILED_PRECONDITION:
		InfoErrors.Add("Retry", 1)
	case vtrpcpb.Code_INTERNAL:
		ErrorStats.Add("Fatal", 1)
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		ErrorStats.Add("TxPoolFull", 1)
	case vtrpcpb.Code_ABORTED:
		ErrorStats.Add("NotInTx", 1)
	default:
		switch te.SQLError {
		case mysqlconn.ERDupEntry:
			InfoErrors.Add("DupKey", 1)
		case mysqlconn.ERLockWaitTimeout, mysqlconn.ERLockDeadlock:
			ErrorStats.Add("Deadlock", 1)
		default:
			ErrorStats.Add("Fail", 1)
		}
	}
}

// LogError logs panics and increments InternalErrors.
func LogError() {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		InternalErrors.Add("Panic", 1)
	}
}
