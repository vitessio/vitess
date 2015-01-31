// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/logutil"
)

const (
	// ErrFail is returned when a query fails
	ErrFail = iota

	// ErrRetry is returned when a query can be retried
	ErrRetry

	// ErrFatal is returned when a query cannot be retried
	ErrFatal

	// ErrTxPoolFull is returned when we can't get a connection
	ErrTxPoolFull

	// ErrNotInTx is returned when we're not in a transaction but should be
	ErrNotInTx
)

var logTxPoolFull = logutil.NewThrottledLogger("TxPoolFull", 1*time.Minute)

// TabletError is the erro type we use in this library
type TabletError struct {
	ErrorType int
	Message   string
	SqlError  int
}

// This is how go-mysql exports its error number
type hasNumber interface {
	Number() int
}

// NewTabletError returns a TabletError of the given type
func NewTabletError(errorType int, format string, args ...interface{}) *TabletError {
	return &TabletError{
		ErrorType: errorType,
		Message:   fmt.Sprintf(format, args...),
	}
}

// NewTabletErrorSql returns a TabletError based on the error
func NewTabletErrorSql(errorType int, err error) *TabletError {
	var errnum int
	errstr := err.Error()
	if sqlErr, ok := err.(hasNumber); ok {
		errnum = sqlErr.Number()
		// Override error type if MySQL is in read-only mode. It's probably because
		// there was a remaster and there are old clients still connected.
		if errnum == mysql.ErrOptionPreventsStatement && strings.Contains(errstr, "read-only") {
			errorType = ErrRetry
		}
	}
	return &TabletError{
		ErrorType: errorType,
		Message:   errstr,
		SqlError:  errnum,
	}
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
		sqlError = err.SqlError
	case hasNumber:
		sqlError = err.Number()
	default:
		match := errExtract.FindStringSubmatch(err.Error())
		if match != nil {
			return false
		}
		var convErr error
		sqlError, convErr = strconv.Atoi(match[1])
		if convErr != nil {
			return false
		}
	}
	// 2013 means that someone sniped the query.
	if sqlError == 2013 {
		return false
	}
	return sqlError >= 2000 && sqlError <= 2018
}

func (te *TabletError) Error() string {
	format := "error: %s"
	switch te.ErrorType {
	case ErrRetry:
		format = "retry: %s"
	case ErrFatal:
		format = "fatal: %s"
	case ErrTxPoolFull:
		format = "tx_pool_full: %s"
	case ErrNotInTx:
		format = "not_in_tx: %s"
	}
	return fmt.Sprintf(format, te.Message)
}

// RecordStats will record the error in the proper stat bucket
func (te *TabletError) RecordStats() {
	switch te.ErrorType {
	case ErrRetry:
		infoErrors.Add("Retry", 1)
	case ErrFatal:
		infoErrors.Add("Fatal", 1)
	case ErrTxPoolFull:
		errorStats.Add("TxPoolFull", 1)
	case ErrNotInTx:
		errorStats.Add("NotInTx", 1)
	default:
		switch te.SqlError {
		case mysql.ErrDupEntry:
			infoErrors.Add("DupKey", 1)
		case mysql.ErrLockWaitTimeout, mysql.ErrLockDeadlock:
			errorStats.Add("Deadlock", 1)
		default:
			errorStats.Add("Fail", 1)
		}
	}
}

func handleError(err *error, logStats *SQLQueryStats) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
			*err = NewTabletError(ErrFail, "%v: uncaught panic", x)
			internalErrors.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats()
		if terr.ErrorType == ErrRetry { // Retry errors are too spammy
			return
		}
		if terr.ErrorType == ErrTxPoolFull {
			logTxPoolFull.Errorf("%v", terr)
		} else {
			log.Errorf("%v", terr)
		}
	}
	if logStats != nil {
		logStats.Error = *err
		logStats.Send()
	}
}

func logError() {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
			internalErrors.Add("Panic", 1)
			return
		}
		if terr.ErrorType == ErrTxPoolFull {
			logTxPoolFull.Errorf("%v", terr)
		} else {
			log.Errorf("%v", terr)
		}
	}
}
