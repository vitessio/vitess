// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/tb"
)

const (
	FAIL = iota
	RETRY
	FATAL
	TX_POOL_FULL
)

const (
	DUPLICATE_KEY = 1062 // MySQL error number
)

type TabletError struct {
	ErrorType int
	Message   string
	SqlError  int
}

// This is how go-mysql exports its error number
type hasNumber interface {
	Number() int
}

func NewTabletError(errorType int, format string, args ...interface{}) *TabletError {
	return &TabletError{errorType, fmt.Sprintf(format, args...), 0}
}

func NewTabletErrorSql(errorType int, err error) *TabletError {
	te := NewTabletError(errorType, "%s", err)
	if sqlErr, ok := err.(hasNumber); ok {
		te.SqlError = sqlErr.Number()
	}
	return te
}

func (te *TabletError) Error() string {
	format := "error: %s"
	switch te.ErrorType {
	case RETRY:
		format = "retry: %s"
	case FATAL:
		format = "fatal: %s"
	}
	return fmt.Sprintf(format, te.Message)
}

func (te *TabletError) RecordStats() {
	switch te.ErrorType {
	case RETRY:
		errorStats.Add("Retry", 1)
	case FATAL:
		errorStats.Add("Fatal", 1)
	case TX_POOL_FULL:
		errorStats.Add("TxPoolFull", 1)
	default:
		if te.SqlError == DUPLICATE_KEY {
			errorStats.Add("DupKey", 1)
		} else {
			errorStats.Add("Fail", 1)
		}
	}
}

func handleError(err *error, logStats *sqlQueryStats) {
	if logStats != nil {
		logStats.Send()
	}
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			relog.Error("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
			*err = NewTabletError(FAIL, "%v: uncaught panic", x)
			errorStats.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats()
		if terr.ErrorType == RETRY { // Retry errors are too spammy
			return
		}
		relog.Error("%s", terr.Message)
	}
}

func logError() {
	if x := recover(); x != nil {
		relog.Error("%s", x.(*TabletError).Message)
	}
}
