// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/relog"
	"fmt"
)

const (
	FAIL = iota
	RETRY
	FATAL
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
	self := NewTabletError(errorType, "%s", err)
	if sqlErr, ok := err.(hasNumber); ok {
		self.SqlError = sqlErr.Number()
	}
	return self
}

func (self *TabletError) Error() string {
	format := "error: %s"
	switch self.ErrorType {
	case RETRY:
		format = "retry: %s"
	case FATAL:
		format = "fatal: %s"
	}
	return fmt.Sprintf(format, self.Message)
}

func (self *TabletError) RecordStats() {
	switch self.ErrorType {
	case RETRY:
		errorStats.Add("Retry", 1)
	case FATAL:
		errorStats.Add("Fatal", 1)
	default:
		if self.SqlError == DUPLICATE_KEY {
			errorStats.Add("DupKey", 1)
		} else {
			errorStats.Add("Fail", 1)
		}
	}
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*TabletError)
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
