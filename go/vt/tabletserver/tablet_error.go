/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package tabletserver

import (
	"fmt"
	"vitess/relog"
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
