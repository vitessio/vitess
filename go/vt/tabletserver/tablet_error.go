// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/tb"
)

const (
	FAIL = iota
	RETRY
	FATAL
	TX_POOL_FULL
	NOT_IN_TX
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
	case TX_POOL_FULL:
		format = "tx_pool_full: %s"
	case NOT_IN_TX:
		format = "not_in_tx: %s"
	}
	return fmt.Sprintf(format, te.Message)
}

func (te *TabletError) RecordStats() {
	switch te.ErrorType {
	case RETRY:
		infoErrors.Add("Retry", 1)
	case FATAL:
		infoErrors.Add("Fatal", 1)
	case TX_POOL_FULL:
		errorStats.Add("TxPoolFull", 1)
	case NOT_IN_TX:
		errorStats.Add("NotInTx", 1)
	default:
		switch te.SqlError {
		case mysql.DUP_ENTRY:
			infoErrors.Add("DupKey", 1)
		case mysql.LOCK_WAIT_TIMEOUT, mysql.LOCK_DEADLOCK:
			errorStats.Add("Deadlock", 1)
		default:
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
			log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
			*err = NewTabletError(FAIL, "%v: uncaught panic", x)
			internalErrors.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats()
		if terr.ErrorType == RETRY { // Retry errors are too spammy
			return
		}
		log.Errorf("%s", terr.Message)
	}
}

func logError() {
	if x := recover(); x != nil {
		log.Errorf("%s", x.(*TabletError).Message)
	}
}
