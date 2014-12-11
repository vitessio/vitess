// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/mysql"
	"github.com/henryanand/vitess/go/tb"
	"github.com/henryanand/vitess/go/vt/logutil"
)

const (
	FAIL = iota
	RETRY
	FATAL
	TX_POOL_FULL
	NOT_IN_TX
)

var logTxPoolFull = logutil.NewThrottledLogger("TxPoolFull", 1*time.Minute)

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
	return &TabletError{
		ErrorType: errorType,
		Message:   fmt.Sprintf(format, args...),
	}
}

func NewTabletErrorSql(errorType int, err error) *TabletError {
	var errnum int
	errstr := err.Error()
	if sqlErr, ok := err.(hasNumber); ok {
		errnum = sqlErr.Number()
		// Override error type if MySQL is in read-only mode. It's probably because
		// there was a remaster and there are old clients still connected.
		if errnum == mysql.OPTION_PREVENTS_STATEMENT && strings.Contains(errstr, "read-only") {
			errorType = RETRY
		}
	}
	return &TabletError{
		ErrorType: errorType,
		Message:   errstr,
		SqlError:  errnum,
	}
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

func handleError(err *error, logStats *SQLQueryStats) {
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
		if terr.ErrorType == TX_POOL_FULL {
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
		if terr.ErrorType == TX_POOL_FULL {
			logTxPoolFull.Errorf("%v", terr)
		} else {
			log.Errorf("%v", terr)
		}
	}
}
