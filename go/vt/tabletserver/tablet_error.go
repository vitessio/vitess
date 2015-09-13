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

	"google.golang.org/grpc"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vterrors"
)

const (
	// ErrFail is returned when a query fails, and we think it's because the query
	// itself is problematic. That means that the query will not  be retried.
	ErrFail = iota

	// ErrRetry is returned when a query can be retried
	ErrRetry

	// ErrFatal is returned when a query fails due to some internal state, but we
	// don't suspect the query itself to be bad. The query can be retried by VtGate
	// to a different VtTablet (in case a different tablet is healthier), but
	// probably shouldn't be retried by clients.
	ErrFatal

	// ErrTxPoolFull is returned when we can't get a connection
	ErrTxPoolFull

	// ErrNotInTx is returned when we're not in a transaction but should be
	ErrNotInTx
)

const (
	maxErrLen = 5000
)

// ErrConnPoolClosed is returned / panicked when the connection pool is closed.
var ErrConnPoolClosed = NewTabletError(ErrFatal,
	// connection pool being closed is not the query's fault, it can be retried on a
	// different VtTablet.
	vtrpc.ErrorCode_INTERNAL_ERROR,
	"connection pool is closed")

var logTxPoolFull = logutil.NewThrottledLogger("TxPoolFull", 1*time.Minute)

// TabletError is the error type we use in this library
type TabletError struct {
	ErrorType int
	Message   string
	SQLError  int
	// ErrorCode will be used to transmit the error across RPC boundaries
	ErrorCode vtrpc.ErrorCode
}

// This is how go-mysql exports its error number
type hasNumber interface {
	Number() int
}

// NewTabletError returns a TabletError of the given type
func NewTabletError(errorType int, errCode vtrpc.ErrorCode, format string, args ...interface{}) *TabletError {
	return &TabletError{
		ErrorType: errorType,
		Message:   printable(fmt.Sprintf(format, args...)),
		ErrorCode: errCode,
	}
}

// NewTabletErrorSQL returns a TabletError based on the error
func NewTabletErrorSQL(errorType int, errCode vtrpc.ErrorCode, err error) *TabletError {
	var errnum int
	errstr := err.Error()
	if sqlErr, ok := err.(hasNumber); ok {
		errnum = sqlErr.Number()
		switch errnum {
		case mysql.ErrOptionPreventsStatement:
			// Override error type if MySQL is in read-only mode. It's probably because
			// there was a remaster and there are old clients still connected.
			if strings.Contains(errstr, "read-only") {
				errorType = ErrRetry
				errCode = vtrpc.ErrorCode_QUERY_NOT_SERVED
			}
		case mysql.ErrDupEntry:
			errCode = vtrpc.ErrorCode_INTEGRITY_ERROR
		default:
		}
	}
	return &TabletError{
		ErrorType: errorType,
		Message:   printable(errstr),
		SQLError:  errnum,
		ErrorCode: errCode,
	}
}

// PrefixTabletError attempts to add a string prefix to a TabletError, while preserving its
// ErrorType. If the given error is not a TabletError, a new TabletError is returned
// with the desired ErrorType.
func PrefixTabletError(errorType int, errCode vtrpc.ErrorCode, err error, prefix string) error {
	if terr, ok := err.(*TabletError); ok {
		return NewTabletError(terr.ErrorType, terr.ErrorCode, "%s%s", prefix, terr.Message)
	}
	return NewTabletError(errorType, errCode, "%s%s", prefix, err)
}

// ToGRPCError returns a TabletError as a grpc error, with the
// appropriate error code. This function lives here, instead of in vterrors,
// so that the vterrors package doesn't have to import tabletserver.
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}

	code := grpc.Code(err)
	if tErr, ok := err.(*TabletError); ok {
		// If we got a TabletError, prefer its error code
		code = vterrors.ErrorCodeToGRPCCode(tErr.ErrorCode)
	}

	return grpc.Errorf(code, "%v %v", vterrors.GRPCServerErrPrefix, err)
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
	case hasNumber:
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
	// ErrServerLost means that someone sniped the query.
	if sqlError == mysql.ErrServerLost {
		return false
	}
	return sqlError >= 2000 && sqlError <= 2018
}

func (te *TabletError) Error() string {
	return te.Prefix() + te.Message
}

// VtErrorCode returns the underlying Vitess error code
func (te *TabletError) VtErrorCode() vtrpc.ErrorCode {
	return te.ErrorCode
}

// Prefix returns the prefix for the error, like error, fatal, etc.
func (te *TabletError) Prefix() string {
	prefix := "error: "
	switch te.ErrorType {
	case ErrRetry:
		prefix = "retry: "
	case ErrFatal:
		prefix = "fatal: "
	case ErrTxPoolFull:
		prefix = "tx_pool_full: "
	case ErrNotInTx:
		prefix = "not_in_tx: "
	}
	// Special case for killed queries.
	if te.SQLError == mysql.ErrServerLost {
		prefix = prefix + "the query was killed either because it timed out or was canceled: "
	}
	return prefix
}

// RecordStats will record the error in the proper stat bucket
func (te *TabletError) RecordStats(queryServiceStats *QueryServiceStats) {
	switch te.ErrorType {
	case ErrRetry:
		queryServiceStats.InfoErrors.Add("Retry", 1)
	case ErrFatal:
		queryServiceStats.InfoErrors.Add("Fatal", 1)
	case ErrTxPoolFull:
		queryServiceStats.ErrorStats.Add("TxPoolFull", 1)
	case ErrNotInTx:
		queryServiceStats.ErrorStats.Add("NotInTx", 1)
	default:
		switch te.SQLError {
		case mysql.ErrDupEntry:
			queryServiceStats.InfoErrors.Add("DupKey", 1)
		case mysql.ErrLockWaitTimeout, mysql.ErrLockDeadlock:
			queryServiceStats.ErrorStats.Add("Deadlock", 1)
		default:
			queryServiceStats.ErrorStats.Add("Fail", 1)
		}
	}
}

func handleError(err *error, logStats *LogStats, queryServiceStats *QueryServiceStats) {
	terr := TabletError{}
	defer func() {
		if logStats != nil {
			logStats.Error = terr
			logStats.Send()
		}
	}()
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
			terr = NewTabletError(ErrFail, vtrpc.ErrorCode_UNKNOWN_ERROR, "%v: uncaught panic", x)
			*err = terr
			queryServiceStats.InternalErrors.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats(queryServiceStats)
		switch terr.ErrorType {
		case ErrRetry: // Retry errors are too spammy
			return
		case ErrTxPoolFull:
			logTxPoolFull.Errorf("%v", terr)
		default:
			switch terr.SQLError {
			// MySQL deadlock errors are (usually) due to client behavior, not server
			// behavior, and therefore logged at the INFO level.
			case mysql.ErrLockWaitTimeout, mysql.ErrLockDeadlock, mysql.ErrDataTooLong, mysql.ErrDataOutOfRange:
				log.Infof("%v", terr)
			default:
				log.Errorf("%v", terr)
			}
		}
	}
}

func logError(queryServiceStats *QueryServiceStats) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
			queryServiceStats.InternalErrors.Add("Panic", 1)
			return
		}
		if terr.ErrorType == ErrTxPoolFull {
			logTxPoolFull.Errorf("%v", terr)
		} else {
			log.Errorf("%v", terr)
		}
	}
}

// rpcErrFromTabletError translate a TabletError to an *mproto.RPCError
func rpcErrFromTabletError(err error) *mproto.RPCError {
	if err == nil {
		return nil
	}
	terr, ok := err.(*TabletError)
	if ok {
		return &mproto.RPCError{
			Code: int64(terr.ErrorCode),
			// Make sure the the VitessError message is identical to the TabletError
			// err, so that downstream consumers will see identical messages no matter
			// which server version they're using.
			Message: terr.Error(),
		}
	}

	// We don't know exactly what the passed in error was
	return &mproto.RPCError{
		Code:    int64(vtrpc.ErrorCode_UNKNOWN_ERROR),
		Message: err.Error(),
	}
}

// AddTabletErrorToQueryResult will mutate a QueryResult struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToQueryResult(err error, reply *mproto.QueryResult) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToSessionInfo will mutate a SessionInfo struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToSessionInfo(err error, reply *proto.SessionInfo) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToTransactionInfo will mutate a TransactionInfo struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToTransactionInfo(err error, reply *proto.TransactionInfo) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToQueryResultList will mutate a QueryResultList struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToQueryResultList(err error, reply *proto.QueryResultList) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToSplitQueryResult will mutate a SplitQueryResult struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToSplitQueryResult(err error, reply *proto.SplitQueryResult) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToBeginResponse will mutate a BeginResponse struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToBeginResponse(err error, reply *proto.BeginResponse) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToCommitResponse will mutate a CommitResponse struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToCommitResponse(err error, reply *proto.CommitResponse) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}

// AddTabletErrorToRollbackResponse will mutate a RollbackResponse struct to fill in the Err
// field with details from the TabletError.
func AddTabletErrorToRollbackResponse(err error, reply *proto.RollbackResponse) {
	if err == nil {
		return
	}
	reply.Err = rpcErrFromTabletError(err)
}
