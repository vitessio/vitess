// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	"github.com/youtube/vitess/go/hack"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"golang.org/x/net/context"
)

// PoolConn is the interface implemented by users of this specialized pool.
type PoolConn interface {
	Exec(query string, maxrows int, wantfields bool, deadline Deadline) (*mproto.QueryResult, error)
}

// RequestContext encapsulates a context and associated variables for a request
type RequestContext struct {
	ctx      context.Context
	logStats *SQLQueryStats
	qe       *QueryEngine
	deadline Deadline
}

func (rqc *RequestContext) getConn(pool *ConnPool) *DBConn {
	start := time.Now()
	timeout, err := rqc.deadline.Timeout()
	if err != nil {
		panic(NewTabletError(ErrFail, "getConn: %v", err))
	}
	conn, err := pool.Get(timeout)
	switch err {
	case nil:
		rqc.logStats.WaitingForConnection += time.Now().Sub(start)
		return conn
	case ErrConnPoolClosed:
		panic(connPoolClosedErr)
	}
	panic(NewTabletErrorSql(ErrFatal, err))
}

func (rqc *RequestContext) qFetch(logStats *SQLQueryStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}) (result *mproto.QueryResult) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, nil)
	q, ok := rqc.qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
		// Wrap default error to TabletError
		if q.Err != nil {
			q.Err = NewTabletError(ErrFail, q.Err.Error())
		}
		waitingForConnectionStart := time.Now()
		timeout, err := rqc.deadline.Timeout()
		if err != nil {
			q.Err = NewTabletError(ErrFail, "qFetch: %v", err)
		}
		conn, err := rqc.qe.connPool.Get(timeout)
		logStats.WaitingForConnection += time.Now().Sub(waitingForConnectionStart)
		if err != nil {
			q.Err = NewTabletErrorSql(ErrFatal, err)
		} else {
			defer conn.Recycle()
			q.Result, q.Err = rqc.execSQLNoPanic(conn, sql, false)
		}
	} else {
		logStats.QuerySources |= QUERY_SOURCE_CONSOLIDATOR
		startTime := time.Now()
		q.Wait()
		waitStats.Record("Consolidations", startTime)
	}
	if q.Err != nil {
		panic(q.Err)
	}
	return q.Result.(*mproto.QueryResult)
}

func (rqc *RequestContext) directFetch(conn PoolConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, buildStreamComment)
	return rqc.execSQL(conn, sql, false)
}

// fullFetch also fetches field info
func (rqc *RequestContext) fullFetch(conn PoolConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, buildStreamComment)
	return rqc.execSQL(conn, sql, true)
}

func (rqc *RequestContext) fullStreamFetch(conn *DBConn, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, callback func(*mproto.QueryResult) error) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, buildStreamComment)
	rqc.execStreamSQL(conn, sql, callback)
}

func (rqc *RequestContext) generateFinalSql(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) string {
	bindVars["#maxLimit"] = rqc.qe.maxResultSize.Get() + 1
	sql, err := parsedQuery.GenerateQuery(bindVars)
	if err != nil {
		panic(NewTabletError(ErrFail, "%s", err))
	}
	if buildStreamComment != nil {
		sql = append(sql, buildStreamComment...)
	}
	// undo hack done by stripTrailing
	sql = restoreTrailing(sql, bindVars)
	return hack.String(sql)
}

func (rqc *RequestContext) execSQL(conn PoolConn, sql string, wantfields bool) *mproto.QueryResult {
	result, err := rqc.execSQLNoPanic(conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (rqc *RequestContext) execSQLNoPanic(conn PoolConn, sql string, wantfields bool) (*mproto.QueryResult, error) {
	defer rqc.logStats.AddRewrittenSql(sql, time.Now())
	return conn.Exec(sql, int(rqc.qe.maxResultSize.Get()), wantfields, rqc.deadline)
}

func (rqc *RequestContext) execStreamSQL(conn *DBConn, sql string, callback func(*mproto.QueryResult) error) {
	start := time.Now()
	err := conn.Stream(sql, callback, int(rqc.qe.streamBufferSize.Get()))
	rqc.logStats.AddRewrittenSql(sql, start)
	if err != nil {
		panic(NewTabletErrorSql(ErrFail, err))
	}
}
