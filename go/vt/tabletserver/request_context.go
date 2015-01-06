// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	"github.com/youtube/vitess/go/hack"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"golang.org/x/net/context"
)

// RequestContext encapsulates a context and associated variables for a request
type RequestContext struct {
	ctx      context.Context
	logStats *SQLQueryStats
	qe       *QueryEngine
	deadline Deadline
}

func (rqc *RequestContext) getConn(pool *dbconnpool.ConnectionPool) dbconnpool.PoolConnection {
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
	case dbconnpool.ErrConnPoolClosed:
		panic(connPoolClosedErr)
	}
	panic(NewTabletErrorSql(ErrFatal, err))
}

func (rqc *RequestContext) qFetch(logStats *SQLQueryStats, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}) (result *mproto.QueryResult) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, nil)
	q, ok := rqc.qe.consolidator.Create(string(sql))
	if ok {
		defer q.Broadcast()
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
		q.Wait()
	}
	if q.Err != nil {
		panic(q.Err)
	}
	return q.Result
}

func (rqc *RequestContext) directFetch(conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, buildStreamComment)
	return rqc.execSQL(conn, sql, false)
}

// fullFetch also fetches field info
func (rqc *RequestContext) fullFetch(conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte) (result *mproto.QueryResult) {
	sql := rqc.generateFinalSql(parsedQuery, bindVars, buildStreamComment)
	return rqc.execSQL(conn, sql, true)
}

func (rqc *RequestContext) fullStreamFetch(conn dbconnpool.PoolConnection, parsedQuery *sqlparser.ParsedQuery, bindVars map[string]interface{}, buildStreamComment []byte, callback func(*mproto.QueryResult) error) {
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

func (rqc *RequestContext) execSQL(conn dbconnpool.PoolConnection, sql string, wantfields bool) *mproto.QueryResult {
	result, err := rqc.execSQLNoPanic(conn, sql, true)
	if err != nil {
		panic(err)
	}
	return result
}

func (rqc *RequestContext) execSQLNoPanic(conn dbconnpool.PoolConnection, sql string, wantfields bool) (*mproto.QueryResult, error) {
	if qd := rqc.qe.connKiller.SetDeadline(conn.ID(), rqc.deadline); qd != nil {
		defer qd.Done()
	}

	start := time.Now()
	result, err := conn.ExecuteFetch(sql, int(rqc.qe.maxResultSize.Get()), wantfields)
	rqc.logStats.AddRewrittenSql(sql, start)
	if err != nil {
		return nil, NewTabletErrorSql(ErrFail, err)
	}
	return result, nil
}

func (rqc *RequestContext) execStreamSQL(conn dbconnpool.PoolConnection, sql string, callback func(*mproto.QueryResult) error) {
	start := time.Now()
	err := conn.ExecuteStreamFetch(sql, callback, int(rqc.qe.streamBufferSize.Get()))
	rqc.logStats.AddRewrittenSql(sql, start)
	if err != nil {
		panic(NewTabletErrorSql(ErrFail, err))
	}
}
