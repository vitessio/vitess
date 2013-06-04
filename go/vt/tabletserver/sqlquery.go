// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"expvar"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/mysql"
	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	rpcproto "code.google.com/p/vitess/go/rpcwrap/proto"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/tb"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

// exclusive transitions can be executed without a lock
// NOT_SERVING/CLOSED -> CONNECTING
// CONNECTING -> ABORT
// ABORT -> CLOSED (exclusive)
// CONNECTING -> INITIALIZING -> OPEN/NOT_SERVING
// OPEN -> SHUTTING_DOWN -> CLOSED
const (
	NOT_SERVING = iota
	CLOSED
	CONNECTING
	ABORT
	INITIALIZING
	OPEN
	SHUTTING_DOWN
)

var stateName = map[int32]string{
	NOT_SERVING:   "NOT_SERVING",
	CLOSED:        "CLOSED",
	CONNECTING:    "CONNECTING",
	ABORT:         "ABORT",
	INITIALIZING:  "INITIALIZING",
	OPEN:          "OPEN",
	SHUTTING_DOWN: "SHUTTING_DOWN",
}

//-----------------------------------------------
// RPC API
type SqlQuery struct {
	// We use a hybrid locking scheme to control state transitions. This is
	// optimal for frequent reads and infrequent state changes.
	// You can use atomic lockless reads if you don't care about any state
	// changes after you've read the variable. This is the common use case.
	// You can use atomic lockless writes if you don't care about, or already
	// know, the previous value of the object. This is true for exclusive
	// transitions as documented above.
	// You should use the statemu lock if you want to execute a transition
	// where you don't want the state to change from the time you've read it.
	statemu sync.Mutex
	state   sync2.AtomicInt32
	states  *stats.States

	qe        *QueryEngine
	sessionId int64
	dbconfig  dbconfigs.DBConfig
}

func NewSqlQuery(config Config) *SqlQuery {
	sq := &SqlQuery{}
	sq.qe = NewQueryEngine(config)
	sq.states = stats.NewStates("", []string{
		stateName[NOT_SERVING],
		stateName[CLOSED],
		stateName[CONNECTING],
		stateName[ABORT],
		stateName[INITIALIZING],
		stateName[OPEN],
		stateName[SHUTTING_DOWN],
	}, time.Now(), NOT_SERVING)
	expvar.Publish("Voltron", stats.StrFunc(func() string { return sq.statsJSON() }))
	return sq
}

func (sq *SqlQuery) setState(state int32) {
	relog.Info("SqlQuery state: %v -> %v", stateName[sq.state.Get()], stateName[state])
	sq.state.Set(state)
	sq.states.SetState(int(state))
}

func (sq *SqlQuery) allowQueries(dbconfig dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules) {
	sq.statemu.Lock()
	v := sq.state.Get()
	switch v {
	case CONNECTING, ABORT, OPEN:
		sq.statemu.Unlock()
		relog.Info("Ignoring allowQueries request, current state: %v", v)
		return
	case INITIALIZING, SHUTTING_DOWN:
		panic("unreachable")
	}
	// state is NOT_SERVING or CLOSED
	sq.setState(CONNECTING)
	sq.statemu.Unlock()

	// Try connecting. disallowQueries can change the state to ABORT during this time.
	waitTime := time.Second
	for {
		c, err := mysql.Connect(dbconfig.MysqlParams())
		if err == nil {
			c.Close()
			break
		}
		relog.Error("%v", err)
		time.Sleep(waitTime)
		// Cap at 32 seconds
		if waitTime < 30*time.Second {
			waitTime = waitTime * 2
		}
		if sq.state.Get() == ABORT {
			// Exclusive transition. No need to lock statemu.
			sq.setState(CLOSED)
			relog.Info("allowQueries aborting")
			return
		}
	}

	// Connection successful. Keep statemu locked.
	sq.statemu.Lock()
	defer sq.statemu.Unlock()
	if sq.state.Get() == ABORT {
		sq.setState(CLOSED)
		relog.Info("allowQueries aborting")
		return
	}
	sq.setState(INITIALIZING)

	defer func() {
		if x := recover(); x != nil {
			relog.Error("%s", x.(*TabletError).Message)
			sq.setState(NOT_SERVING)
			return
		}
		sq.setState(OPEN)
	}()

	sq.qe.Open(dbconfig, schemaOverrides, qrs)
	sq.dbconfig = dbconfig
	sq.sessionId = Rand()
	relog.Info("Session id: %d", sq.sessionId)
}

func (sq *SqlQuery) disallowQueries(forRestart bool) {
	sq.statemu.Lock()
	defer sq.statemu.Unlock()
	switch sq.state.Get() {
	case CONNECTING:
		sq.setState(ABORT)
		return
	case NOT_SERVING, CLOSED:
		if forRestart {
			sq.setState(CLOSED)
		} else {
			sq.setState(NOT_SERVING)
		}
		return
	case ABORT:
		return
	case INITIALIZING, SHUTTING_DOWN:
		panic("unreachable")
	}
	// state is OPEN
	sq.setState(SHUTTING_DOWN)
	defer func() {
		if forRestart {
			sq.setState(CLOSED)
		} else {
			sq.setState(NOT_SERVING)
		}
	}()

	relog.Info("Stopping query service: %d", sq.sessionId)
	sq.qe.Close(forRestart)
	sq.sessionId = 0
	sq.dbconfig = dbconfigs.DBConfig{}
}

// checkState checks if we can serve queries. If not, it causes an
// error whose category is state dependent:
// OPEN: Everything is allowed.
// SHUTTING_DOWN:
//   SELECT & BEGIN: RETRY errors
//   DMLs & COMMITS: Allowed
// CLOSED: RETRY for all.
// NOT_SERVING: FATAL for all.
func (sq *SqlQuery) checkState(sessionId int64, allowShutdown bool) {
	switch sq.state.Get() {
	case NOT_SERVING:
		panic(NewTabletError(FATAL, "not serving"))
	case CLOSED:
		panic(NewTabletError(RETRY, "unavailable"))
	case CONNECTING, ABORT, INITIALIZING:
		panic(NewTabletError(FATAL, "initalizing"))
	case SHUTTING_DOWN:
		if !allowShutdown {
			panic(NewTabletError(RETRY, "unavailable"))
		}
	}
	// state is OPEN
	if sessionId == 0 || sessionId != sq.sessionId {
		panic(NewTabletError(RETRY, "Invalid session Id %v", sessionId))
	}
}

func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	if sessionParams.DbName == "" {
		if sessionParams.Keyspace != sq.dbconfig.Keyspace {
			return NewTabletError(FATAL, "Keyspace mismatch, expecting %v, received %v", sq.dbconfig.Keyspace, sessionParams.Keyspace)
		}
		if sessionParams.Shard != sq.dbconfig.Shard {
			return NewTabletError(FATAL, "Shard mismatch, expecting %v, received %v", sq.dbconfig.Shard, sessionParams.Shard)
		}
	} else {
		if sessionParams.DbName != sq.dbconfig.Dbname {
			return NewTabletError(FATAL, "db name mismatch, expecting %v, received %v", sq.dbconfig.Dbname, sessionParams.DbName)
		}
		if sessionParams.KeyRange != sq.dbconfig.KeyRange {
			return NewTabletError(FATAL, "KeyRange mismatch, expecting %v, received %v", sq.dbconfig.KeyRange.String(), sessionParams.KeyRange.String())
		}
	}
	sessionInfo.SessionId = sq.sessionId
	return nil
}

func (sq *SqlQuery) Begin(context *rpcproto.Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	logStats := newSqlQueryStats("Begin", context)
	logStats.OriginalSql = "begin"
	defer handleError(&err, logStats)
	sq.checkState(session.SessionId, false)

	txInfo.TransactionId = sq.qe.Begin(logStats, session.ConnectionId)
	return nil
}

func (sq *SqlQuery) Commit(context *rpcproto.Context, session *proto.Session, noOutput *string) (err error) {
	logStats := newSqlQueryStats("Commit", context)
	logStats.OriginalSql = "commit"
	defer handleError(&err, logStats)
	sq.checkState(session.SessionId, true)

	sq.qe.Commit(logStats, session.TransactionId)
	return nil
}

func (sq *SqlQuery) Rollback(context *rpcproto.Context, session *proto.Session, noOutput *string) (err error) {
	logStats := newSqlQueryStats("Rollback", context)
	logStats.OriginalSql = "rollback"
	defer handleError(&err, logStats)
	sq.checkState(session.SessionId, true)

	sq.qe.Rollback(logStats, session.TransactionId)
	return nil
}

func (sq *SqlQuery) CreateReserved(session *proto.Session, connectionInfo *proto.ConnectionInfo) (err error) {
	defer handleError(&err, nil)
	sq.checkState(session.SessionId, false)
	connectionInfo.ConnectionId = sq.qe.CreateReserved()
	return nil
}

func (sq *SqlQuery) CloseReserved(session *proto.Session, noOutput *string) (err error) {
	defer handleError(&err, nil)
	sq.checkState(session.SessionId, false)
	sq.qe.CloseReserved(session.ConnectionId)
	return nil
}

func handleExecError(query *proto.Query, err *error, logStats *sqlQueryStats) {
	if logStats != nil {
		logStats.Send()
	}
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			relog.Error("Uncaught panic for %v:\n%v\n%s", query, x, tb.Stack(4))
			*err = NewTabletError(FAIL, "%v: uncaught panic for %v", x, query)
			errorStats.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats()
		if terr.ErrorType == RETRY || terr.SqlError == DUPLICATE_KEY { // suppress these errors in logs
			return
		}
		relog.Error("%s: %v", terr.Message, query)
	}
}

func (sq *SqlQuery) Execute(context *rpcproto.Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
	logStats := newSqlQueryStats("Execute", context)
	defer handleExecError(query, &err, logStats)

	// allow shutdown state if we're in a transaction
	allowShutdown := (query.TransactionId != 0)
	sq.checkState(query.SessionId, allowShutdown)

	*reply = *sq.qe.Execute(logStats, query)
	return nil
}

// the first QueryResult will have Fields set (and Rows nil)
// the subsequent QueryResult will have Rows set (and Fields nil)
func (sq *SqlQuery) StreamExecute(context *rpcproto.Context, query *proto.Query, sendReply func(reply interface{}) error) (err error) {
	logStats := newSqlQueryStats("StreamExecute", context)
	defer handleExecError(query, &err, logStats)

	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(FAIL, "Transactions not supported with streaming")
	}
	if query.ConnectionId != 0 {
		return NewTabletError(FAIL, "Persistent connections not supported with streaming")
	}

	// allow shutdown state if we're in a transaction
	allowShutdown := (query.TransactionId != 0)
	sq.checkState(query.SessionId, allowShutdown)

	sq.qe.StreamExecute(logStats, query, sendReply)
	return nil
}

func (sq *SqlQuery) ExecuteBatch(context *rpcproto.Context, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	defer handleError(&err, nil)
	ql := queryList.List
	if len(ql) == 0 {
		panic(NewTabletError(FAIL, "Empty query list"))
	}
	sq.checkState(ql[0].SessionId, false)
	begin_called := false
	var noOutput string
	session := proto.Session{
		TransactionId: ql[0].TransactionId,
		ConnectionId:  ql[0].ConnectionId,
		SessionId:     ql[0].SessionId,
	}
	reply.List = make([]mproto.QueryResult, 0, len(ql))
	for _, query := range ql {
		trimmed := strings.ToLower(strings.Trim(query.Sql, " \t\r\n"))
		switch trimmed {
		case "begin":
			if session.TransactionId != 0 {
				panic(NewTabletError(FAIL, "Nested transactions disallowed"))
			}
			var txInfo proto.TransactionInfo
			if err = sq.Begin(context, &session, &txInfo); err != nil {
				return err
			}
			session.TransactionId = txInfo.TransactionId
			begin_called = true
			reply.List = append(reply.List, mproto.QueryResult{})
		case "commit":
			if !begin_called {
				panic(NewTabletError(FAIL, "Cannot commit without begin"))
			}
			if err = sq.Commit(context, &session, &noOutput); err != nil {
				return err
			}
			session.TransactionId = 0
			begin_called = false
			reply.List = append(reply.List, mproto.QueryResult{})
		default:
			query.TransactionId = session.TransactionId
			query.ConnectionId = session.ConnectionId
			query.SessionId = session.SessionId
			var localReply mproto.QueryResult
			if err = sq.Execute(context, &query, &localReply); err != nil {
				if begin_called {
					sq.Rollback(context, &session, &noOutput)
				}
				return err
			}
			reply.List = append(reply.List, localReply)
		}
	}
	if begin_called {
		sq.Rollback(context, &session, &noOutput)
		panic(NewTabletError(FAIL, "begin called with no commit"))
	}
	return nil
}

func (sq *SqlQuery) statsJSON() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	// TODO(alainjobart) when no monitoring depends on 'State',
	// remove it (use 'States.Current' instead)
	fmt.Fprintf(buf, "\n \"State\": \"%v\",", stateName[sq.state.Get()])
	fmt.Fprintf(buf, "\n \"States\": %v,", sq.states.String())
	fmt.Fprintf(buf, "\n \"CachePool\": %v,", sq.qe.cachePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"QueryCache\": %v,", sq.qe.schemaInfo.queries.StatsJSON())
	fmt.Fprintf(buf, "\n \"SchemaReloadTime\": %v,", int64(sq.qe.schemaInfo.reloadTime))
	fmt.Fprintf(buf, "\n \"ConnPool\": %v,", sq.qe.connPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"StreamConnPool\": %v,", sq.qe.streamConnPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"TxPool\": %v,", sq.qe.txPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActiveTxPool\": %v,", sq.qe.activeTxPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActivePool\": %v,", sq.qe.activePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"MaxResultSize\": %v,", sq.qe.maxResultSize.Get())
	fmt.Fprintf(buf, "\n \"StreamBufferSize\": %v,", sq.qe.streamBufferSize.Get())
	fmt.Fprintf(buf, "\n \"ReservedPool\": %v", sq.qe.reservedPool.StatsJSON())
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

func Rand() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63()
}
