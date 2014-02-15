// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// exclusive transitions can be executed without a lock
// NOT_SERVING -> CONNECTING
// CONNECTING -> ABORT -> NOT_SERVING
// CONNECTING -> INITIALIZING -> SERVING/NOT_SERVING
// SERVING -> SHUTTING_DOWN -> NOT_SERVING
const (
	NOT_SERVING = iota
	CONNECTING
	ABORT
	INITIALIZING
	SERVING
	SHUTTING_DOWN
)

var stateName = map[int64]string{
	NOT_SERVING:   "NOT_SERVING",
	CONNECTING:    "CONNECTING",
	ABORT:         "ABORT",
	INITIALIZING:  "INITIALIZING",
	SERVING:       "SERVING",
	SHUTTING_DOWN: "SHUTTING_DOWN",
}

//-----------------------------------------------
// RPC API
type Context struct {
	RemoteAddr string
	Username   string
}

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
	state   sync2.AtomicInt64

	qe        *QueryEngine
	rci       *RowcacheInvalidator
	sessionId int64
	dbconfig  *dbconfigs.DBConfig
}

func NewSqlQuery(config Config) *SqlQuery {
	sq := &SqlQuery{}
	sq.qe = NewQueryEngine(config)
	sq.rci = NewRowcacheInvalidator(sq.qe)
	stats.PublishJSONFunc("Voltron", sq.statsJSON)
	stats.Publish("TabletState", stats.IntFunc(sq.state.Get))
	stats.Publish("TabletStateName", stats.StringFunc(sq.GetState))
	return sq
}

// GetState returns the name of the current SqlQuery state (which is
// read atomically).
func (sq *SqlQuery) GetState() string {
	return stateName[sq.state.Get()]
}

func (sq *SqlQuery) setState(state int64) {
	log.Infof("SqlQuery state: %v -> %v", stateName[sq.state.Get()], stateName[state])
	sq.state.Set(state)
}

func (sq *SqlQuery) allowQueries(dbconfig *dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules, mysqld *mysqlctl.Mysqld) {
	sq.statemu.Lock()
	defer sq.statemu.Unlock()

	v := sq.state.Get()
	switch v {
	case CONNECTING, ABORT, SERVING:
		log.Infof("Ignoring allowQueries request, current state: %v", v)
		return
	case INITIALIZING, SHUTTING_DOWN:
		panic("unreachable")
	}
	// state is NOT_SERVING
	sq.setState(CONNECTING)

	// When this function exits, state can be CONNECTING or ABORT
	func() {
		sq.statemu.Unlock()
		defer sq.statemu.Lock()

		waitTime := time.Second
		// disallowQueries can change the state to ABORT during this time.
		for sq.state.Get() != ABORT {
			params, err := dbconfigs.MysqlParams(&dbconfig.ConnectionParams)
			if err == nil {
				c, err := mysql.Connect(params)
				if err == nil {
					c.Close()
					break
				}
				log.Errorf("mysql.Connect() error: %v", err)
			} else {
				log.Errorf("dbconfigs.MysqlParams error: %v", err)
			}
			time.Sleep(waitTime)
			// Cap at 32 seconds
			if waitTime < 30*time.Second {
				waitTime = waitTime * 2
			}
		}
	}()

	if sq.state.Get() == ABORT {
		sq.setState(NOT_SERVING)
		log.Infof("allowQueries aborting")
		return
	}
	sq.setState(INITIALIZING)

	defer func() {
		if x := recover(); x != nil {
			log.Errorf("%s", x.(*TabletError).Message)
			sq.qe.Close()
			sq.rci.Close()
			sq.setState(NOT_SERVING)
			return
		}
		sq.setState(SERVING)
	}()

	sq.qe.Open(&dbconfig.ConnectionParams, schemaOverrides, qrs, dbconfig.EnableRowcache)
	if dbconfig.EnableRowcache && dbconfig.EnableInvalidator {
		sq.rci.Open(dbconfig.DbName, mysqld)
	}
	sq.dbconfig = dbconfig
	sq.sessionId = Rand()
	log.Infof("Session id: %d", sq.sessionId)
}

func (sq *SqlQuery) disallowQueries() {
	sq.statemu.Lock()
	defer sq.statemu.Unlock()

	switch sq.state.Get() {
	case CONNECTING:
		sq.setState(ABORT)
		return
	case NOT_SERVING, ABORT:
		return
	case INITIALIZING, SHUTTING_DOWN:
		panic("unreachable")
	}
	// state is SERVING
	sq.setState(SHUTTING_DOWN)
	defer func() {
		sq.setState(NOT_SERVING)
	}()

	log.Infof("Stopping query service: %d", sq.sessionId)
	sq.qe.Close()
	sq.rci.Close()
	sq.sessionId = 0
	sq.dbconfig = &dbconfigs.DBConfig{}
}

// checkState checks if we can serve queries. If not, it causes an
// error whose category is state dependent:
// SERVING: Everything is allowed.
// SHUTTING_DOWN:
//   SELECT & BEGIN: RETRY errors
//   DMLs & COMMITS: Allowed
// NOT_SERVING: RETRY for all.
func (sq *SqlQuery) checkState(sessionId int64, allowShutdown bool) {
	switch sq.state.Get() {
	case NOT_SERVING:
		panic(NewTabletError(RETRY, "not serving"))
	case CONNECTING, ABORT, INITIALIZING:
		panic(NewTabletError(RETRY, "initalizing"))
	case SHUTTING_DOWN:
		if !allowShutdown {
			panic(NewTabletError(RETRY, "unavailable"))
		}
	}
	// state is SERVING
	if sessionId == 0 || sessionId != sq.sessionId {
		panic(NewTabletError(RETRY, "Invalid session Id %v", sessionId))
	}
}

func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	if sq.state.Get() != SERVING {
		return NewTabletError(RETRY, "Query server is in %s state", stateName[sq.state.Get()])
	}
	if sessionParams.Keyspace != sq.dbconfig.Keyspace {
		return NewTabletError(FATAL, "Keyspace mismatch, expecting %v, received %v", sq.dbconfig.Keyspace, sessionParams.Keyspace)
	}
	if sessionParams.Shard != sq.dbconfig.Shard {
		return NewTabletError(FATAL, "Shard mismatch, expecting %v, received %v", sq.dbconfig.Shard, sessionParams.Shard)
	}
	sessionInfo.SessionId = sq.sessionId
	return nil
}

func (sq *SqlQuery) Begin(context *Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	logStats := newSqlQueryStats("Begin", context)
	logStats.OriginalSql = "begin"
	defer handleError(&err, logStats)
	sq.checkState(session.SessionId, false)

	txInfo.TransactionId = sq.qe.Begin(logStats)
	return nil
}

func (sq *SqlQuery) Commit(context *Context, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Commit", context)
	logStats.OriginalSql = "commit"
	defer handleError(&err, logStats)
	sq.checkState(session.SessionId, true)

	sq.qe.Commit(logStats, session.TransactionId)
	return nil
}

func (sq *SqlQuery) Rollback(context *Context, session *proto.Session) (err error) {
	logStats := newSqlQueryStats("Rollback", context)
	logStats.OriginalSql = "rollback"
	defer handleError(&err, logStats)
	sq.checkState(session.SessionId, true)

	sq.qe.Rollback(logStats, session.TransactionId)
	return nil
}

func handleInvalidationError(request interface{}) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic for %v:\n%v\n%s", request, x, tb.Stack(4))
			internalErrors.Add("Panic", 1)
			return
		}
		log.Errorf("%s: %v", terr.Message, request)
		internalErrors.Add("Invalidation", 1)
	}
}

func (sq *SqlQuery) invalidateForDml(dml *proto.DmlType) {
	if sq.state.Get() != SERVING {
		return
	}
	defer handleInvalidationError(dml)
	sq.qe.InvalidateForDml(dml)
}

func (sq *SqlQuery) invalidateForDDL(ddlInvalidate *proto.DDLInvalidate) {
	if sq.state.Get() != SERVING {
		return
	}
	defer handleInvalidationError(ddlInvalidate)
	sq.qe.InvalidateForDDL(ddlInvalidate)
}

func handleExecError(query *proto.Query, err *error, logStats *sqlQueryStats) {
	if logStats != nil {
		logStats.Send()
	}
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic for %v:\n%v\n%s", query, x, tb.Stack(4))
			*err = NewTabletError(FAIL, "%v: uncaught panic for %v", x, query)
			internalErrors.Add("Panic", 1)
			return
		}
		*err = terr
		terr.RecordStats()
		// suppress these errors in logs
		if terr.ErrorType == RETRY || terr.ErrorType == TX_POOL_FULL || terr.SqlError == mysql.DUP_ENTRY {
			return
		}
		log.Errorf("%s: %v", terr.Message, query)
	}
}

func (sq *SqlQuery) Execute(context *Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
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
func (sq *SqlQuery) StreamExecute(context *Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) (err error) {
	logStats := newSqlQueryStats("StreamExecute", context)
	defer handleExecError(query, &err, logStats)

	// check cases we don't handle yet
	if query.TransactionId != 0 {
		return NewTabletError(FAIL, "Transactions not supported with streaming")
	}

	sq.checkState(query.SessionId, false)
	sq.qe.StreamExecute(logStats, query, sendReply)
	return nil
}

func (sq *SqlQuery) ExecuteBatch(context *Context, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	defer handleError(&err, nil)
	if len(queryList.Queries) == 0 {
		panic(NewTabletError(FAIL, "Empty query list"))
	}
	sq.checkState(queryList.SessionId, false)
	begin_called := false
	session := proto.Session{
		TransactionId: queryList.TransactionId,
		SessionId:     queryList.SessionId,
	}
	reply.List = make([]mproto.QueryResult, 0, len(queryList.Queries))
	for _, bound := range queryList.Queries {
		trimmed := strings.ToLower(strings.Trim(bound.Sql, " \t\r\n"))
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
			if err = sq.Commit(context, &session); err != nil {
				return err
			}
			session.TransactionId = 0
			begin_called = false
			reply.List = append(reply.List, mproto.QueryResult{})
		default:
			query := proto.Query{
				Sql:           bound.Sql,
				BindVariables: bound.BindVariables,
				TransactionId: session.TransactionId,
				SessionId:     session.SessionId,
			}
			var localReply mproto.QueryResult
			if err = sq.Execute(context, &query, &localReply); err != nil {
				if begin_called {
					sq.Rollback(context, &session)
				}
				return err
			}
			reply.List = append(reply.List, localReply)
		}
	}
	if begin_called {
		sq.Rollback(context, &session)
		panic(NewTabletError(FAIL, "begin called with no commit"))
	}
	return nil
}

func (sq *SqlQuery) statsJSON() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\n \"State\": \"%v\",", stateName[sq.state.Get()])
	fmt.Fprintf(buf, "\n \"CachePool\": %v,", sq.qe.cachePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"QueryCache\": %v,", sq.qe.schemaInfo.queries.StatsJSON())
	fmt.Fprintf(buf, "\n \"SchemaReloadTime\": %v,", int64(sq.qe.schemaInfo.reloadTime))
	fmt.Fprintf(buf, "\n \"ConnPool\": %v,", sq.qe.connPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"StreamConnPool\": %v,", sq.qe.streamConnPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"TxPool\": %v,", sq.qe.txPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActiveTxPool\": %v,", sq.qe.activeTxPool.StatsJSON())
	fmt.Fprintf(buf, "\n \"ActivePool\": %v,", sq.qe.activePool.StatsJSON())
	fmt.Fprintf(buf, "\n \"MaxResultSize\": %v,", sq.qe.maxResultSize.Get())
	fmt.Fprintf(buf, "\n \"StreamBufferSize\": %v", sq.qe.streamBufferSize.Get())
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Rand() int64 {
	return rand.Int63()
}
