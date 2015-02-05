// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconnpool"
)

// dbconn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type dbconn struct {
	conn *dbconnpool.DBConnection
	info *mysql.ConnectionParams
	pool *ConnPool

	current sync2.AtomicString
	// killed is used to synchronize between Kill
	// and exec functions.
	killed chan bool
}

func newdbconn(cp *ConnPool, appParams, dbaParams *mysql.ConnectionParams) (*dbconn, error) {
	c, err := dbconnpool.NewDBConnection(appParams, mysqlStats)
	if err != nil {
		go CheckMySQL()
		return nil, err
	}
	return &dbconn{
		conn:   c,
		info:   appParams,
		pool:   cp,
		killed: make(chan bool, 1),
	}, nil
}

func (dbc *dbconn) Exec(query string, maxrows int, wantfields bool, deadline Deadline) (*proto.QueryResult, error) {
	for attempt := 1; attempt <= 2; attempt++ {
		r, err := dbc.execOnce(query, maxrows, wantfields, deadline)
		switch {
		case err == nil:
			return r, nil
		case !IsConnErr(err):
			return nil, NewTabletErrorSql(ErrFail, err)
		case attempt == 2:
			return nil, NewTabletErrorSql(ErrFatal, err)
		}
		err2 := dbc.reconnect()
		if err2 != nil {
			go CheckMySQL()
			return nil, NewTabletErrorSql(ErrFatal, err)
		}
	}
	panic("unreachable")
}

func (dbc *dbconn) execOnce(query string, maxrows int, wantfields bool, deadline Deadline) (*proto.QueryResult, error) {
	dbc.startRequest(query)
	defer dbc.endRequest()

	qd, err := SetDeadline(dbc, deadline)
	if err != nil {
		return nil, err
	}
	if qd != nil {
		defer qd.Done()
	}
	// Uncomment this line for manual testing.
	// defer time.Sleep(20 * time.Second)
	return dbc.conn.ExecuteFetch(query, maxrows, wantfields)
}

func (dbc *dbconn) Stream(query string, callback func(*proto.QueryResult) error, streamBufferSize int) error {
	dbc.startRequest(query)
	defer dbc.endRequest()
	return dbc.conn.ExecuteStreamFetch(query, callback, streamBufferSize)
}

func (dbc *dbconn) Current() string {
	return dbc.current.Get()
}

func (dbc *dbconn) Close() {
	dbc.conn.Close()
}

func (dbc *dbconn) Recycle() {
	if dbc.conn.IsClosed() {
		dbc.pool.Put(nil)
	} else {
		dbc.pool.Put(dbc)
	}
}

func (dbc *dbconn) Kill() {
	select {
	case killed := <-dbc.killed:
		defer func() { dbc.killed <- true }()
		// A previous kill killed this query already.
		if killed {
			return
		}
	default:
		// Nothing is executing
		return
	}
	killStats.Add("Queries", 1)
	log.Infof("killing query %s", dbc.Current())
	dbc.conn.Shutdown()
	killConn, err := dbc.pool.dbaPool.Get(0)
	if err != nil {
		log.Warningf("Failed to get conn from dba pool: %v", err)
		return
	}
	defer killConn.Recycle()
	sql := fmt.Sprintf("kill %d", dbc.conn.ID())
	_, err = killConn.ExecuteFetch(sql, 10000, false)
	if err != nil {
		log.Errorf("Could not kill query %s: %v", dbc.Current(), err)
	}
}

func (dbc *dbconn) startRequest(query string) {
	dbc.current.Set(query)
	dbc.killed <- false
}

func (dbc *dbconn) endRequest() {
	killed := <-dbc.killed
	defer dbc.current.Set("")
	if killed {
		dbc.Close()
	}
}

func (dbc *dbconn) reconnect() error {
	dbc.conn.Close()
	newConn, err := dbconnpool.NewDBConnection(dbc.info, mysqlStats)
	if err != nil {
		return err
	}
	dbc.conn = newConn
	return nil
}

// SetDeadline starts a timer for the specified connection and returns
// a QueryDeadline. If Done is not called before the timer runs out, the
// timer kills the connection.
func SetDeadline(conn PoolConn, deadline Deadline) (QueryDeadliner, error) {
	timeout, err := deadline.Timeout()
	if err != nil {
		return nil, fmt.Errorf("SetDeadline: %v", err)
	}
	if timeout == 0 {
		return nil, nil
	}
	qd := make(QueryDeadliner)
	tmr := time.NewTimer(timeout)
	go func() {
		defer tmr.Stop()
		select {
		case <-tmr.C:
			conn.Kill()
		case <-qd:
			return
		}

		// Verify the query got killed.
		tmr2 := time.NewTimer(15 * time.Second)
		defer tmr2.Stop()
		select {
		case <-tmr2.C:
			internalErrors.Add("HungQuery", 1)
			log.Warningf("Query may be hung: %s", conn.Current())
		case <-qd:
			return
		}
		<-qd
		log.Warningf("Hung query returned")
	}()
	return qd, nil
}
