// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconnpool"
)

// DBConn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type DBConn struct {
	conn *dbconnpool.DBConnection
	info *mysql.ConnectionParams
	pool *ConnPool

	current sync2.AtomicString
	// killed is used to synchronize between Kill
	// and exec functions.
	killed chan bool
}

// NewDBConn creates a new DBConn. It triggers a CheckMySQL if creation fails.
func NewDBConn(cp *ConnPool, appParams, dbaParams *mysql.ConnectionParams) (*DBConn, error) {
	c, err := dbconnpool.NewDBConnection(appParams, mysqlStats)
	if err != nil {
		go checkMySQL()
		return nil, err
	}
	return &DBConn{
		conn:   c,
		info:   appParams,
		pool:   cp,
		killed: make(chan bool, 1),
	}, nil
}

// Exec executes the specified query. If there is a connection error, it will reconnect
// and retry. A failed reconnect will trigger a CheckMySQL.
func (dbc *DBConn) Exec(query string, maxrows int, wantfields bool, deadline Deadline) (*mproto.QueryResult, error) {
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
			go checkMySQL()
			return nil, NewTabletErrorSql(ErrFatal, err)
		}
	}
	panic("unreachable")
}

func (dbc *DBConn) execOnce(query string, maxrows int, wantfields bool, deadline Deadline) (*mproto.QueryResult, error) {
	dbc.startRequest(query)
	defer dbc.endRequest()

	done, err := dbc.setDeadline(deadline)
	if err != nil {
		return nil, err
	}
	if done != nil {
		defer close(done)
	}
	// Uncomment this line for manual testing.
	// defer time.Sleep(20 * time.Second)
	return dbc.conn.ExecuteFetch(query, maxrows, wantfields)
}

// Stream executes the query and streams the results.
func (dbc *DBConn) Stream(query string, callback func(*mproto.QueryResult) error, streamBufferSize int) error {
	dbc.startRequest(query)
	defer dbc.endRequest()
	return dbc.conn.ExecuteStreamFetch(query, callback, streamBufferSize)
}

// VerifyStrict returns true if MySQL is in STRICT mode.
func (dbc *DBConn) VerifyStrict() bool {
	return dbc.conn.VerifyStrict()
}

// Close closes the DBConn.
func (dbc *DBConn) Close() {
	dbc.conn.Close()
}

// IsClosed returns true if DBConn is closed.
func (dbc *DBConn) IsClosed() bool {
	return dbc.conn.IsClosed()
}

// Recycle returns the DBConn to the pool.
func (dbc *DBConn) Recycle() {
	if dbc.conn.IsClosed() {
		dbc.pool.Put(nil)
	} else {
		dbc.pool.Put(dbc)
	}
}

// Kill kills the currently executing query both on MySQL side
// and on the connection side. If no query is executing, it's a no-op.
// Kill will also not kill a query more than once.
func (dbc *DBConn) Kill() {
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

// Current returns the currently executing query.
func (dbc *DBConn) Current() string {
	return dbc.current.Get()
}

// ID returns the connection id.
func (dbc *DBConn) ID() int64 {
	return dbc.conn.ID()
}

func (dbc *DBConn) startRequest(query string) {
	dbc.current.Set(query)
	dbc.killed <- false
}

func (dbc *DBConn) endRequest() {
	killed := <-dbc.killed
	defer dbc.current.Set("")
	if killed {
		dbc.Close()
	}
}

func (dbc *DBConn) reconnect() error {
	dbc.conn.Close()
	newConn, err := dbconnpool.NewDBConnection(dbc.info, mysqlStats)
	if err != nil {
		return err
	}
	dbc.conn = newConn
	return nil
}

func (dbc *DBConn) setDeadline(deadline Deadline) (done chan bool, err error) {
	timeout, err := deadline.Timeout()
	if err != nil {
		return nil, fmt.Errorf("setDeadline: %v", err)
	}
	if timeout == 0 {
		return nil, nil
	}
	done = make(chan bool)
	tmr := time.NewTimer(timeout)
	go func() {
		defer tmr.Stop()
		select {
		case <-tmr.C:
			dbc.Kill()
		case <-done:
			return
		}

		// Verify the query got killed.
		tmr2 := time.NewTimer(15 * time.Second)
		defer tmr2.Stop()
		select {
		case <-tmr2.C:
			internalErrors.Add("HungQuery", 1)
			log.Warningf("Query may be hung: %s", dbc.Current())
		case <-done:
			return
		}
		<-done
		log.Warningf("Hung query returned")
	}()
	return done, nil
}
