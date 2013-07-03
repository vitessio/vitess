// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"code.google.com/p/vitess/go/sync2"
	"launchpad.net/gozk/zookeeper"
)

// Every blocking call into CGO causes another thread which blows up
// the the virtual memory.  It seems better to solve this here at the
// root of the problem rather than forcing all other apps to take into
// account limiting the number of concurrent operations on a zk
// connection.  Since this applies to any zookeeper connection, this
// is global.
var sem sync2.Semaphore

func init() {
	// The zookeeper C module logs quite a bit of useful information,
	// but much of it does not come back in the error API. To aid
	// debugging, enable the log to stderr for warnings.
	//zookeeper.SetLogLevel(zookeeper.LOG_WARN)

	maxConcurrency := 64
	x := os.Getenv("ZK_CLIENT_MAX_CONCURRENCY")
	if x != "" {
		var err error
		maxConcurrency, err = strconv.Atoi(x)
		if err != nil {
			log.Printf("invalid ZK_CLIENT_MAX_CONCURRENCY: %v", err)
		}
	}

	sem = sync2.NewSemaphore(maxConcurrency)
}

// ZkConn is a client class that implements zk.Conn using a zookeeper.Conn
type ZkConn struct {
	conn *zookeeper.Conn
}

// Dial a ZK server and waits for connection event. Returns a ZkConn
// encapsulating the zookeeper.Conn, and the zookeeper session event
// channel to monitor the connection
//
// The value for baseTimeout is used as a session timeout as well, and
// will be used to negotiate a 'good' value with the server. From
// reading the zookeeper source code, it has to be between 6 and 60
// seconds (2x and 20x the tickTime by default, with default tick time
// being 3 seconds).  min session time, max session time and ticktime
// can all be overwritten on the zookeeper server side, so these
// numbers may vary.
//
// Then this baseTimeout is used to compute other related timeouts:
// - connect timeout is 1/3 of baseTimeout
// - recv timeout is 2/3 of baseTimeout minus a ping time
// - send timeout is 1/3 of baseTimeout
// - we try to send a ping a least every baseTimeout / 3
//
// Note the baseTimeout has *nothing* to do with the time between we
// call Dial and the maximum time before we receive the event on the
// session. The library will actually try to re-connect in the background
// (after each timeout), and may *never* send an event if the TCP connections
// always fail. Use DialZkTimeout to enforce a timeout for the initial connect.
func DialZk(zkAddr string, baseTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	sem.Acquire()
	defer sem.Release()
	zconn, session, err := zookeeper.Dial(zkAddr, baseTimeout)
	if err == nil {
		// Wait for connection, possibly forever
		event := <-session
		if event.State != zookeeper.STATE_CONNECTED {
			err = fmt.Errorf("zk connect failed: %v", event.State)
		}
		if err == nil {
			return &ZkConn{zconn}, session, nil
		} else {
			zconn.Close()
		}
	}
	return nil, nil, err
}

func DialZkTimeout(zkAddr string, baseTimeout time.Duration, connectTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	sem.Acquire()
	defer sem.Release()
	zconn, session, err := zookeeper.Dial(zkAddr, baseTimeout)
	if err == nil {
		// Wait for connection, with a timeout
		timer := time.NewTimer(connectTimeout)
		select {
		case <-timer.C:
			err = fmt.Errorf("zk connect timed out")
		case event := <-session:
			if event.State != zookeeper.STATE_CONNECTED {
				err = fmt.Errorf("zk connect failed: %v", event.State)
			}
		}

		if err == nil {
			return &ZkConn{zconn}, session, nil
		} else {
			zconn.Close()
		}
	}
	return nil, nil, err
}

func (conn *ZkConn) Get(path string) (data string, stat Stat, err error) {
	sem.Acquire()
	defer sem.Release()
	data, s, err := conn.conn.Get(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error) {
	sem.Acquire()
	defer sem.Release()
	data, s, watch, err := conn.conn.GetW(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) Children(path string) (children []string, stat Stat, err error) {
	sem.Acquire()
	defer sem.Release()
	children, s, err := conn.conn.Children(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) ChildrenW(path string) (children []string, stat Stat, watch <-chan zookeeper.Event, err error) {
	sem.Acquire()
	defer sem.Release()
	children, s, watch, err := conn.conn.ChildrenW(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) Exists(path string) (stat Stat, err error) {
	sem.Acquire()
	defer sem.Release()
	s, err := conn.conn.Exists(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		return nil, err
	}
	return s, err
}

func (conn *ZkConn) ExistsW(path string) (stat Stat, watch <-chan zookeeper.Event, err error) {
	sem.Acquire()
	defer sem.Release()
	s, w, err := conn.conn.ExistsW(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		return nil, w, err
	}
	return s, w, err
}

func (conn *ZkConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	sem.Acquire()
	defer sem.Release()
	return conn.conn.Create(path, value, flags, aclv)
}

func (conn *ZkConn) Set(path, value string, version int) (stat Stat, err error) {
	sem.Acquire()
	defer sem.Release()
	s, err := conn.conn.Set(path, value, version)
	if s == nil {
		// Handle nil-nil interface conversion.
		return nil, err
	}
	return s, err
}

func (conn *ZkConn) Delete(path string, version int) (err error) {
	sem.Acquire()
	defer sem.Release()
	return conn.conn.Delete(path, version)
}

func (conn *ZkConn) Close() error {
	sem.Acquire()
	defer sem.Release()
	return conn.conn.Close()
}

func (conn *ZkConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc ChangeFunc) error {
	sem.Acquire()
	defer sem.Release()
	return conn.conn.RetryChange(path, flags, acl, func(oldValue string, oldStat *zookeeper.Stat) (newValue string, err error) {
		return changeFunc(oldValue, oldStat)
	})
}

func (conn *ZkConn) ACL(path string) (acls []zookeeper.ACL, stat Stat, err error) {
	sem.Acquire()
	defer sem.Release()
	acls, s, err := conn.conn.ACL(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	sem.Acquire()
	defer sem.Release()
	return conn.conn.SetACL(path, aclv, version)
}
