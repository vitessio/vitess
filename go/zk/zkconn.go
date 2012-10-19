// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"fmt"
	"launchpad.net/gozk/zookeeper"
	"time"
)

// ZkConn is a client class that implements zk.Conn using a zookeeper.Conn
type ZkConn struct {
	conn *zookeeper.Conn
}

// Dial a ZK server and waits for connection event. Returns a ZkConn
// encapsulating the zookeeper.Conn, and the zookeeper session event
// channel to monitor the connection
//
// The value for recvTimeout is used as a session timeout as well, and
// will be used to negotiate a 'good' value with the server. From
// reading the zookeeper source code, it has to be between 6 and 60
// seconds (2x and 20x the tickTime by default, with default tick time
// being 3 seconds).  min session time, max session time and ticktime
// can all be overwritten on the zookeeper server side, so these
// numbers may vary.
//
// Then this recvTimeout is used to compute other related timeouts:
// - connect timeout is 1/3 of recvTimeout
// - recv timeout is 2/3 of recvTimeout minus a ping time
// - send timeout is 1/3 of recvTimeout
// - we try to send a ping a least every recvTimeout / 3
//
// Note the recvTimeout has *nothing* to do with the time between we
// call Dial and the maximum time before we receive the event on the
// session. The library will actually try to re-connect in the background
// (after each timeout), and may *never* send an event if the TCP connections
// always fail. Use DialZkTimeout to enforce a timeout for the initial connect.
func DialZk(zkAddr string, recvTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	zconn, session, err := zookeeper.Dial(zkAddr, recvTimeout)
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

func DialZkTimeout(zkAddr string, recvTimeout time.Duration, connectTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	zconn, session, err := zookeeper.Dial(zkAddr, recvTimeout)
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
	return conn.conn.Get(path)
}

func (conn *ZkConn) GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error) {
	return conn.conn.GetW(path)
}

func (conn *ZkConn) Children(path string) (children []string, stat Stat, err error) {
	return conn.conn.Children(path)
}

func (conn *ZkConn) ChildrenW(path string) (children []string, stat Stat, watch <-chan zookeeper.Event, err error) {
	return conn.conn.ChildrenW(path)
}

func (conn *ZkConn) Exists(path string) (stat Stat, err error) {
	s, err := conn.conn.Exists(path)
	if s == nil {
		return nil, err
	}
	return s, err
}

func (conn *ZkConn) ExistsW(path string) (stat Stat, watch <-chan zookeeper.Event, err error) {
	s, w, err := conn.conn.ExistsW(path)
	if s == nil {
		return nil, w, err
	}
	return s, w, err
}

func (conn *ZkConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	return conn.conn.Create(path, value, flags, aclv)
}

func (conn *ZkConn) Set(path, value string, version int) (stat Stat, err error) {
	return conn.conn.Set(path, value, version)
}

func (conn *ZkConn) Delete(path string, version int) (err error) {
	return conn.conn.Delete(path, version)
}

func (conn *ZkConn) Close() error {
	return conn.conn.Close()
}

func (conn *ZkConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zookeeper.ChangeFunc) error {
	return conn.conn.RetryChange(path, flags, acl, changeFunc)
}

func (conn *ZkConn) ACL(path string) ([]zookeeper.ACL, Stat, error) {
	return conn.conn.ACL(path)
}

func (conn *ZkConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	return conn.conn.SetACL(path, aclv, version)
}
