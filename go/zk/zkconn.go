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
func DialZk(zkAddr string, connectTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	zconn, session, err := zookeeper.Dial(zkAddr, connectTimeout)
	if err == nil {
		// Wait for connection.
		// FIXME(msolomon) the deadlines seems to be a bit fuzzy, need to double check
		// and potentially do a high-level select here.
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
