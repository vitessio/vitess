// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"launchpad.net/gozk/zookeeper"
)

// ZkConn is a client class that implements zk.Conn using a zookeeper.Conn
type ZkConn struct {
	conn *zookeeper.Conn
}

func NewZkConn(conn *zookeeper.Conn) *ZkConn {
	return &ZkConn{conn}
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

func (conn *ZkConn) Exists(path string) (exists bool, stat Stat, err error) {
	s, err := conn.conn.Exists(path)
	return s != nil, s, err
}

func (conn *ZkConn) ExistsW(path string) (exists bool, stat Stat, watch <-chan zookeeper.Event, err error) {
	s, w, err := conn.conn.ExistsW(path)
	return s != nil, s, w, err
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
