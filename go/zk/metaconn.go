// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"strings"
	"time"

	"launchpad.net/gozk/zookeeper"
)

type Conn interface {
	Get(path string) (data string, stat *zookeeper.Stat, err error)
	GetW(path string) (data string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error)

	Children(path string) (children []string, stat *zookeeper.Stat, err error)
	ChildrenW(path string) (children []string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error)

	Exists(path string) (stat *zookeeper.Stat, err error)
	ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error)

	Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error)

	Set(path, value string, version int) (stat *zookeeper.Stat, err error)

	Delete(path string, version int) (err error)

	Close() error

	RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zookeeper.ChangeFunc) error

	ACL(path string) ([]zookeeper.ACL, *zookeeper.Stat, error)
	SetACL(path string, aclv []zookeeper.ACL, version int) error
}

/* Smooth API to talk to any zk path in the global system.  Emulates
"/zk/local" paths by guessing and substituting the correct cell for
your current environment.  */

type MetaConn struct {
	connCache *ConnCache
}

func resolveZkPath(path string) string {
	cell := ZkCellFromZkPath(path)
	if cell != "local" {
		return path
	}
	parts := strings.Split(path, "/")
	parts[2] = guessLocalCell()
	return strings.Join(parts, "/")
}

func (conn *MetaConn) Get(path string) (data string, stat *zookeeper.Stat, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.Get(resolveZkPath(path))
}

func (conn *MetaConn) GetW(path string) (data string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.GetW(resolveZkPath(path))
}

func (conn *MetaConn) Children(path string) (children []string, stat *zookeeper.Stat, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.Children(resolveZkPath(path))
}

func (conn *MetaConn) ChildrenW(path string) (children []string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.ChildrenW(resolveZkPath(path))
}

func (conn *MetaConn) Exists(path string) (stat *zookeeper.Stat, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.Exists(resolveZkPath(path))
}

func (conn *MetaConn) ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.ExistsW(resolveZkPath(path))
}

func (conn *MetaConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.Create(resolveZkPath(path), value, flags, aclv)
}

func (conn *MetaConn) Set(path, value string, version int) (stat *zookeeper.Stat, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.Set(resolveZkPath(path), value, version)

}
func (conn *MetaConn) Delete(path string, version int) (err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.Delete(resolveZkPath(path), version)
}

func (conn *MetaConn) Close() error {
	return conn.connCache.Close()
}

func (conn *MetaConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zookeeper.ChangeFunc) error {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return err
	}
	return zconn.RetryChange(resolveZkPath(path), flags, acl, changeFunc)
}

func (conn *MetaConn) ACL(path string) ([]zookeeper.ACL, *zookeeper.Stat, error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return nil, nil, err
	}
	return zconn.ACL(path)
}

func (conn *MetaConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return err
	}
	return zconn.SetACL(path, aclv, version)
}

/*
 NOTE(msolomon) not a good idea
func (conn *MetaConn) GetSession(path string) <-chan zookeeper.Event {
	return conn.connCache.SessionForPath(path)
}
*/

func NewMetaConn(connectTimeout time.Duration) *MetaConn {
	return &MetaConn{NewConnCache(connectTimeout)}
}
