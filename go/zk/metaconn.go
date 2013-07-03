// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"math/rand"
	"strings"
	"time"

	"launchpad.net/gozk/zookeeper"
)

type Stat interface {
	Czxid() int64
	Mzxid() int64
	CTime() time.Time
	MTime() time.Time
	Version() int
	CVersion() int
	AVersion() int
	EphemeralOwner() int64
	DataLength() int
	NumChildren() int
	Pzxid() int64
}

// This interface is really close to the zookeeper connection
// interface.  It uses the Stat interface defined here instead of the
// zookeeper.Stat structure for stats. Everything else is the same as
// in zookeeper.  So refer to the zookeeper docs for the conventions
// used here (for instance, using -1 as version to specify any
// version)
type Conn interface {
	Get(path string) (data string, stat Stat, err error)
	GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error)

	Children(path string) (children []string, stat Stat, err error)
	ChildrenW(path string) (children []string, stat Stat, watch <-chan zookeeper.Event, err error)

	Exists(path string) (stat Stat, err error)
	ExistsW(path string) (stat Stat, watch <-chan zookeeper.Event, err error)

	Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error)

	Set(path, value string, version int) (stat Stat, err error)

	Delete(path string, version int) (err error)

	Close() error

	RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc ChangeFunc) error

	ACL(path string) ([]zookeeper.ACL, Stat, error)
	SetACL(path string, aclv []zookeeper.ACL, version int) error
}

type ChangeFunc func(oldValue string, oldStat Stat) (newValue string, err error)

// Smooth API to talk to any zk path in the global system.  Emulates
// "/zk/local" paths by guessing and substituting the correct cell for
// your current environment.

type MetaConn struct {
	connCache *ConnCache
}

func resolveZkPath(path string) string {
	cell, err := ZkCellFromZkPath(path)
	if err != nil {
		// ConnForPath was already called on path
		panic(err) // should never happen
	}
	if cell != "local" {
		return path
	}
	parts := strings.Split(path, "/")
	parts[2] = GuessLocalCell()
	return strings.Join(parts, "/")
}

const (
	maxAttempts = 2
)

// Some errors are not gracefully handled by zookeeper client. This is
// sort of odd, but in general it doesn't affect the kind of code you
// need to have a truly reliable watcher.
//
// However, it can manifest itself as an annoying transient error that
// is likely avoidable when trying simple operations like Get.
// To that end, we retry when possible to minimize annoyance at
// higher levels.
//
// Metaconn is a better place to do this than the actual ZkConn glue
// because you are already doing an implicit connect per call.
//
// https://issues.apache.org/jira/browse/ZOOKEEPER-22
func shouldRetry(err error) bool {
	if err != nil && zookeeper.IsError(err, zookeeper.ZCONNECTIONLOSS) {
		// This is slightly gross, but we should inject a bit of backoff
		// here to give zk a chance to correct itself.
		time.Sleep(1*time.Second + time.Duration(rand.Int63n(5e9)))
		return true
	}
	return false
}

func (conn *MetaConn) Get(path string) (data string, stat Stat, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		data, stat, err = zconn.Get(resolveZkPath(path))
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.GetW(resolveZkPath(path))
}

func (conn *MetaConn) Children(path string) (children []string, stat Stat, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		children, stat, err = zconn.Children(resolveZkPath(path))
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) ChildrenW(path string) (children []string, stat Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.ChildrenW(resolveZkPath(path))
}

func (conn *MetaConn) Exists(path string) (stat Stat, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		stat, err = zconn.Exists(resolveZkPath(path))
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) ExistsW(path string) (stat Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.ExistsW(resolveZkPath(path))
}

func (conn *MetaConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		pathCreated, err = zconn.Create(resolveZkPath(path), value, flags, aclv)
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) Set(path, value string, version int) (stat Stat, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		stat, err = zconn.Set(resolveZkPath(path), value, version)
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) Delete(path string, version int) (err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		err = zconn.Delete(resolveZkPath(path), version)
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) Close() error {
	return conn.connCache.Close()
}

func (conn *MetaConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc ChangeFunc) error {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return err
	}
	return zconn.RetryChange(resolveZkPath(path), flags, acl, changeFunc)
}

func (conn *MetaConn) ACL(path string) (acl []zookeeper.ACL, stat Stat, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		acl, stat, err = zconn.ACL(path)
		if !shouldRetry(err) {
			return
		}
	}
	return
}

func (conn *MetaConn) SetACL(path string, aclv []zookeeper.ACL, version int) (err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		err = zconn.SetACL(path, aclv, version)
		if !shouldRetry(err) {
			return
		}
	}

	return
}

// Implements expvar.Var()
func (conn *MetaConn) String() string {
	return conn.connCache.String()
}

func NewMetaConn(useZkocc bool) *MetaConn {
	return &MetaConn{NewConnCache(useZkocc)}
}
