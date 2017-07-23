/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zk

import (
	"math/rand"
	"strings"
	"time"

	zookeeper "github.com/samuel/go-zookeeper/zk"
)

// Conn is really close to the zookeeper library connection interface.
// So refer to the zookeeper docs for the conventions
// used here (for instance, using -1 as version to specify any
// version)
type Conn interface {
	Get(path string) (data []byte, stat *zookeeper.Stat, err error)
	GetW(path string) (data []byte, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error)

	Children(path string) (children []string, stat *zookeeper.Stat, err error)
	ChildrenW(path string) (children []string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error)

	Exists(path string) (stat *zookeeper.Stat, err error)
	ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error)

	Create(path string, value []byte, flags int, aclv []zookeeper.ACL) (pathCreated string, err error)

	Set(path string, value []byte, version int32) (stat *zookeeper.Stat, err error)

	Delete(path string, version int32) (err error)

	Close() error

	ACL(path string) ([]zookeeper.ACL, *zookeeper.Stat, error)
	SetACL(path string, aclv []zookeeper.ACL, version int32) error
}

// Smooth API to talk to any zk path in the global system.  Emulates
// "/zk/local" paths by guessing and substituting the correct cell for
// your current environment.

// MetaConn is an implementation of Conn that routes to multiple cells.
// It uses the <cell> in /zk/<cell>/... paths to decide which ZK cluster to
// send a given request to.
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
	if err == zookeeper.ErrConnectionClosed {
		// This is slightly gross, but we should inject a bit of backoff
		// here to give zk a chance to correct itself.
		time.Sleep(1*time.Second + time.Duration(rand.Int63n(5e9)))
		return true
	}
	return false
}

// Get implements Conn.
func (conn *MetaConn) Get(path string) (data []byte, stat *zookeeper.Stat, err error) {
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

// GetW implements Conn.
func (conn *MetaConn) GetW(path string) (data []byte, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.GetW(resolveZkPath(path))
}

// Children implements Conn.
func (conn *MetaConn) Children(path string) (children []string, stat *zookeeper.Stat, err error) {
	if path == ("/" + MagicPrefix) {
		// NOTE(msolo) There is a slight hack there - but there really is
		// no valid stat for the top level path.
		children, err = ZkKnownCells()
		return
	}
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

// ChildrenW implements Conn.
func (conn *MetaConn) ChildrenW(path string) (children []string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.ChildrenW(resolveZkPath(path))
}

// Exists implements Conn.
func (conn *MetaConn) Exists(path string) (stat *zookeeper.Stat, err error) {
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

// ExistsW implements Conn.
func (conn *MetaConn) ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	zconn, err := conn.connCache.ConnForPath(path)
	if err != nil {
		return
	}
	return zconn.ExistsW(resolveZkPath(path))
}

// Create implements Conn.
func (conn *MetaConn) Create(path string, value []byte, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	var zconn Conn
	for i := 0; i < maxAttempts; i++ {
		zconn, err = conn.connCache.ConnForPath(path)
		if err != nil {
			return
		}
		path = resolveZkPath(path)
		pathCreated, err = zconn.Create(path, value, flags, aclv)
		if err == zookeeper.ErrNoNode {
			parts := strings.Split(path, "/")
			if len(parts) == 3 && parts[0] == "" && parts[1] == MagicPrefix {
				// We were asked to create a /zk/<cell> path, but /zk doesn't exist.
				// We should create /zk automatically in this case, because it's
				// impossible to create /zk via MetaConn, since there's no cell name.
				_, err = zconn.Create("/"+MagicPrefix, nil, 0, zookeeper.WorldACL(zookeeper.PermAll))
				if err != nil {
					if shouldRetry(err) {
						continue
					}
					if err != zookeeper.ErrNodeExists {
						return "", err
					}
				}
				// Now try the original path again.
				pathCreated, err = zconn.Create(path, value, flags, aclv)
			}
		}
		if !shouldRetry(err) {
			return
		}
	}
	return
}

// Set implements Conn.
func (conn *MetaConn) Set(path string, value []byte, version int32) (stat *zookeeper.Stat, err error) {
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

// Delete implements Conn.
func (conn *MetaConn) Delete(path string, version int32) (err error) {
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

// Close implements Conn.
func (conn *MetaConn) Close() error {
	return conn.connCache.Close()
}

// ACL implements Conn.
func (conn *MetaConn) ACL(path string) (acl []zookeeper.ACL, stat *zookeeper.Stat, err error) {
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

// SetACL implements Conn.
func (conn *MetaConn) SetACL(path string, aclv []zookeeper.ACL, version int32) (err error) {
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

// NewMetaConn creates a MetaConn.
func NewMetaConn() *MetaConn {
	return &MetaConn{NewConnCache()}
}
