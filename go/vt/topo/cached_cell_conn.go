/*
Copyright 2026 The Vitess Authors.

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

package topo

import (
	"context"
	"sync"
	"time"
)

// cachedCellConn is the Conn that ConnForCell returns when the CellInfo cache is enabled. Reads go to the cell's
// cached connection, which may be up to cellInfoCacheTTL stale. Writes and locks first read the cell's CellInfo
// again, ignoring the TTL, and go to the cell's current connection, so that they never land on a topo server the
// cell has been re-pointed from.
//
// A version read from one topo server means nothing to another: ZooKeeper's counts the writes to that node, so
// the same record at two locations often has the same version. cachedCellConn therefore tags each version it
// returns with the connection it came from, and fails a versioned Update or Delete with BadVersion when that is
// not the cell's current connection, rather than send the version to the new topo server. Callers that
// read-modify-write then read again, from the current connection.
type cachedCellConn struct {
	ts   *Server
	cell string

	// mu protects conn.
	mu sync.Mutex
	// conn is the connection that reads go to. A write moves it to the cell's current connection, so that a
	// caller retrying a write through the same cachedCellConn reads from where it writes.
	conn Conn
}

// cellConnVersion is a Version tagged with the cell connection it was read from.
type cellConnVersion struct {
	Version
	conn Conn
}

func (c *cachedCellConn) readConn() Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// writeConn reads the cell's CellInfo and returns the cell's current connection, which reads go to from then on.
func (c *cachedCellConn) writeConn(ctx context.Context) (Conn, error) {
	conn, err := c.ts.refreshCellConn(ctx, c.cell, true /*force*/)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
	return conn, nil
}

// writeVersion returns the version to send to conn for a write at filePath: version as the backend returned
// it, or a BadVersion error if it was read from another connection.
func writeVersion(conn Conn, filePath string, version Version) (Version, error) {
	cv, ok := version.(cellConnVersion)
	if !ok {
		return version, nil
	}
	if cv.conn != conn {
		return nil, NewError(BadVersion, filePath)
	}
	return cv.Version, nil
}

func tagVersion(conn Conn, version Version) Version {
	if version == nil {
		return nil
	}
	return cellConnVersion{Version: version, conn: conn}
}

// ListDir is part of the Conn interface.
func (c *cachedCellConn) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
	return c.readConn().ListDir(ctx, dirPath, full)
}

// Create is part of the Conn interface.
func (c *cachedCellConn) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return nil, err
	}
	version, err := conn.Create(ctx, filePath, contents)
	return tagVersion(conn, version), err
}

// Update is part of the Conn interface.
func (c *cachedCellConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return nil, err
	}
	if version, err = writeVersion(conn, filePath, version); err != nil {
		return nil, err
	}
	version, err = conn.Update(ctx, filePath, contents, version)
	return tagVersion(conn, version), err
}

// Get is part of the Conn interface.
func (c *cachedCellConn) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
	conn := c.readConn()
	contents, version, err := conn.Get(ctx, filePath)
	return contents, tagVersion(conn, version), err
}

// GetVersion is part of the Conn interface.
func (c *cachedCellConn) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	return c.readConn().GetVersion(ctx, filePath, version)
}

// List is part of the Conn interface.
func (c *cachedCellConn) List(ctx context.Context, filePathPrefix string) ([]KVInfo, error) {
	conn := c.readConn()
	kvs, err := conn.List(ctx, filePathPrefix)
	for i := range kvs {
		kvs[i].Version = tagVersion(conn, kvs[i].Version)
	}
	return kvs, err
}

// Delete is part of the Conn interface.
func (c *cachedCellConn) Delete(ctx context.Context, filePath string, version Version) error {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return err
	}
	if version, err = writeVersion(conn, filePath, version); err != nil {
		return err
	}
	return conn.Delete(ctx, filePath, version)
}

// Lock is part of the Conn interface.
func (c *cachedCellConn) Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Lock(ctx, dirPath, contents)
}

// LockWithTTL is part of the Conn interface.
func (c *cachedCellConn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (LockDescriptor, error) {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.LockWithTTL(ctx, dirPath, contents, ttl)
}

// LockName is part of the Conn interface.
func (c *cachedCellConn) LockName(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.LockName(ctx, dirPath, contents)
}

// TryLock is part of the Conn interface.
func (c *cachedCellConn) TryLock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
	conn, err := c.writeConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.TryLock(ctx, dirPath, contents)
}

// Watch is part of the Conn interface.
func (c *cachedCellConn) Watch(ctx context.Context, filePath string) (*WatchData, <-chan *WatchData, error) {
	conn := c.readConn()
	current, changes, err := conn.Watch(ctx, filePath)
	if err != nil {
		return current, changes, err
	}
	if current != nil {
		tagged := *current
		tagged.Version = tagVersion(conn, current.Version)
		current = &tagged
	}
	if changes == nil {
		return current, nil, nil
	}
	// Forward the changes with their versions tagged, until the watch closes its channel.
	tagged := make(chan *WatchData, cap(changes))
	go func() {
		defer close(tagged)
		for wd := range changes {
			out := *wd
			out.Version = tagVersion(conn, wd.Version)
			tagged <- &out
		}
	}()
	return current, tagged, nil
}

// WatchRecursive is part of the Conn interface.
func (c *cachedCellConn) WatchRecursive(ctx context.Context, path string) ([]*WatchDataRecursive, <-chan *WatchDataRecursive, error) {
	conn := c.readConn()
	current, changes, err := conn.WatchRecursive(ctx, path)
	if err != nil {
		return current, changes, err
	}
	tag := func(wd *WatchDataRecursive) *WatchDataRecursive {
		out := *wd
		out.Version = tagVersion(conn, wd.Version)
		return &out
	}
	for i, wd := range current {
		current[i] = tag(wd)
	}
	if changes == nil {
		return current, nil, nil
	}
	tagged := make(chan *WatchDataRecursive, cap(changes))
	go func() {
		defer close(tagged)
		for wd := range changes {
			tagged <- tag(wd)
		}
	}()
	return current, tagged, nil
}

// NewLeaderParticipation is part of the Conn interface.
func (c *cachedCellConn) NewLeaderParticipation(name, id string) (LeaderParticipation, error) {
	// The refresh read is bounded by RemoteOperationTimeout.
	conn, err := c.writeConn(context.Background())
	if err != nil {
		return nil, err
	}
	return conn.NewLeaderParticipation(name, id)
}

// Close is part of the Conn interface. The cell's connection is the Server's to close, as when ConnForCell
// returns it without the cache, so this closes the connection that reads go to.
func (c *cachedCellConn) Close() {
	c.readConn().Close()
}
