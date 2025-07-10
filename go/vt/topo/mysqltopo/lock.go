/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// MySQLLockDescriptor implements topo.LockDescriptor for MySQL.
type MySQLLockDescriptor struct {
	server   *Server
	path     string
	contents string
	ttl      time.Duration

	// heartbeat context and cancel function
	ctx    context.Context
	cancel context.CancelFunc
}

// Check is part of the topo.LockDescriptor interface.
func (ld *MySQLLockDescriptor) Check(ctx context.Context) error {
	if err := ld.server.checkClosed(); err != nil {
		return convertError(err, ld.path)
	}

	// Check if the lock still exists and hasn't expired
	var exists bool
	err := ld.server.db.QueryRowContext(ctx,
		"SELECT 1 FROM topo_locks WHERE path = ? AND expires_at > NOW()",
		ld.path).Scan(&exists)

	if err == sql.ErrNoRows {
		return topo.NewError(topo.NoNode, ld.path)
	}
	if err != nil {
		return convertError(err, ld.path)
	}

	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *MySQLLockDescriptor) Unlock(ctx context.Context) error {
	// Stop the heartbeat goroutine first
	if ld.cancel != nil {
		ld.cancel()
		ld.cancel = nil // Prevent double cancellation
	}

	if err := ld.server.checkClosed(); err != nil {
		return convertError(err, ld.path)
	}

	// Remove the lock
	result, err := ld.server.db.ExecContext(ctx, "DELETE FROM topo_locks WHERE path = ?", ld.path)
	if err != nil {
		return convertError(err, ld.path)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return convertError(err, ld.path)
	}
	if rowsAffected == 0 {
		// Lock was already removed or expired - this should be an error for double unlock
		return topo.NewError(topo.NoNode, ld.path)
	}

	return nil
}

// Lock is part of the topo.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return s.LockWithTTL(ctx, dirPath, contents, time.Duration(lockTTL)*time.Second)
}

// LockWithTTL is part of the topo.Conn interface.
func (s *Server) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullPath := s.fullPath(dirPath)

	// Check if the directory exists by looking for any files under it
	var exists bool
	likePattern := fullPath + "%"
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM topo_data WHERE path LIKE ? LIMIT 1", likePattern).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, topo.NewError(topo.NoNode, fullPath)
	}
	if err != nil {
		return nil, convertError(err, fullPath)
	}

	return s.acquireLock(ctx, fullPath, contents, ttl, false)
}

// LockName is part of the topo.Conn interface.
func (s *Server) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullPath := s.fullPath(dirPath)

	// Named locks have a static 24 hour TTL
	ttl := 24 * time.Hour

	return s.acquireLock(ctx, fullPath, contents, ttl, false)
}

// TryLock is part of the topo.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullPath := s.fullPath(dirPath)

	// Check if the directory exists by looking for any files under it
	var exists bool
	likePattern := fullPath + "%"
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM topo_data WHERE path LIKE ? LIMIT 1", likePattern).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, topo.NewError(topo.NoNode, fullPath)
	}
	if err != nil {
		return nil, convertError(err, fullPath)
	}

	return s.acquireLock(ctx, fullPath, contents, time.Duration(lockTTL)*time.Second, true)
}

// acquireLock attempts to acquire a lock with the given parameters.
func (s *Server) acquireLock(ctx context.Context, path, contents string, ttl time.Duration, tryLock bool) (topo.LockDescriptor, error) {
	expiresAt := time.Now().Add(ttl)

	for {
		// Clean up any expired locks first
		_, err := s.db.ExecContext(ctx, "DELETE FROM topo_locks WHERE expires_at < NOW()")
		if err != nil {
			return nil, convertError(err, path)
		}

		// Try to acquire the lock
		_, err = s.db.ExecContext(ctx,
			"INSERT INTO topo_locks (path, contents, expires_at) VALUES (?, ?, ?)",
			path, contents, expiresAt)

		if err == nil {
			// Lock acquired successfully
			break
		}

		// Check if it's a duplicate key error (lock already exists)
		if isDuplicateKeyError(err) {
			if tryLock {
				return nil, topo.NewError(topo.NodeExists, path)
			}

			// Wait a bit and try again
			select {
			case <-ctx.Done():
				return nil, convertError(ctx.Err(), path)
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		// Other error
		return nil, convertError(err, path)
	}

	// Create the lock descriptor with heartbeat
	lockCtx, cancel := context.WithCancel(s.ctx)
	ld := &MySQLLockDescriptor{
		server:   s,
		path:     path,
		contents: contents,
		ttl:      ttl,
		ctx:      lockCtx,
		cancel:   cancel,
	}

	// Start heartbeat goroutine to keep the lock alive
	go ld.heartbeat()

	return ld, nil
}

// heartbeat keeps the lock alive by periodically updating its expiration time.
func (ld *MySQLLockDescriptor) heartbeat() {
	ticker := time.NewTicker(ld.ttl / 3) // Refresh at 1/3 of TTL
	defer ticker.Stop()

	for {
		select {
		case <-ld.ctx.Done():
			return
		case <-ticker.C:
			// Update the lock expiration
			newExpiresAt := time.Now().Add(ld.ttl)
			_, err := ld.server.db.ExecContext(ld.ctx,
				"UPDATE topo_locks SET expires_at = ? WHERE path = ?",
				newExpiresAt, ld.path)

			if err != nil {
				log.Warningf("Failed to refresh lock for path %s: %v", ld.path, err)
				// The lock may have been lost, but we'll let Check() handle detection
			}
		}
	}
}

// isDuplicateKeyError checks if the error is a MySQL duplicate key error.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Duplicate entry") || strings.Contains(errStr, "duplicate key")
}
