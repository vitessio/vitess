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
	"log/slog"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// MySQLLockDescriptor implements topo.LockDescriptor for MySQL.
type MySQLLockDescriptor struct {
	server *Server
	path   string
	// contents is the exact value stored in the topo_locks row: the
	// caller-provided contents plus a per-acquisition unique suffix (see
	// acquireLock). Check/Unlock and the heartbeat match on (path,
	// contents) so a descriptor can only ever validate, extend, or delete
	// the row it inserted itself — never a lock the same path acquired
	// later by another process after ours expired.
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

	// Check that OUR lock row (not merely any lock on the path) still
	// exists and hasn't expired. Matching on path alone would let a
	// descriptor whose row expired and was re-acquired by another process
	// report success — both holders would then believe they own the lock.
	var exists bool
	err := ld.server.db.QueryRowContext(ctx,
		"SELECT 1 FROM topo_locks WHERE path = ? AND contents = ? AND expires_at > NOW()",
		ld.path, ld.contents).Scan(&exists)

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

	// Remove OUR lock row only. If our row expired and another process
	// has since acquired the same path, deleting by path alone would
	// release the other process's live lock.
	result, err := ld.server.db.ExecContext(ctx,
		"DELETE FROM topo_locks WHERE path = ? AND contents = ?", ld.path, ld.contents)
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
// Before we acquire a lock, we check that the row exists in topo_data
func (s *Server) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullPath := s.resolvePath(dirPath)
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM topo_data WHERE path LIKE ? LIMIT 1", matchDirectory(fullPath)).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, topo.NewError(topo.NoNode, fullPath)
	}
	if err != nil {
		return nil, convertError(err, fullPath)
	}

	return s.acquireLock(ctx, s.resolvePath(dirPath), contents, ttl, false)
}

// LockName is part of the topo.Conn interface.
func (s *Server) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullPath := s.resolvePath(dirPath)

	// Named locks have a static 24 hour TTL
	ttl := 24 * time.Hour

	return s.acquireLock(ctx, fullPath, contents, ttl, false)
}

// TryLock is part of the topo.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullPath := s.resolvePath(dirPath)
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM topo_data WHERE path LIKE ? LIMIT 1", matchDirectory(fullPath)).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, topo.NewError(topo.NoNode, fullPath)
	}
	if err != nil {
		return nil, convertError(err, fullPath)
	}

	return s.acquireLock(ctx, s.resolvePath(dirPath), contents, time.Duration(lockTTL)*time.Second, true)
}

// acquireLock attempts to acquire a lock with the given parameters.
func (s *Server) acquireLock(ctx context.Context, path, contents string, ttl time.Duration, tryLock bool) (topo.LockDescriptor, error) {
	// Suffix the stored contents with a per-acquisition unique token so this
	// descriptor's Check/Unlock/heartbeat can match its own row exactly.
	// Caller-provided contents (typically json with hostname/action) are not
	// guaranteed unique across processes.
	ownedContents := contents + "\nlock-uid:" + uuid.NewString()

	for {
		// Compute the expiry inside the loop: a blocking acquisition can wait
		// on a contended path for longer than the TTL, and an expiry computed
		// once up front would insert a row that is already (or nearly)
		// expired — immediately reapable by any other acquirer's cleanup
		// DELETE below, allowing two processes to hold the same lock.
		expiresAt := time.Now().Add(ttl)

		// Clean up any expired locks first
		_, err := s.db.ExecContext(ctx, "DELETE FROM topo_locks WHERE expires_at < NOW()")
		if err != nil {
			return nil, convertError(err, path)
		}

		// Try to acquire the lock using INSERT IGNORE
		result, err := s.db.ExecContext(ctx,
			"INSERT IGNORE INTO topo_locks (path, contents, expires_at) VALUES (?, ?, ?)",
			path, ownedContents, expiresAt)
		if err != nil {
			// Unexpected error (not duplicate key related)
			return nil, convertError(err, path)
		}

		// Check if the insert was successful by examining affected rows
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, convertError(err, path)
		}

		if rowsAffected > 0 {
			// Lock acquired successfully
			break
		}

		// Lock already exists (rowsAffected == 0)
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

	// Create the lock descriptor with heartbeat
	lockCtx, cancel := context.WithCancel(s.ctx)
	ld := &MySQLLockDescriptor{
		server:   s,
		path:     path,
		contents: ownedContents,
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
			// Update OUR lock row's expiration. Matching on (path, contents)
			// means an expired-and-reaped lock is never resurrected, and a
			// row re-acquired by another process is never extended by us.
			newExpiresAt := time.Now().Add(ld.ttl)
			result, err := ld.server.db.ExecContext(ld.ctx,
				"UPDATE topo_locks SET expires_at = ? WHERE path = ? AND contents = ?",
				newExpiresAt, ld.path, ld.contents)
			if err != nil {
				log.Warn("Failed to refresh lock", slog.String("path", ld.path), slog.Any("error", err))
				// The lock may have been lost, but we'll let Check() handle detection
				continue
			}
			if n, err := result.RowsAffected(); err == nil && n == 0 {
				// Our row is gone: it expired and was reaped (and possibly
				// re-acquired by another process). Do not re-insert it —
				// mutual exclusion is already lost; surface loudly and let
				// Check() report NoNode to the holder.
				log.Error("Lock row disappeared while held; lock was lost",
					slog.String("path", ld.path))
			}
		}
	}
}
