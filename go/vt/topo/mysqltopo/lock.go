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
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// lockDescriptor implements topo.LockDescriptor.
type lockDescriptor struct {
	s       *Server
	dirPath string
	lockID  string
}

// Lock is part of the topo.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return s.LockWithTTL(ctx, dirPath, contents, *mysqlTopoLockTTL)
}

// LockWithTTL is part of the topo.Conn interface.
func (s *Server) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	// Convert to full path using the server's root
	fullDirPath := s.fullPath(dirPath)

	// Check if the directory exists
	if fullDirPath != "/" {
		result, err := s.queryRow(`
			SELECT EXISTS(
				SELECT 1 FROM (
					SELECT 1 FROM topo_files WHERE path LIKE %s
					UNION
					SELECT 1 FROM topo_directories WHERE path = %s
				) as subquery
			)
		`, fullDirPath+"/%", fullDirPath)

		if err != nil {
			return nil, convertError(err, dirPath)
		}

		if len(result.Rows) == 0 {
			return nil, topo.NewError(topo.NoNode, dirPath)
		}

		exists, err := result.Rows[0][0].ToBool()
		if err != nil {
			return nil, convertError(err, dirPath)
		}

		if !exists {
			return nil, topo.NewError(topo.NoNode, dirPath)
		}
	}

	// Create a unique lock ID
	lockID := fmt.Sprintf("%v", time.Now().UnixNano())

	// Calculate expiration time
	expiration := time.Now().Add(ttl).Format("2006-01-02 15:04:05")

	// Try to acquire the lock with proper transaction isolation and lock timeouts
	for {
		select {
		case <-ctx.Done():
			return nil, convertError(ctx.Err(), dirPath)
		default:
		}

		// Try to acquire the lock using a single transaction
		log.Infof("Attempting to acquire lock for path %s with lockID %s", fullDirPath, lockID)

		acquired, err := s.tryAcquireLockInTransaction(fullDirPath, lockID, contents, expiration)
		if err != nil {
			log.Errorf("Failed to acquire lock: %v", err)
			return nil, convertError(err, dirPath)
		}

		if acquired {
			// Lock acquired
			log.Infof("Lock acquired successfully for path %s with lockID %s", fullDirPath, lockID)
			return &lockDescriptor{
				s:       s,
				dirPath: fullDirPath,
				lockID:  lockID,
			}, nil
		}

		// Lock not acquired, wait and try again
		select {
		case <-ctx.Done():
			return nil, convertError(ctx.Err(), dirPath)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// LockName is part of the topo.Conn interface.
func (s *Server) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// Named locks don't require the path to exist, so we skip the directory existence check
	// Convert to full path using the server's root
	fullDirPath := s.fullPath(dirPath)

	// Create parent directories if needed for named locks
	if err := s.createParentDirectories(ctx, fullDirPath); err != nil {
		return nil, convertError(err, dirPath)
	}

	// Create a unique lock ID
	lockID := fmt.Sprintf("%v", time.Now().UnixNano())

	// Use a 24-hour TTL as specified in the interface
	ttl := 24 * time.Hour
	expiration := time.Now().Add(ttl).Format("2006-01-02 15:04:05")

	// Try to acquire the lock with proper transaction isolation and lock timeouts
	for {
		select {
		case <-ctx.Done():
			return nil, convertError(ctx.Err(), dirPath)
		default:
		}

		// Try to acquire the lock using a single transaction
		log.Infof("Attempting to acquire named lock for path %s with lockID %s", fullDirPath, lockID)

		acquired, err := s.tryAcquireLockInTransaction(fullDirPath, lockID, contents, expiration)
		if err != nil {
			log.Errorf("Failed to acquire named lock: %v", err)
			return nil, convertError(err, dirPath)
		}

		if acquired {
			// Lock acquired
			log.Infof("Named lock acquired successfully for path %s with lockID %s", fullDirPath, lockID)
			return &lockDescriptor{
				s:       s,
				dirPath: fullDirPath,
				lockID:  lockID,
			}, nil
		}

		// Lock not acquired, wait and try again
		select {
		case <-ctx.Done():
			return nil, convertError(ctx.Err(), dirPath)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TryLock is part of the topo.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// Convert to full path using the server's root
	fullDirPath := s.fullPath(dirPath)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the directory exists
	if fullDirPath != "/" {
		result, err := s.queryRowUnsafe(`
			SELECT EXISTS(
				SELECT 1 FROM (
					SELECT 1 FROM topo_files WHERE path LIKE %s
					UNION
					SELECT 1 FROM topo_directories WHERE path = %s
				) as subquery
			)
		`, fullDirPath+"/%", fullDirPath)

		if err != nil {
			return nil, convertError(err, dirPath)
		}

		if len(result.Rows) == 0 {
			return nil, topo.NewError(topo.NoNode, dirPath)
		}

		exists, err := result.Rows[0][0].ToBool()
		if err != nil {
			return nil, convertError(err, dirPath)
		}

		if !exists {
			return nil, topo.NewError(topo.NoNode, dirPath)
		}
	}

	// Try to acquire the lock atomically with proper transaction isolation
	lockID := fmt.Sprintf("%v", time.Now().UnixNano())
	expiration := time.Now().Add(*mysqlTopoLockTTL).Format("2006-01-02 15:04:05")

	// Try to acquire the lock using a single transaction (without additional mutex)
	acquired, err := s.tryAcquireLockInTransactionUnsafe(fullDirPath, lockID, contents, expiration)
	if err != nil {
		return nil, convertError(err, dirPath)
	}

	if acquired {
		// Lock acquired
		return &lockDescriptor{
			s:       s,
			dirPath: fullDirPath,
			lockID:  lockID,
		}, nil
	}

	// Lock not acquired, return NodeExists error
	return nil, topo.NewError(topo.NodeExists, dirPath)
}

// Check is part of the topo.LockDescriptor interface.
func (ld *lockDescriptor) Check(ctx context.Context) error {
	result, err := ld.s.queryRow("SELECT owner, expiration FROM topo_locks WHERE path = %s", ld.dirPath)
	if err != nil {
		return err
	}

	if len(result.Rows) == 0 {
		return fmt.Errorf("lock lost")
	}

	owner := result.Rows[0][0].ToString()
	expirationStr := result.Rows[0][1].ToString()

	if owner != ld.lockID {
		return fmt.Errorf("lock was lost (owner mismatch: expected %s, got %s)", ld.lockID, owner)
	}

	// Parse the expiration time
	expiration, err := time.Parse("2006-01-02 15:04:05", expirationStr)
	if err != nil {
		return fmt.Errorf("failed to parse expiration time: %v", err)
	}

	// Only refresh the lock if it's going to expire soon (within 1 minute)
	if expiration.Before(time.Now().Add(1 * time.Minute)) {
		newExpiration := time.Now().Add(*mysqlTopoLockTTL).Format("2006-01-02 15:04:05")
		_, err = ld.s.exec("UPDATE topo_locks SET expiration = %s WHERE path = %s AND owner = %s",
			newExpiration, ld.dirPath, ld.lockID)
		if err != nil {
			return err
		}
	}

	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *lockDescriptor) Unlock(ctx context.Context) error {
	result, err := ld.s.exec("DELETE FROM topo_locks WHERE path = %s AND owner = %s", ld.dirPath, ld.lockID)
	if err != nil {
		return err
	}

	// Check if we actually deleted a lock
	if result.RowsAffected == 0 {
		return fmt.Errorf("lock not found or already unlocked")
	}

	return nil
}

// tryAcquireLockInTransaction attempts to acquire a lock atomically using a MySQL transaction.
// It returns true if the lock was successfully acquired, false if another lock exists.
func (s *Server) tryAcquireLockInTransaction(dirPath, lockID, contents, expiration string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return false, fmt.Errorf("connection closed")
	}

	// Start a transaction
	_, err := s.conn.ExecuteFetch("START TRANSACTION", 0, false)
	if err != nil {
		return false, err
	}

	// Ensure we rollback on any error
	defer func() {
		if err != nil {
			_, _ = s.conn.ExecuteFetch("ROLLBACK", 0, false)
		}
	}()

	// First, clean up any expired locks for this path
	_, err = s.conn.ExecuteFetch(expandQuery(
		"DELETE FROM topo_locks WHERE path = %s AND expiration < NOW()",
		dirPath,
	), 0, false)
	if err != nil {
		return false, err
	}

	// Try to acquire the lock by inserting a new record
	// Use INSERT IGNORE to avoid errors if a lock already exists
	result, err := s.conn.ExecuteFetch(expandQuery(
		"INSERT IGNORE INTO topo_locks (path, owner, contents, expiration) VALUES (%s, %s, %s, %s)",
		dirPath, lockID, contents, expiration,
	), 0, false)
	if err != nil {
		return false, err
	}

	// Check if we successfully inserted the lock
	lockAcquired := result.RowsAffected > 0

	if lockAcquired {
		// Commit the transaction
		_, err = s.conn.ExecuteFetch("COMMIT", 0, false)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// Lock was not acquired, rollback and return false
	_, err = s.conn.ExecuteFetch("ROLLBACK", 0, false)
	if err != nil {
		return false, err
	}

	return false, nil
}

// tryAcquireLockInTransactionUnsafe attempts to acquire a lock atomically using a MySQL transaction.
// This version assumes the mutex is already held by the caller.
func (s *Server) tryAcquireLockInTransactionUnsafe(dirPath, lockID, contents, expiration string) (bool, error) {
	if s.conn == nil {
		return false, fmt.Errorf("connection closed")
	}

	// Start a transaction
	_, err := s.conn.ExecuteFetch("START TRANSACTION", 0, false)
	if err != nil {
		return false, err
	}

	// Ensure we rollback on any error
	defer func() {
		if err != nil {
			_, _ = s.conn.ExecuteFetch("ROLLBACK", 0, false)
		}
	}()

	// First, clean up any expired locks for this path
	_, err = s.conn.ExecuteFetch(expandQuery(
		"DELETE FROM topo_locks WHERE path = %s AND expiration < NOW()",
		dirPath,
	), 0, false)
	if err != nil {
		return false, err
	}

	// Try to acquire the lock by inserting a new record
	// Use INSERT IGNORE to avoid errors if a lock already exists
	result, err := s.conn.ExecuteFetch(expandQuery(
		"INSERT IGNORE INTO topo_locks (path, owner, contents, expiration) VALUES (%s, %s, %s, %s)",
		dirPath, lockID, contents, expiration,
	), 0, false)
	if err != nil {
		return false, err
	}

	// Check if we successfully inserted the lock
	lockAcquired := result.RowsAffected > 0

	if lockAcquired {
		// Commit the transaction
		_, err = s.conn.ExecuteFetch("COMMIT", 0, false)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// Lock was not acquired, rollback and return false
	_, err = s.conn.ExecuteFetch("ROLLBACK", 0, false)
	if err != nil {
		return false, err
	}

	return false, nil
}
