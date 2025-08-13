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
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// MySQLLeaderParticipation implements topo.LeaderParticipation for MySQL.
type MySQLLeaderParticipation struct {
	server   *Server
	name     string
	id       string
	contents string

	// State management
	mu       sync.RWMutex
	isLeader bool
	stopped  bool

	// Leadership context - cancelled when leadership is lost
	leaderCtx    context.Context
	leaderCancel context.CancelFunc

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewLeaderParticipation is part of the topo.Conn interface.
func (s *Server) NewLeaderParticipation(name, id string) (topo.LeaderParticipation, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, name)
	}

	ctx, cancel := context.WithCancel(s.ctx)

	lp := &MySQLLeaderParticipation{
		server:   s,
		name:     name,
		id:       id,
		contents: fmt.Sprintf("Leader: %s", id),
		ctx:      ctx,
		cancel:   cancel,
	}

	return lp, nil
}

// WaitForLeadership is part of the topo.LeaderParticipation interface.
func (lp *MySQLLeaderParticipation) WaitForLeadership() (context.Context, error) {
	lp.mu.Lock()

	if lp.stopped {
		lp.mu.Unlock()
		return nil, topo.NewError(topo.Interrupted, lp.name)
	}

	// If we're already the leader, return the existing context
	if lp.isLeader && lp.leaderCtx != nil {
		ctx := lp.leaderCtx
		lp.mu.Unlock()
		return ctx, nil
	}

	// Start the leadership campaign if not already running
	if !lp.isLeader {
		lp.wg.Add(1)
		go lp.campaignForLeadership()
	}
	lp.mu.Unlock()

	// Wait for leadership to be acquired with a timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(30 * time.Second) // Give more time for election
	defer timeout.Stop()

	for {
		select {
		case <-lp.ctx.Done():
			return nil, topo.NewError(topo.Interrupted, lp.name)
		case <-timeout.C:
			return nil, fmt.Errorf("deadline exceeded: %s", lp.name)
		case <-ticker.C:
			lp.mu.RLock()
			if lp.stopped {
				lp.mu.RUnlock()
				return nil, topo.NewError(topo.Interrupted, lp.name)
			}
			if lp.isLeader && lp.leaderCtx != nil {
				ctx := lp.leaderCtx
				lp.mu.RUnlock()
				return ctx, nil
			}
			lp.mu.RUnlock()
		}
	}
}

// Stop is part of the topo.LeaderParticipation interface.
func (lp *MySQLLeaderParticipation) Stop() {
	lp.mu.Lock()
	if lp.stopped {
		lp.mu.Unlock()
		return
	}
	lp.stopped = true

	// Cancel leadership context if we're the leader
	if lp.leaderCancel != nil {
		lp.leaderCancel()
	}

	// Cancel the main context
	if lp.cancel != nil {
		lp.cancel()
	}
	lp.mu.Unlock()

	// Remove our election record (best effort)
	_, _ = lp.server.db.ExecContext(context.Background(),
		"DELETE FROM topo_elections WHERE name = ? AND leader_id = ?",
		lp.name, lp.id)

	// Wait for campaign goroutine to finish
	lp.wg.Wait()
}

// GetCurrentLeaderID is part of the topo.LeaderParticipation interface.
func (lp *MySQLLeaderParticipation) GetCurrentLeaderID(ctx context.Context) (string, error) {
	if err := lp.server.checkClosed(); err != nil {
		return "", convertError(err, lp.name)
	}

	// Don't clean up expired elections on every call - this can cause race conditions
	// The cleanup will happen during heartbeat and other operations

	var leaderID string
	err := lp.server.db.QueryRowContext(ctx,
		"SELECT leader_id FROM topo_elections WHERE name = ? AND expires_at > NOW()",
		lp.name).Scan(&leaderID)

	if err == sql.ErrNoRows {
		// No leader currently - return empty string, not an error
		return "", nil
	}
	if err != nil {
		return "", convertError(err, lp.name)
	}

	return leaderID, nil
}

// WaitForNewLeader is part of the topo.LeaderParticipation interface.
func (lp *MySQLLeaderParticipation) WaitForNewLeader(ctx context.Context) (<-chan string, error) {
	// This is a simplified implementation that polls for leader changes
	// In a production system, you might want to use MySQL's binlog events
	// or other notification mechanisms for better efficiency

	ch := make(chan string, 8)

	// Get the initial leader synchronously and send it immediately if there is one
	currentLeader, err := lp.GetCurrentLeaderID(ctx)
	if err != nil {
		close(ch)
		// Don't log warnings for interrupted contexts - this is expected during shutdown
		if !topo.IsErrType(err, topo.NoNode) && !topo.IsErrType(err, topo.Interrupted) {
			log.Warningf("Failed to get initial leader: %v", err)
		}
		return ch, nil
	}

	// Send the initial leader only if there is one (matching etcd behavior)
	if currentLeader != "" {
		ch <- currentLeader
	}

	go func() {
		defer close(ch)

		lastLeader := currentLeader
		ticker := time.NewTicker(100 * time.Millisecond) // Poll more frequently for tests
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-lp.ctx.Done():
				return
			case <-ticker.C:
				newLeader, err := lp.GetCurrentLeaderID(ctx)
				if err != nil {
					// Don't log warnings for interrupted contexts - this is expected during shutdown
					if !topo.IsErrType(err, topo.NoNode) && !topo.IsErrType(err, topo.Interrupted) {
						log.Warningf("Failed to get current leader: %v", err)
					}
					continue
				}

				// Only send changes when there's actually a leader (matching etcd behavior)
				if newLeader != lastLeader && newLeader != "" {
					lastLeader = newLeader
					select {
					case ch <- newLeader:
					case <-ctx.Done():
						return
					case <-lp.ctx.Done():
						return
					}
				} else if newLeader == "" && lastLeader != "" {
					// Leader disappeared, update our tracking but don't send empty string
					lastLeader = newLeader
				}
			}
		}
	}()

	return ch, nil
}

// campaignForLeadership runs the leadership campaign.
func (lp *MySQLLeaderParticipation) campaignForLeadership() {
	defer lp.wg.Done()

	// Try to become leader immediately
	if lp.tryBecomeLeader() {
		lp.mu.Lock()
		if !lp.isLeader {
			lp.isLeader = true
			lp.leaderCtx, lp.leaderCancel = context.WithCancel(lp.ctx)
		}
		lp.mu.Unlock()

		// Start heartbeat to maintain leadership
		lp.wg.Add(1)
		go lp.maintainLeadership()
		return
	}

	// If we didn't become leader immediately, keep trying
	ticker := time.NewTicker(time.Duration(electionTTL/3) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-lp.ctx.Done():
			return
		case <-ticker.C:
			if lp.tryBecomeLeader() {
				lp.mu.Lock()
				if !lp.isLeader {
					lp.isLeader = true
					lp.leaderCtx, lp.leaderCancel = context.WithCancel(lp.ctx)
				}
				lp.mu.Unlock()

				// Start heartbeat to maintain leadership
				lp.wg.Add(1)
				go lp.maintainLeadership()
				return
			}
		}
	}
}

// tryBecomeLeader attempts to become the leader.
func (lp *MySQLLeaderParticipation) tryBecomeLeader() bool {
	expiresAt := time.Now().Add(time.Duration(electionTTL) * time.Second)

	// Clean up expired elections first (best effort)
	_, _ = lp.server.db.ExecContext(lp.ctx, "DELETE FROM topo_elections WHERE expires_at < NOW()")

	// Try to insert our election record using INSERT IGNORE
	result, err := lp.server.db.ExecContext(lp.ctx,
		"INSERT IGNORE INTO topo_elections (name, leader_id, contents, expires_at) VALUES (?, ?, ?, ?)",
		lp.name, lp.id, lp.contents, expiresAt)

	if err != nil {
		log.Infof("Failed to insert election record for %s (id: %s): %v", lp.name, lp.id, err)
		return false
	}

	// Check if the insert was successful by examining affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Infof("Failed to get affected rows for %s (id: %s): %v", lp.name, lp.id, err)
		return false
	}

	if rowsAffected > 0 {
		// Successfully became leader
		log.Infof("Became leader for %s (id: %s)", lp.name, lp.id)
		return true
	}

	// Election record already exists (rowsAffected == 0)
	// Try to update if we're already the leader
	result, err = lp.server.db.ExecContext(lp.ctx,
		"UPDATE topo_elections SET expires_at = ? WHERE name = ? AND leader_id = ?",
		expiresAt, lp.name, lp.id)

	if err == nil {
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			log.Infof("Renewed leadership for %s (id: %s)", lp.name, lp.id)
			return true // We renewed our leadership
		}
	}
	log.Infof("Failed to renew leadership for %s (id: %s): %v", lp.name, lp.id, err)

	return false
}

// maintainLeadership maintains leadership through heartbeats.
func (lp *MySQLLeaderParticipation) maintainLeadership() {
	defer lp.wg.Done()

	ticker := time.NewTicker(time.Duration(electionTTL/3) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-lp.ctx.Done():
			lp.loseLeadership()
			return
		case <-ticker.C:
			if !lp.renewLeadership() {
				lp.loseLeadership()
				return
			}
		}
	}
}

// renewLeadership renews the leadership lease.
func (lp *MySQLLeaderParticipation) renewLeadership() bool {
	expiresAt := time.Now().Add(time.Duration(electionTTL) * time.Second)

	result, err := lp.server.db.ExecContext(lp.ctx,
		"UPDATE topo_elections SET expires_at = ? WHERE name = ? AND leader_id = ?",
		expiresAt, lp.name, lp.id)

	if err != nil {
		log.Infof("Failed to obtain leadership for %s: %v", lp.name, err)
		return false
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warningf("Could not determine leadership state for %s: %v", lp.name, err)
		return false
	}
	if rowsAffected == 0 {
		log.Warningf("Lost leadership for %s", lp.name)
		return false
	}
	return true
}

// loseLeadership handles loss of leadership.
func (lp *MySQLLeaderParticipation) loseLeadership() {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	if lp.isLeader {
		lp.isLeader = false
		if lp.leaderCancel != nil {
			lp.leaderCancel()
			lp.leaderCancel = nil
		}

		// Proactively delete our election record instead of waiting for TTL expiry
		_, _ = lp.server.db.ExecContext(context.Background(),
			"DELETE FROM topo_elections WHERE name = ? AND leader_id = ?",
			lp.name, lp.id)

		log.Infof("Lost leadership for %s", lp.name)
	}
}
