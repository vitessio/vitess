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
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// MySQLLeaderParticipation implements topo.LeaderParticipation.
type MySQLLeaderParticipation struct {
	s       *Server
	name    string
	id      string
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	stopped bool
	// waiters are waiting for a new leader
	waiters []chan string
}

// NewLeaderParticipation is part of the topo.Conn interface.
func (s *Server) NewLeaderParticipation(name, id string) (topo.LeaderParticipation, error) {
	return &MySQLLeaderParticipation{
		s:       s,
		name:    name,
		id:      id,
		done:    make(chan struct{}),
		waiters: make([]chan string, 0),
	}, nil
}

// WaitForLeadership is part of the topo.LeaderParticipation interface.
func (mp *MySQLLeaderParticipation) WaitForLeadership() (context.Context, error) {
	mp.mu.Lock()
	if mp.stopped {
		mp.mu.Unlock()
		return nil, topo.NewError(topo.Interrupted, "leadership election stopped")
	}
	if mp.cancel != nil {
		mp.mu.Unlock()
		return nil, topo.NewError(topo.Interrupted, "leadership election already running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	mp.cancel = cancel
	mp.mu.Unlock()

	// Try to acquire leadership
	go func() {
		defer close(mp.done)

		// Keep trying until we acquire leadership or are canceled
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Try to acquire leadership
			acquired, err := mp.tryAcquireLeadership()
			if err != nil {
				log.Warningf("Error in leadership election for %s: %v", mp.name, err)
				time.Sleep(1 * time.Second)
				continue
			}

			if acquired {
				// We are the leader, notify waiters
				mp.notifyWaiters(mp.id)

				// Keep refreshing leadership until canceled
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(3 * time.Second):
						// Refresh our leadership
						err := mp.refreshLeadership()
						if err != nil {
							log.Warningf("Error refreshing leadership for %s: %v", mp.name, err)
							// Don't exit, just try again
						}
					}
				}
			}

			// Wait before trying again
			time.Sleep(1 * time.Second)
		}
	}()

	return ctx, nil
}

// tryAcquireLeadership attempts to acquire leadership
func (mp *MySQLLeaderParticipation) tryAcquireLeadership() (bool, error) {
	expiration := time.Now().UTC().Add(30 * time.Second).Format("2006-01-02 15:04:05")

	// Try to insert a new election record (if no election exists)
	result, err := mp.s.exec(`
		INSERT IGNORE INTO topo_elections (name, leader_id, contents, expiration)
		VALUES (%s, %s, %s, %s)
	`, mp.name, mp.id, "", expiration)
	if err != nil {
		return false, err
	}

	if result.RowsAffected > 0 {
		// We became the leader
		return true, nil
	}

	// Check if we are already the leader
	currentLeader, err := mp.GetCurrentLeaderID(context.Background())
	if err != nil {
		return false, err
	}

	if currentLeader == mp.id {
		// We are already the leader
		return true, nil
	}

	// Election record exists with a different leader, check if we can take over from an expired leader
	updateResult, err := mp.s.exec(`
		UPDATE topo_elections 
		SET leader_id = %s, contents = %s, expiration = %s 
		WHERE name = %s AND expiration < NOW()
	`, mp.id, "", expiration, mp.name)
	if err != nil {
		return false, err
	}

	return updateResult.RowsAffected > 0, nil
}

// refreshLeadership refreshes our leadership expiration
func (mp *MySQLLeaderParticipation) refreshLeadership() error {
	refreshExpiration := time.Now().UTC().Add(30 * time.Second).Format("2006-01-02 15:04:05")
	_, err := mp.s.exec("UPDATE topo_elections SET expiration = %s WHERE name = %s AND leader_id = %s",
		refreshExpiration, mp.name, mp.id)
	return err
}

// Stop is part of the topo.LeaderParticipation interface.
func (mp *MySQLLeaderParticipation) Stop() {
	mp.mu.Lock()
	mp.stopped = true
	if mp.cancel != nil {
		mp.cancel()
		mp.cancel = nil
	}
	mp.mu.Unlock()

	// Wait for the leadership goroutine to exit
	<-mp.done

	// If we're the leader, release leadership
	currentLeader, err := mp.GetCurrentLeaderID(context.Background())
	if err == nil && currentLeader == mp.id {
		_, err := mp.s.exec("DELETE FROM topo_elections WHERE name = %s AND leader_id = %s", mp.name, mp.id)
		if err != nil {
			log.Warningf("Error releasing leadership: %v", err)
		}
	}
}

// GetCurrentLeaderID is part of the topo.LeaderParticipation interface.
func (mp *MySQLLeaderParticipation) GetCurrentLeaderID(ctx context.Context) (string, error) {
	result, err := mp.s.queryRow("SELECT leader_id, expiration FROM topo_elections WHERE name = %s", mp.name)
	if err != nil {
		return "", err
	}

	if len(result.Rows) == 0 {
		// No leader elected yet
		return "", nil
	}

	leaderID := result.Rows[0][0].ToString()
	expirationStr := result.Rows[0][1].ToString()

	// Parse the expiration time
	expiration, err := time.Parse("2006-01-02 15:04:05", expirationStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse expiration time: %v", err)
	}

	// Check if the leadership has expired
	if expiration.Before(time.Now().UTC()) {
		// Leadership has expired, delete it
		_, err := mp.s.exec("DELETE FROM topo_elections WHERE name = %s AND leader_id = %s", mp.name, leaderID)
		if err != nil {
			log.Warningf("Error deleting expired leadership: %v", err)
		}
		return "", nil
	}

	return leaderID, nil
}

// WaitForNewLeader is part of the topo.LeaderParticipation interface.
func (mp *MySQLLeaderParticipation) WaitForNewLeader(ctx context.Context) (<-chan string, error) {
	// Create a channel to receive leader updates
	leaderChan := make(chan string, 5)

	// Get the current leader
	currentLeader, err := mp.GetCurrentLeaderID(ctx)
	if err != nil {
		return nil, err
	}

	// Send the current leader if there is one
	if currentLeader != "" {
		leaderChan <- currentLeader
	}

	// Register this channel to receive updates
	mp.mu.Lock()
	mp.waiters = append(mp.waiters, leaderChan)
	mp.mu.Unlock()

	// Start a goroutine to poll for changes
	go func() {
		defer func() {
			// Unregister the channel when done
			mp.mu.Lock()
			for i, ch := range mp.waiters {
				if ch == leaderChan {
					mp.waiters = append(mp.waiters[:i], mp.waiters[i+1:]...)
					break
				}
			}
			mp.mu.Unlock()
			close(leaderChan)
		}()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		lastLeader := currentLeader

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				newLeader, err := mp.GetCurrentLeaderID(ctx)
				if err != nil {
					log.Warningf("Error checking leadership: %v", err)
					continue
				}

				if newLeader != lastLeader {
					// Leader changed
					leaderChan <- newLeader
					lastLeader = newLeader
				}
			}
		}
	}()

	return leaderChan, nil
}

// notifyWaiters notifies all waiters of a new leader
func (mp *MySQLLeaderParticipation) notifyWaiters(leaderID string) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	for _, ch := range mp.waiters {
		select {
		case ch <- leaderID:
		default:
			// Channel is full, skip
		}
	}
}
