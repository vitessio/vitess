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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

func TestLeadershipTransfer(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	electionName := "test-leadership-transfer"

	// First participant becomes leader
	lp1, err := server.NewLeaderParticipation(electionName, "participant-1")
	require.NoError(t, err)
	defer lp1.Stop()

	leaderCtx1, err := lp1.WaitForLeadership()
	require.NoError(t, err)
	require.NotNil(t, leaderCtx1, "Expected non-nil leadership context")

	// Verify first participant is leader
	leaderID, err := lp1.GetCurrentLeaderID(t.Context())
	require.NoError(t, err)
	require.Equal(t, "participant-1", leaderID, "Expected participant-1 to be the leader")

	// Second participant waits for leadership
	lp2, err := server.NewLeaderParticipation(electionName, "participant-2")
	require.NoError(t, err)
	defer lp2.Stop()

	// Second participant should fail to get leadership initially (first participant is leader)
	leaderCtx2, waitErr := lp2.WaitForLeadership()
	require.Error(t, waitErr, "Expected error when trying to get leadership while someone else is leader")
	require.True(t, topo.IsErrType(waitErr, topo.NoNode), "Expected NoNode error, got: %v", waitErr)
	require.Nil(t, leaderCtx2, "Expected nil context when leadership acquisition fails")

	// First participant should still be leader
	leaderID, err = lp1.GetCurrentLeaderID(t.Context())
	require.NoError(t, err)
	require.Equal(t, "participant-1", leaderID)

	// Stop first participant (leadership transfer)
	lp1.Stop()

	// Wait a moment for the leadership to be released
	time.Sleep(200 * time.Millisecond)

	// Now second participant should be able to get leadership
	leaderCtx2, waitErr = lp2.WaitForLeadership()
	require.NoError(t, waitErr, "Expected second participant to get leadership after first stopped")
	require.NotNil(t, leaderCtx2, "Expected non-nil leadership context")

	// Verify second participant is now leader
	leaderID, err = lp2.GetCurrentLeaderID(t.Context())
	require.NoError(t, err)
	require.Equal(t, "participant-2", leaderID)

	// First participant's context should be cancelled
	select {
	case <-leaderCtx1.Done():
		// Good, context was cancelled
	case <-time.After(1 * time.Second):
		t.Error("context was expected to be cancelled")
	}
}

func TestWaitForNewLeader(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	electionName := "test-wait-for-new-leader"

	// Create observer participant
	observer, err := server.NewLeaderParticipation(electionName, "observer")
	require.NoError(t, err, "NewLeaderParticipation() error for observer")
	defer observer.Stop()

	// Start watching for leaders
	leaderCh, err := observer.WaitForNewLeader(t.Context())
	require.NoError(t, err, "WaitForNewLeader() error")

	// Initially there should be no leader
	select {
	case leaderID := <-leaderCh:
		t.Errorf("Expected no initial leader, got %q", leaderID)
	case <-time.After(200 * time.Millisecond):
		// Good, no leader initially
	}

	// Create first leader
	lp1, err := server.NewLeaderParticipation(electionName, "leader-1")
	require.NoError(t, err, "NewLeaderParticipation() error for leader-1")
	defer lp1.Stop()

	_, err = lp1.WaitForLeadership()
	require.NoError(t, err, "WaitForLeadership() error")

	// Should detect first leader
	select {
	case leaderID := <-leaderCh:
		require.Equal(t, "leader-1", leaderID, "Expected leader-1, got %q", leaderID)
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout waiting for first leader notification")
	}

	// Create second leader (will wait until first stops)
	lp2, err := server.NewLeaderParticipation(electionName, "leader-2")
	require.NoError(t, err, "NewLeaderParticipation() error for leader-2")
	defer lp2.Stop()

	// Start waiting for leadership in background
	go func() {
		_, _ = lp2.WaitForLeadership()
	}()

	// Stop first leader
	lp1.Stop()

	// Should detect second leader
	select {
	case leaderID := <-leaderCh:
		require.Equal(t, "leader-2", leaderID, "Expected leader-2, got %q", leaderID)
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout waiting for second leader notification")
	}
}

func TestLeaderParticipationStop(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	electionName := "test-stop"

	lp, err := server.NewLeaderParticipation(electionName, "participant-1")
	require.NoError(t, err, "NewLeaderParticipation() error")

	// Become leader
	leaderCtx, err := lp.WaitForLeadership()
	require.NoError(t, err, "WaitForLeadership() error")
	require.NotNil(t, leaderCtx, "WaitForLeadership() returned nil context")

	// Stop should cancel leadership context
	lp.Stop()

	select {
	case <-leaderCtx.Done():
		// Good, context was cancelled
	case <-time.After(1 * time.Second):
		t.Errorf("Leadership context was not cancelled after Stop()")
	}

	// Multiple calls to Stop should be safe
	lp.Stop()
	lp.Stop()

	// After stop, there should be no leader
	leaderID, err := lp.GetCurrentLeaderID(t.Context())
	require.NoError(t, err, "GetCurrentLeaderID() error")
	require.Empty(t, leaderID, "After Stop(), GetCurrentLeaderID() = %q, want empty string", leaderID)
}

func TestConcurrentElections(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	// Test multiple elections running concurrently
	numElections := 3
	numParticipantsPerElection := 2

	var allParticipants []topo.LeaderParticipation

	// Create multiple elections with multiple participants each
	for electionIdx := 0; electionIdx < numElections; electionIdx++ {
		electionName := fmt.Sprintf("concurrent-election-%d", electionIdx)

		for participantIdx := 0; participantIdx < numParticipantsPerElection; participantIdx++ {
			participantID := fmt.Sprintf("election-%d-participant-%d", electionIdx, participantIdx)
			lp, err := server.NewLeaderParticipation(electionName, participantID)
			require.NoError(t, err)
			defer lp.Stop()
			allParticipants = append(allParticipants, lp)
		}
	}

	// All participants try to become leader concurrently
	var wg sync.WaitGroup
	results := make([]struct {
		electionIdx    int
		participantIdx int
		leaderCtx      context.Context
		err            error
	}, len(allParticipants))

	for i, lp := range allParticipants {
		wg.Add(1)
		go func(idx int, participant topo.LeaderParticipation) {
			defer wg.Done()
			electionIdx := idx / numParticipantsPerElection
			participantIdx := idx % numParticipantsPerElection

			ctx, err := participant.WaitForLeadership()
			results[idx] = struct {
				electionIdx    int
				participantIdx int
				leaderCtx      context.Context
				err            error
			}{electionIdx, participantIdx, ctx, err}
		}(i, lp)
	}

	wg.Wait()

	// Verify each election has exactly one leader
	leadersByElection := make(map[int]int) // election -> number of leaders
	for _, result := range results {
		if result.err == nil && result.leaderCtx != nil {
			leadersByElection[result.electionIdx]++
		}
	}

	for electionIdx := 0; electionIdx < numElections; electionIdx++ {
		leaderCount := leadersByElection[electionIdx]
		require.Equal(t, 1, leaderCount, "Election %d has %d leaders, want exactly 1", electionIdx, leaderCount)
	}
}

func TestElectionTimeout(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	lp, err := server.NewLeaderParticipation("test-timeout", "participant-1")
	require.NoError(t, err)
	defer lp.Stop()

	// Stop the participation immediately to test timeout behavior
	lp.Stop()

	// WaitForLeadership should fail with interrupted error
	_, err = lp.WaitForLeadership()
	require.Error(t, err, "Expected error when waiting for leadership after Stop()")

	// Should be an interrupted error
	require.True(t, topo.IsErrType(err, topo.Interrupted), "Expected interrupted error, got: %v", err)
}

func TestLeadershipRenewal(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	// Save original TTL and restore it after the test
	originalTTL := electionTTL
	defer func() {
		electionTTL = originalTTL
	}()

	// Use a short TTL for faster test execution
	electionTTL = 2

	electionName := "test-leadership-renewal"
	participantID := "renewal-participant"

	lp, err := server.NewLeaderParticipation(electionName, participantID)
	require.NoError(t, err)
	defer lp.Stop()

	// Become leader
	leaderCtx, err := lp.WaitForLeadership()
	require.NoError(t, err)
	require.NotNil(t, leaderCtx)

	// Verify we're the leader
	leaderID, err := lp.GetCurrentLeaderID(context.Background())
	require.NoError(t, err)
	require.Equal(t, participantID, leaderID)

	// Wait longer than the TTL to ensure renewal happens
	// The renewal should happen every TTL/3 seconds (approximately every 0.67 seconds)
	// We wait 3 times the TTL to ensure multiple renewals occur
	time.Sleep(time.Duration(electionTTL*3) * time.Second)

	// Verify we're still the leader (renewal worked)
	leaderID, err = lp.GetCurrentLeaderID(context.Background())
	require.NoError(t, err)
	require.Equal(t, participantID, leaderID, "Leadership should be maintained through renewal")

	// Verify leadership context is still active
	select {
	case <-leaderCtx.Done():
		t.Error("Leadership context should not be cancelled if renewal is working")
	default:
		// Good, context is still active
	}

	// Test that renewal fails when we're no longer the leader in the database
	// Simulate another process taking over leadership by directly updating the database
	_, err = server.db.ExecContext(context.Background(),
		"UPDATE topo_elections SET leader_id = ? WHERE name = ?",
		"other-leader", electionName)
	require.NoError(t, err)

	// Wait for the next renewal attempt (should fail and lose leadership)
	// The renewal happens every TTL/3, so wait a bit longer than that
	time.Sleep(time.Duration(electionTTL/3+1) * time.Second)

	// Leadership context should now be cancelled due to failed renewal
	select {
	case <-leaderCtx.Done():
		// Good, context was cancelled due to failed renewal
	case <-time.After(2 * time.Second):
		t.Error("Leadership context should be cancelled when renewal fails")
	}

	// Verify we're no longer the leader (the original participant should have lost leadership)
	leaderID, err = lp.GetCurrentLeaderID(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, participantID, leaderID, "Should no longer be the leader after renewal failure")
}

func TestRenewLeadershipDirectly(t *testing.T) {
	server, _, cleanup := createTestServer(t, "")
	defer cleanup()

	// Save original TTL and restore it after the test
	originalTTL := electionTTL
	defer func() {
		electionTTL = originalTTL
	}()

	// Use a longer TTL to avoid automatic renewal interfering with our test
	electionTTL = 30

	electionName := "test-renew-leadership-direct"
	participantID := "direct-renewal-participant"

	lp, err := server.NewLeaderParticipation(electionName, participantID)
	require.NoError(t, err)
	defer lp.Stop()

	// Cast to MySQLLeaderParticipation to access renewLeadership directly
	mysqlLP, ok := lp.(*MySQLLeaderParticipation)
	require.True(t, ok, "Expected MySQLLeaderParticipation type")

	// Become leader first
	leaderCtx, err := lp.WaitForLeadership()
	require.NoError(t, err)
	require.NotNil(t, leaderCtx)

	// If we call renewLeadership() within the same second as it was established,
	// then the update to extend the expiration will return 0 rows affected.
	// This is because the datatype is a timestamp (second granularity).
	// Unfortunately this will make it look like we _lost_ leadership,
	// which is not true.
	time.Sleep(time.Second)

	// Check what's in the database before renewal
	var dbLeaderID string
	var expiresAtStr string
	err = server.db.QueryRowContext(t.Context(),
		"SELECT leader_id, expires_at FROM topo_elections WHERE name = ?",
		electionName).Scan(&dbLeaderID, &expiresAtStr)
	require.NoError(t, err, "Should be able to find election record")
	require.Equal(t, participantID, dbLeaderID, "Database should show us as leader")
	t.Logf("Before renewal: leader_id=%s, expires_at=%s, name=%s", dbLeaderID, expiresAtStr, electionName)

	// Test successful renewal
	success := mysqlLP.renewLeadership()
	require.True(t, success, "renewLeadership should succeed when we are the leader")

	// Check what's in the database after renewal
	var newExpiresAtStr string
	err = server.db.QueryRowContext(t.Context(),
		"SELECT leader_id, expires_at FROM topo_elections WHERE name = ?",
		electionName).Scan(&dbLeaderID, &newExpiresAtStr)
	require.NoError(t, err, "Should be able to find election record after renewal")
	require.Equal(t, participantID, dbLeaderID, "Database should still show us as leader")
	require.NotEqual(t, expiresAtStr, newExpiresAtStr, "Expiration time should be updated")
	t.Logf("After renewal: leader_id=%s, expires_at=%s", dbLeaderID, newExpiresAtStr)

	// Verify we're still the leader
	leaderID, err := lp.GetCurrentLeaderID(t.Context())
	require.NoError(t, err)
	require.Equal(t, participantID, leaderID)

	// Test renewal failure when someone else is leader
	// Update the database to make someone else the leader
	_, err = server.db.ExecContext(t.Context(),
		"UPDATE topo_elections SET leader_id = ? WHERE name = ?",
		"other-leader", electionName)
	require.NoError(t, err)

	// Now renewal should fail
	success = mysqlLP.renewLeadership()
	require.False(t, success, "renewLeadership should fail when we are not the leader")

	// Test renewal failure when election record doesn't exist
	// Delete the election record
	_, err = server.db.ExecContext(t.Context(),
		"DELETE FROM topo_elections WHERE name = ?", electionName)
	require.NoError(t, err)

	// Now renewal should fail
	success = mysqlLP.renewLeadership()
	require.False(t, success, "renewLeadership should fail when election record doesn't exist")
}
