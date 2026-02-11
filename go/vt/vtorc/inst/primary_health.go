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

package inst

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtorcdata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtorc/db"
)

const minPrimaryHealthCheckFailures = 2

type primaryHealthEvent struct {
	at      time.Time
	success bool
}

type primaryHealthState struct {
	events                 []primaryHealthEvent
	unhealthy              bool
	lastPersistedAt        time.Time
	lastPersistedUnhealthy bool
	lastPersistedCount     int
}

var (
	primaryHealthMu      sync.Mutex
	primaryHealthByAlias = make(map[string]*primaryHealthState)
	primaryHealthLoadMu  sync.Mutex
	primaryHealthLoads   = make(map[string]struct{})
)

const primaryHealthPersistInterval = 5 * time.Second

// RecordPrimaryHealthCheck records the outcome of a primary health check.
func RecordPrimaryHealthCheck(tabletAlias string, success bool) {
	recordPrimaryHealthCheckAt(tabletAlias, success, time.Now())
}

// IsPrimaryHealthCheckUnhealthy reports whether the primary has an unhealthy healthcheck window.
// If the alias is missing from memory, this triggers a best-effort load from persisted
// state and returns false so callers do not block on storage reads.
func IsPrimaryHealthCheckUnhealthy(tabletAlias string) bool {
	window := primaryHealthWindow()
	if window <= 0 || tabletAlias == "" {
		return false
	}

	state := func() *primaryHealthState {
		primaryHealthMu.Lock()
		defer primaryHealthMu.Unlock()
		return primaryHealthByAlias[tabletAlias]
	}()

	if state == nil {
		return loadPrimaryHealthState(tabletAlias)
	}

	if state == nil {
		return false
	}

	evict, unhealthy := func() (bool, bool) {
		primaryHealthMu.Lock()
		defer primaryHealthMu.Unlock()
		state = primaryHealthByAlias[tabletAlias]
		if state == nil {
			return false, false
		}
		updatePrimaryHealthWindowLocked(state, time.Now(), window)
		evict := shouldEvictPrimaryHealthWindow(state)
		if evict {
			delete(primaryHealthByAlias, tabletAlias)
		}
		return evict, state.unhealthy
	}()
	if state == nil {
		return false
	}

	if evict {
		if err := deletePrimaryHealthState(tabletAlias); err != nil {
			log.Warningf("failed to delete primary health state for %s: %v", tabletAlias, err)
		}
		return false
	}
	return unhealthy
}

// recordPrimaryHealthCheckAt records a healthcheck outcome at a specific time. It updates the
// in-memory window and persists or evicts state outside the mutex to avoid blocking readers.
func recordPrimaryHealthCheckAt(tabletAlias string, success bool, now time.Time) {
	window := primaryHealthWindow()
	if window <= 0 || tabletAlias == "" {
		return
	}

	stateCopy, evict := func() (*primaryHealthState, bool) {
		primaryHealthMu.Lock()
		defer primaryHealthMu.Unlock()
		state := primaryHealthByAlias[tabletAlias]
		if state == nil {
			state = &primaryHealthState{}
			primaryHealthByAlias[tabletAlias] = state
		}
		state.events = append(state.events, primaryHealthEvent{at: now, success: success})
		updatePrimaryHealthWindowLocked(state, now, window)
		evict := shouldEvictPrimaryHealthWindow(state)
		if evict {
			delete(primaryHealthByAlias, tabletAlias)
		}
		return clonePrimaryHealthStateLocked(state), evict
	}()

	if evict {
		if err := deletePrimaryHealthState(tabletAlias); err != nil {
			log.Warningf("failed to delete primary health state for %s: %v", tabletAlias, err)
		}
		return
	}
	if err := maybeWritePrimaryHealthState(tabletAlias, stateCopy); err != nil {
		log.Warningf("failed to write primary health state for %s: %v", tabletAlias, err)
	}
}

// updatePrimaryHealthWindowLocked updates the current state of the primary's health. A primary is marked as
// unhealthy when:
//
//   - At least minPrimaryHealthCheckFailures (2) failures exist in the window.
//   - Failures outnumber successes.
//
// A primary is marked as healthy when there are no failures in the window and there is at least one success.
// Requiring no failures in the window helps prevent flappiness and requires the primary to show it is
// consistently successful to be considered healthy.
func updatePrimaryHealthWindowLocked(state *primaryHealthState, now time.Time, window time.Duration) {
	if state == nil {
		return
	}

	successCount := 0
	failureCount := 0
	pruned := state.events[:0]
	for _, event := range state.events {
		if now.Sub(event.at) > window {
			continue
		}
		pruned = append(pruned, event)
		if event.success {
			successCount++
		} else {
			failureCount++
		}
	}
	state.events = pruned
	if len(state.events) == 0 {
		state.events = nil
		state.unhealthy = false
	}

	if state.unhealthy {
		if failureCount == 0 && successCount > 0 {
			state.unhealthy = false
		}
		return
	}

	if failureCount >= minPrimaryHealthCheckFailures && failureCount > successCount {
		state.unhealthy = true
	}
}

// shouldEvictPrimaryHealthWindow reports whether a health window can be removed entirely.
// We evict only when the primary is healthy and the window has no remaining events.
func shouldEvictPrimaryHealthWindow(state *primaryHealthState) bool {
	return state != nil && !state.unhealthy && len(state.events) == 0
}

// clonePrimaryHealthStateLocked creates a deep copy of the in-memory health state.
// It must be called with primaryHealthMu held so callers can safely use the copy
// after releasing the lock without racing on the underlying slice.
func clonePrimaryHealthStateLocked(state *primaryHealthState) *primaryHealthState {
	if state == nil {
		return nil
	}
	cloned := &primaryHealthState{
		unhealthy:              state.unhealthy,
		lastPersistedAt:        state.lastPersistedAt,
		lastPersistedUnhealthy: state.lastPersistedUnhealthy,
		lastPersistedCount:     state.lastPersistedCount,
	}
	if len(state.events) > 0 {
		cloned.events = make([]primaryHealthEvent, len(state.events))
		copy(cloned.events, state.events)
	}
	return cloned
}

// loadPrimaryHealthState schedules a background load of the persisted state and
// returns false immediately to avoid blocking on storage during analysis queries.
// It coalesces concurrent loads so only one read happens per alias at a time.
func loadPrimaryHealthState(tabletAlias string) bool {
	if tabletAlias == "" {
		return false
	}
	shouldLoad := func() bool {
		primaryHealthLoadMu.Lock()
		defer primaryHealthLoadMu.Unlock()
		if _, ok := primaryHealthLoads[tabletAlias]; ok {
			return false
		}
		primaryHealthLoads[tabletAlias] = struct{}{}
		return true
	}()
	if !shouldLoad {
		return false
	}

	go func() {
		defer func() {
			primaryHealthLoadMu.Lock()
			defer primaryHealthLoadMu.Unlock()
			delete(primaryHealthLoads, tabletAlias)
		}()

		persisted, err := readPrimaryHealthState(tabletAlias)
		if err != nil {
			log.Warningf("failed to read primary health state for %s: %v", tabletAlias, err)
			return
		}
		if persisted == nil {
			return
		}
		primaryHealthMu.Lock()
		if primaryHealthByAlias[tabletAlias] == nil {
			primaryHealthByAlias[tabletAlias] = persisted
		}
		primaryHealthMu.Unlock()
	}()

	return false
}

// primaryHealthWindow returns the time window used for primary health analysis.
// It is derived from the topo remote operation timeout to track transient flaps
// without immediately triggering recovery.
func primaryHealthWindow() time.Duration {
	return topo.RemoteOperationTimeout * 4
}

// maybeWritePrimaryHealthState persists the health window only when it changes
// or when enough time has elapsed since the last write.
func maybeWritePrimaryHealthState(tabletAlias string, state *primaryHealthState) error {
	if tabletAlias == "" || state == nil {
		return nil
	}
	if shouldEvictPrimaryHealthWindow(state) {
		return deletePrimaryHealthState(tabletAlias)
	}
	now := time.Now()
	if now.Sub(state.lastPersistedAt) < primaryHealthPersistInterval &&
		state.unhealthy == state.lastPersistedUnhealthy &&
		len(state.events) == state.lastPersistedCount {
		return nil
	}
	if err := writePrimaryHealthState(tabletAlias, state); err != nil {
		return err
	}
	primaryHealthMu.Lock()
	if current := primaryHealthByAlias[tabletAlias]; current != nil {
		current.lastPersistedAt = now
		current.lastPersistedUnhealthy = current.unhealthy
		current.lastPersistedCount = len(current.events)
	}
	primaryHealthMu.Unlock()
	return nil
}

// readPrimaryHealthState loads the persisted health window for a tablet alias.
// It returns nil when no row exists, and wraps any unmarshaling errors with
// context to help identify corrupt or incompatible state.
func readPrimaryHealthState(tabletAlias string) (*primaryHealthState, error) {
	query := "select health_state from primary_health where alias = ?"
	var state *primaryHealthState
	err := db.QueryVTOrc(query, sqlutils.Args(tabletAlias), func(row sqlutils.RowMap) error {
		value := row.GetString("health_state")
		if value == "" {
			return nil
		}
		pb := &vtorcdata.PrimaryHealthState{}
		if err := prototext.Unmarshal([]byte(value), pb); err != nil {
			return fmt.Errorf("unmarshal primary health state: %w", err)
		}
		state = fromProtoPrimaryHealthState(pb)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

// writePrimaryHealthState persists the current health window for a tablet alias.
// It is a no-op for empty aliases or nil state, and it deletes the row if the
// state is already evictable.
func writePrimaryHealthState(tabletAlias string, state *primaryHealthState) error {
	if tabletAlias == "" || state == nil {
		return nil
	}
	if shouldEvictPrimaryHealthWindow(state) {
		return deletePrimaryHealthState(tabletAlias)
	}
	pb := toProtoPrimaryHealthState(state)
	data, err := prototext.Marshal(pb)
	if err != nil {
		return fmt.Errorf("marshal primary health state: %w", err)
	}
	query := `REPLACE INTO primary_health (alias, health_state, last_updated) VALUES (?, ?, DATETIME('now'))`
	_, err = db.ExecVTOrc(query, tabletAlias, string(data))
	return err
}

// deletePrimaryHealthState removes the persisted health window for a tablet alias.
// It is safe to call repeatedly or with an empty alias.
func deletePrimaryHealthState(tabletAlias string) error {
	if tabletAlias == "" {
		return nil
	}
	_, err := db.ExecVTOrc("delete from primary_health where alias = ?", tabletAlias)
	return err
}

// toProtoPrimaryHealthState converts the in-memory health window to its protobuf form.
// Timestamps are stored as Unix nanoseconds so ordering remains stable across reloads.
func toProtoPrimaryHealthState(state *primaryHealthState) *vtorcdata.PrimaryHealthState {
	if state == nil {
		return &vtorcdata.PrimaryHealthState{}
	}
	events := make([]*vtorcdata.PrimaryHealthEvent, 0, len(state.events))
	for _, event := range state.events {
		events = append(events, &vtorcdata.PrimaryHealthEvent{
			AtUnixNanos: event.at.UnixNano(),
			Success:     event.success,
		})
	}
	return &vtorcdata.PrimaryHealthState{
		Events:    events,
		Unhealthy: state.unhealthy,
	}
}

// fromProtoPrimaryHealthState converts the persisted protobuf health window into the
// in-memory representation used by the primary health checks.
func fromProtoPrimaryHealthState(pb *vtorcdata.PrimaryHealthState) *primaryHealthState {
	if pb == nil {
		return nil
	}
	events := make([]primaryHealthEvent, 0, len(pb.Events))
	for _, event := range pb.Events {
		if event == nil {
			continue
		}
		events = append(events, primaryHealthEvent{
			at:      time.Unix(0, event.AtUnixNanos),
			success: event.Success,
		})
	}
	return &primaryHealthState{
		events:    events,
		unhealthy: pb.Unhealthy,
	}
}
