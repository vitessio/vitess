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
	"vitess.io/vitess/go/vt/vtorc/test"
)

const minPrimaryHealthCheckFailures = 2

type primaryHealthEvent struct {
	at      time.Time
	success bool
}

type primaryHealthState struct {
	events    []primaryHealthEvent
	unhealthy bool
}

var (
	primaryHealthMu      sync.Mutex
	primaryHealthByAlias = make(map[string]*primaryHealthState)
)

// RecordPrimaryHealthCheck records the outcome of a primary health check.
func RecordPrimaryHealthCheck(tabletAlias string, success bool) {
	recordPrimaryHealthCheckAt(tabletAlias, success, time.Now())
}

// IsPrimaryHealthCheckUnhealthy reports whether the primary health checks are unhealthy.
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
		if _, ok := db.Db.(*test.DB); ok {
			return false
		}
		go func() {
			persisted, err := readPrimaryHealthState(tabletAlias)
			if err != nil {
				log.Warningf("failed to read primary health state for %s: %v", tabletAlias, err)
				return
			}
			if persisted == nil {
				return
			}
			_ = func() *primaryHealthState {
				primaryHealthMu.Lock()
				defer primaryHealthMu.Unlock()
				if primaryHealthByAlias[tabletAlias] == nil {
					primaryHealthByAlias[tabletAlias] = persisted
				}
				return primaryHealthByAlias[tabletAlias]
			}()
		}()
		return false
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
		if _, ok := db.Db.(*test.DB); !ok {
			go func() {
				if err := deletePrimaryHealthState(tabletAlias); err != nil {
					log.Warningf("failed to delete primary health state for %s: %v", tabletAlias, err)
				}
			}()
		}
		return false
	}
	return unhealthy
}

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
	if err := writePrimaryHealthState(tabletAlias, stateCopy); err != nil {
		log.Warningf("failed to write primary health state for %s: %v", tabletAlias, err)
	}
}

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

func shouldEvictPrimaryHealthWindow(state *primaryHealthState) bool {
	return state != nil && !state.unhealthy && len(state.events) == 0
}

func clonePrimaryHealthStateLocked(state *primaryHealthState) *primaryHealthState {
	if state == nil {
		return nil
	}
	cloned := &primaryHealthState{
		unhealthy: state.unhealthy,
	}
	if len(state.events) > 0 {
		cloned.events = make([]primaryHealthEvent, len(state.events))
		copy(cloned.events, state.events)
	}
	return cloned
}

func primaryHealthWindow() time.Duration {
	return topo.RemoteOperationTimeout * 4
}

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

func deletePrimaryHealthState(tabletAlias string) error {
	if tabletAlias == "" {
		return nil
	}
	_, err := db.ExecVTOrc("delete from primary_health where alias = ?", tabletAlias)
	return err
}

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
