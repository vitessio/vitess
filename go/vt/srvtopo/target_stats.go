/*
Copyright 2019 The Vitess Authors.

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

package srvtopo

import (
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TargetStatsEntry has the updated information for a Target.
type TargetStatsEntry struct {
	// Target is what this entry applies to.
	Target *querypb.Target

	// TabletExternallyReparentedTimestamp is the latest timestamp
	// that was reported for this entry. It applies to masters only.
	TabletExternallyReparentedTimestamp int64
}

// TargetStatsMultiplexer is a helper class to help broadcast stats updates.
// It doesn't have any synchronization, as the container class will already
// have some and this can just use it.
type TargetStatsMultiplexer struct {
	// listeners has the map of channels to send updates to.
	listeners map[int]chan (*TargetStatsEntry)

	// nextIndex has the next map id.
	nextIndex int
}

// NewTargetStatsMultiplexer returns an initialized TargetStatsMultiplexer.
func NewTargetStatsMultiplexer() TargetStatsMultiplexer {
	return TargetStatsMultiplexer{
		listeners: make(map[int]chan (*TargetStatsEntry)),
	}
}

// Subscribe adds a channel to the list.
// Will change the list.
func (tsm *TargetStatsMultiplexer) Subscribe() (int, <-chan (*TargetStatsEntry)) {
	i := tsm.nextIndex
	tsm.nextIndex++
	c := make(chan (*TargetStatsEntry), 100)
	tsm.listeners[i] = c
	return i, c
}

// Unsubscribe removes a channel from the list.
// Will change the list.
func (tsm *TargetStatsMultiplexer) Unsubscribe(i int) error {
	c, ok := tsm.listeners[i]
	if !ok {
		return fmt.Errorf("TargetStatsMultiplexer.Unsubscribe(%v): not suc channel", i)
	}
	delete(tsm.listeners, i)
	close(c)
	return nil
}

// HasSubscribers returns true if we have registered subscribers.
// Will read the list.
func (tsm *TargetStatsMultiplexer) HasSubscribers() bool {
	return len(tsm.listeners) > 0
}

// Broadcast sends an update to the list.
// Will read the list.
func (tsm *TargetStatsMultiplexer) Broadcast(tse *TargetStatsEntry) {
	for _, c := range tsm.listeners {
		c <- tse
	}
}
