/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttler

import (
	"sort"
	"time"

	"github.com/youtube/vitess/go/vt/discovery"
)

// replicationLagCache caches for each replica a bounded list of historic
// replicationlagRecord entries.
type replicationLagCache struct {
	// entries maps from the replica to its history.
	// The map key is replicationLagRecord.TabletStats.Key.
	entries map[string]*replicationLagHistory

	// slowReplicas is a set of slow replicas.
	// The map key is replicationLagRecord.TabletStats.Key.
	// This map will always be recomputed by sortByLag() and must not be modified
	// from other methods.
	slowReplicas map[string]bool

	// ignoredSlowReplicasInARow is a set of slow replicas for which the method
	// ignoreSlowReplica() has returned true.
	// It's used to detect the case where *all* replicas in a row have been
	// ignored. This happens when the lag on every replica increases and each
	// becomes the new slowest replica. This set is used to detect such a chain.
	// The set will be cleared if ignoreSlowReplica() returns false.
	//
	// The map key is replicationLagRecord.TabletStats.Key.
	// If an entry is deleted from "entries", it must be deleted here as well.
	ignoredSlowReplicasInARow map[string]bool

	historyCapacityPerReplica int
}

func newReplicationLagCache(historyCapacityPerReplica int) *replicationLagCache {
	return &replicationLagCache{
		entries:                   make(map[string]*replicationLagHistory),
		ignoredSlowReplicasInARow: make(map[string]bool),
		historyCapacityPerReplica: historyCapacityPerReplica,
	}
}

// add inserts or updates "r" in the cache for the replica with the key "r.Key".
func (c *replicationLagCache) add(r replicationLagRecord) {
	if !r.Up {
		// Tablet is down. Do no longer track it.
		delete(c.entries, r.Key)
		delete(c.ignoredSlowReplicasInARow, r.Key)
		return
	}

	entry, ok := c.entries[r.Key]
	if !ok {
		entry = newReplicationLagHistory(c.historyCapacityPerReplica)
		c.entries[r.Key] = entry
	}

	entry.add(r)
}

// latest returns the current lag record for the given TabletStats.Key string.
// A zero record is returned if there is no latest entry.
func (c *replicationLagCache) latest(key string) replicationLagRecord {
	entry, ok := c.entries[key]
	if !ok {
		return replicationLagRecord{}
	}
	return entry.latest()
}

// atOrAfter returns the oldest replicationLagRecord which happened at "at"
// or just after it.
// If there is no such record, a zero record is returned.
func (c *replicationLagCache) atOrAfter(key string, at time.Time) replicationLagRecord {
	entry, ok := c.entries[key]
	if !ok {
		return replicationLagRecord{}
	}
	return entry.atOrAfter(at)
}

// sortByLag sorts all replicas by their latest replication lag value and
// tablet uid and updates the c.slowReplicas set.
func (c *replicationLagCache) sortByLag(ignoreNSlowestReplicas int, minimumReplicationLag int64) {
	// Reset the current list of ignored replicas.
	c.slowReplicas = make(map[string]bool)

	if ignoreNSlowestReplicas >= len(c.entries) {
		// Do not ignore slow replicas if all would get ignored.
		return
	}

	// Turn the map of replicas into a list and then sort it.
	var list byLagAndTabletUID
	i := 0
	for _, v := range c.entries {
		record := v.latest()
		if int64(record.Stats.SecondsBehindMaster) >= minimumReplicationLag {
			list = append(list, record.TabletStats)
			i++
		}
	}
	sort.Sort(list)

	// Now remember the N slowest replicas.
	for i := len(list) - 1; len(list) > 0 && i >= len(list)-ignoreNSlowestReplicas; i-- {
		c.slowReplicas[list[i].Key] = true
	}
}

// byLagAndTabletUID is a slice of discovery.TabletStats elements that
// implements sort.Interface to sort by replication lag and tablet Uid.
type byLagAndTabletUID []discovery.TabletStats

func (a byLagAndTabletUID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byLagAndTabletUID) Len() int      { return len(a) }
func (a byLagAndTabletUID) Less(i, j int) bool {
	return a[i].Stats.SecondsBehindMaster < a[j].Stats.SecondsBehindMaster ||
		(a[i].Stats.SecondsBehindMaster == a[j].Stats.SecondsBehindMaster &&
			a[i].Tablet.Alias.Uid < a[j].Tablet.Alias.Uid)
}

// ignoreSlowReplica returns true if the MaxReplicationLagModule should ignore
// this slow replica.
// "key" refers to ReplicationLagRecord.TabletStats.Key.
func (c *replicationLagCache) ignoreSlowReplica(key string) bool {
	if len(c.slowReplicas) == 0 {
		// No slow replicas at all.
		return false
	}

	slow := c.slowReplicas[key]
	if slow {
		// Record that we're ignoring this replica.
		c.ignoredSlowReplicasInARow[key] = true

		if len(c.ignoredSlowReplicasInARow) == len(c.entries) {
			// All but this replica have been ignored in a row. Break this cycle now.
			slow = false
		}
	}

	if !slow {
		// Replica is not slow.
		if len(c.ignoredSlowReplicasInARow) != 0 {
			// Reset the set of replicas which have been slow in a row so far.
			c.ignoredSlowReplicasInARow = make(map[string]bool)
		}
	}
	return slow
}

// isIgnored returns true if the given replica is a slow, ignored replica.
// "key" refers to ReplicationLagRecord.TabletStats.Key.
// Note: Unlike ignoreSlowReplica(key), this method does not update the count
// how many replicas in a row have been ignored. Instead, it's meant to find out
// when a replica is ignored and therefore the module should not wait for it.
func (c *replicationLagCache) isIgnored(key string) bool {
	return c.slowReplicas[key]
}

// replicationLagHistory stores the most recent replicationLagRecord entries
// in a ring buffer for a single replica.
type replicationLagHistory struct {
	records []replicationLagRecord
	// current has the index in "records" of the last element added by add().
	current int
}

func newReplicationLagHistory(capacity int) *replicationLagHistory {
	return &replicationLagHistory{
		records: make([]replicationLagRecord, capacity),
		current: -1,
	}
}

func (h *replicationLagHistory) add(r replicationLagRecord) {
	h.advanceCurrent()
	h.records[h.current] = r
}

func (h *replicationLagHistory) latest() replicationLagRecord {
	return h.records[h.current]
}

// atOrAfter returns the oldest replicationLagRecord which happened at "at"
// or just after it.
// If there is no such record, a zero record is returned.
func (h *replicationLagHistory) atOrAfter(at time.Time) replicationLagRecord {
	wrapped := false
	i := h.current
	for {
		// Look at the previous (older) entry to decide if we should return the
		// current entry.
		prev := i - 1
		if prev < 0 {
			wrapped = true
			prev = len(h.records) - 1
		}

		if h.records[prev].isZero() || h.records[prev].time.Before(at) {
			// Return this entry because the previous one does not exist or
			// it happened before the time we're interested in.
			return h.records[i]
		}
		if wrapped && prev == h.current {
			// We scanned the whole list and all entries match. Return the oldest.
			return h.records[i]
		}

		i = prev
	}
}

func (h *replicationLagHistory) advanceCurrent() {
	h.current++
	if h.current == len(h.records) {
		h.current = 0
	}
}
