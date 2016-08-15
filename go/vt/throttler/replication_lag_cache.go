package throttler

import "time"

// replicationLagCache caches for each replica a bounded list of historic
// replicationlagRecord entries.
type replicationLagCache struct {
	// entries maps from the replica to its history.
	// The map key is replicationLagRecord.TabletStats.Key.
	entries map[string]*replicationLagHistory

	historyCapacityPerReplica int
}

func newReplicationLagCache(historyCapacityPerReplica int) *replicationLagCache {
	return &replicationLagCache{
		entries:                   make(map[string]*replicationLagHistory),
		historyCapacityPerReplica: historyCapacityPerReplica,
	}
}

// add inserts or updates "r" in the cache for the replica with the key "r.Key".
func (c *replicationLagCache) add(r replicationLagRecord) {
	if !r.Up {
		// Tablet is down. Do no longer track it.
		delete(c.entries, r.Key)
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
	if h.current > len(h.records) {
		h.current = 0
	}
}
