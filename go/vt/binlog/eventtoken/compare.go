// Package eventtoken includes utility methods for event token
// handling.
//
// FIXME(alainjobart): it will be used to compare event tokens in
// other ways later, but for now it's a bit small.
package eventtoken

import (
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// Minimum returns an event token that is guaranteed to happen before
// both provided EventToken objects.
//
// FIXME(alainjobart) for now, we always strip the shard and position,
// and only look at timestamp. It is only used across shards so it's
// not a big deal. When we compare values within a shard, we'll have
// to fix this.
func Minimum(ev1, ev2 *querypb.EventToken) *querypb.EventToken {
	if ev1 == nil || ev2 == nil {
		// One or the other is not set, we can't do anything.
		return nil
	}

	if ev1.Timestamp < ev2.Timestamp {
		return &querypb.EventToken{
			Timestamp: ev1.Timestamp,
		}
	}
	return &querypb.EventToken{
		Timestamp: ev2.Timestamp,
	}
}

// Fresher returns true if ev1 is fresher than ev2. In case of doubt,
// it returns false.
//
// FIXME(alainjobart): if both events come from the same shard, we can
// compare replication position (after parsing it). Need to implement that.
// FIMXE(alainjobart): when it's more than just a simple compare, add
// unit tests too.
func Fresher(ev1, ev2 *querypb.EventToken) bool {
	if ev1 == nil || ev2 == nil {
		return false
	}

	return ev1.Timestamp > ev2.Timestamp
}
