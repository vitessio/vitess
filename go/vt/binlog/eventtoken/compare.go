// Package eventtoken includes utility methods for event token
// handling.
package eventtoken

import (
	"github.com/youtube/vitess/go/mysqlconn/replication"

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

// Fresher compares two event tokens.  It returns a negative number if
// ev1<ev2, zero if they're equal, and a positive number if
// ev1>ev2. In case of doubt (we don't have enough information to know
// for sure), it returns a negative number.
func Fresher(ev1, ev2 *querypb.EventToken) int {
	if ev1 == nil || ev2 == nil {
		// Either one is nil, we don't know.
		return -1
	}

	if ev1.Timestamp != ev2.Timestamp {
		// The timestamp is enough to set them apart.
		return int(ev1.Timestamp - ev2.Timestamp)
	}

	if ev1.Shard != "" && ev1.Shard == ev2.Shard {
		// They come from the same shard. See if we have positions.
		if ev1.Position == "" || ev2.Position == "" {
			return -1
		}

		// We can parse them.
		pos1, err := replication.DecodePosition(ev1.Position)
		if err != nil {
			return -1
		}
		pos2, err := replication.DecodePosition(ev2.Position)
		if err != nil {
			return -1
		}

		// Then compare.
		if pos1.Equal(pos2) {
			return 0
		}
		if pos1.AtLeast(pos2) {
			return 1
		}
		return -1
	}

	// We do not know.
	return -1
}
