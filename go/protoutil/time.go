package protoutil

import (
	"time"

	"vitess.io/vitess/go/vt/proto/vttime"
)

// TimeFromProto converts a vttime.Time proto message into a time.Time object.
func TimeFromProto(tpb *vttime.Time) time.Time {
	if tpb == nil {
		return time.Time{}
	}

	return time.Unix(tpb.Seconds, int64(tpb.Nanoseconds))
}

// TimeToProto converts a time.Time object into a vttime.Time proto mesasge.
func TimeToProto(t time.Time) *vttime.Time {
	secs, nanos := t.Unix(), t.UnixNano()

	nsecs := secs * 1e9
	extraNanos := nanos - nsecs
	return &vttime.Time{
		Seconds:     secs,
		Nanoseconds: int32(extraNanos),
	}
}
