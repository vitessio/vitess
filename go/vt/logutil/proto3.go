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

package logutil

import (
	"time"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

// This file contains a few functions to help with proto3.

// ProtoToTime converts a vttimepb.Time to a time.Time.
// proto3 will eventually support timestamps, at which point we'll retire
// this.
//
// A nil pointer is like the empty timestamp.
func ProtoToTime(ts *vttimepb.Time) time.Time {
	if ts == nil {
		// treat nil like the empty Timestamp
		return time.Time{}
	}
	return time.Unix(ts.Seconds, int64(ts.Nanoseconds)).UTC()
}

// TimeToProto converts the time.Time to a vttimepb.Time.
func TimeToProto(t time.Time) *vttimepb.Time {
	seconds := t.Unix()
	nanos := int64(t.Sub(time.Unix(seconds, 0)))
	return &vttimepb.Time{
		Seconds:     seconds,
		Nanoseconds: int32(nanos),
	}
}

// EventStream is an interface used by RPC clients when the streaming
// RPC returns a stream of log events.
type EventStream interface {
	// Recv returns the next event in the logs.
	// If there are no more, it will return io.EOF.
	Recv() (*logutilpb.Event, error)
}
