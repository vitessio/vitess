/*
Copyright 2021 The Vitess Authors.

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
