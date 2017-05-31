/*
Copyright 2017 Google Inc.

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
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

const (
	// Seconds field of the earliest valid Timestamp.
	// This is time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix().
	minValidSeconds = -62135596800
	// Seconds field just after the latest valid Timestamp.
	// This is time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).Unix().
	maxValidSeconds = 253402300800
)

func utcDate(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

var tests = []struct {
	pt *logutilpb.Time
	t  time.Time
}{
	// The timestamp representing the Unix epoch date.
	{pt: &logutilpb.Time{Seconds: 0, Nanoseconds: 0},
		t: utcDate(1970, 1, 1)},

	// The smallest representable timestamp with non-negative nanos.
	{pt: &logutilpb.Time{Seconds: math.MinInt64, Nanoseconds: 0},
		t: time.Unix(math.MinInt64, 0).UTC()},

	// The earliest valid timestamp.
	{pt: &logutilpb.Time{Seconds: minValidSeconds, Nanoseconds: 0},
		t: utcDate(1, 1, 1)},

	// The largest representable timestamp with nanos in range.
	{pt: &logutilpb.Time{Seconds: math.MaxInt64, Nanoseconds: 1e9 - 1},
		t: time.Unix(math.MaxInt64, 1e9-1).UTC()},

	// The largest valid timestamp.
	{pt: &logutilpb.Time{Seconds: maxValidSeconds - 1, Nanoseconds: 1e9 - 1},
		t: time.Date(9999, 12, 31, 23, 59, 59, 1e9-1, time.UTC)},

	// The smallest invalid timestamp that is larger than the valid range.
	{pt: &logutilpb.Time{Seconds: maxValidSeconds, Nanoseconds: 0},
		t: time.Unix(maxValidSeconds, 0).UTC()},

	// A date before the epoch.
	{pt: &logutilpb.Time{Seconds: -281836800, Nanoseconds: 0},
		t: utcDate(1961, 1, 26)},

	// A date after the epoch.
	{pt: &logutilpb.Time{Seconds: 1296000000, Nanoseconds: 0},
		t: utcDate(2011, 1, 26)},

	// A date after the epoch, in the middle of the day.
	{pt: &logutilpb.Time{Seconds: 1296012345, Nanoseconds: 940483},
		t: time.Date(2011, 1, 26, 3, 25, 45, 940483, time.UTC)},
}

func TestProtoToTime(t *testing.T) {
	for i, s := range tests {
		got := ProtoToTime(s.pt)
		if got != s.t {
			t.Errorf("ProtoToTime[%v](%v) = %v, want %v", i, s.pt, got, s.t)
		}
	}
}

func TestTimeToProto(t *testing.T) {
	for i, s := range tests {
		got := TimeToProto(s.t)
		if !proto.Equal(got, s.pt) {
			t.Errorf("TimeToProto[%v](%v) = %v, want %v", i, s.t, got, s.pt)
		}
	}
}
