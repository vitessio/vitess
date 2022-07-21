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
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/proto/vttime"
)

// DurationFromProto converts a durationpb type to a time.Duration. It returns a
// three-tuple of (dgo, ok, err) where dgo is the go time.Duration, ok indicates
// whether the proto value was set, and err is set on failure to convert the
// proto value.
func DurationFromProto(dpb *vttime.Duration) (time.Duration, bool, error) {
	if dpb == nil {
		return 0, false, nil
	}

	d := time.Duration(dpb.Seconds) * time.Second
	if int64(d/time.Second) != dpb.Seconds {
		return 0, true, fmt.Errorf("duration: %v is out of range for time.Duration", dpb)
	}
	if dpb.Nanos != 0 {
		d += time.Duration(dpb.Nanos) * time.Nanosecond
		if (d < 0) != (dpb.Nanos < 0) {
			return 0, true, fmt.Errorf("duration: %v is out of range for time.Duration", dpb)
		}
	}
	return d, true, nil
}

// DurationToProto converts a time.Duration to a durpb.Duration.
func DurationToProto(d time.Duration) *vttime.Duration {
	nanos := d.Nanoseconds()
	secs := nanos / 1e9
	nanos -= secs * 1e9
	return &vttime.Duration{
		Seconds: secs,
		Nanos:   int32(nanos),
	}
}
