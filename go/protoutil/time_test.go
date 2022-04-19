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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestTimeFromProto(t *testing.T) {
	now := time.Date(2021, time.June, 12, 13, 14, 15, 0 /* nanos */, time.UTC)
	vtt := TimeToProto(now)

	utils.MustMatch(t, now, TimeFromProto(vtt))

	vtt.Nanoseconds = 100
	utils.MustMatch(t, now.Add(100*time.Nanosecond), TimeFromProto(vtt))

	vtt.Nanoseconds = 1e9
	utils.MustMatch(t, now.Add(time.Second), TimeFromProto(vtt))

	assert.True(t, TimeFromProto(nil).IsZero(), "expected Go time from nil vttime to be Zero")
}

func TestTimeToProto(t *testing.T) {
	now := time.Date(2021, time.June, 12, 13, 14, 15, 0 /* nanos */, time.UTC)
	secs := now.Unix()
	utils.MustMatch(t, &vttime.Time{Seconds: secs}, TimeToProto(now))

	// Testing secs/nanos conversions
	utils.MustMatch(t, &vttime.Time{Seconds: secs, Nanoseconds: 100}, TimeToProto(now.Add(100*time.Nanosecond)))
	utils.MustMatch(t, &vttime.Time{Seconds: secs + 1}, TimeToProto(now.Add(1e9*time.Nanosecond))) // this should rollover to a full second
}
