/*
Copyright 2026 The Vitess Authors.

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
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/protoutil"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

// TestEventAttrs checks that an event's level is always an attribute and that
// its time, file, line and text are left out when unset.
func TestEventAttrs(t *testing.T) {
	when := time.Unix(1700000000, 5)
	tests := []struct {
		name  string
		event *logutilpb.Event
		want  []slog.Attr
	}{
		{
			name:  "level only",
			event: &logutilpb.Event{Level: logutilpb.Level_WARNING},
			want:  []slog.Attr{slog.String("event_level", "WARNING")},
		},
		{
			name: "all fields",
			event: &logutilpb.Event{
				Time:  protoutil.TimeToProto(when),
				Level: logutilpb.Level_ERROR,
				File:  "reparent.go",
				Line:  42,
				Value: "primary is not reachable",
			},
			want: []slog.Attr{
				slog.String("event_level", "ERROR"),
				slog.Time("event_time", when),
				slog.String("event_file", "reparent.go"),
				slog.Int64("event_line", 42),
				slog.String("message", "primary is not reachable"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EventAttrs(tt.event)
			assert.True(t, slices.EqualFunc(got, tt.want, slog.Attr.Equal), "got %v, want %v", got, tt.want)
		})
	}
}
