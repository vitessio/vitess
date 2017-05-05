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

package events

import (
	"log/syslog"
	"testing"

	base "github.com/youtube/vitess/go/vt/events"
)

func TestSplitCloneSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123/cell-1 [split clone] status"
	ev := &SplitClone{
		Cell:          "cell-1",
		Keyspace:      "keyspace-123",
		Shard:         "shard-123",
		StatusUpdater: base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}

func TestVerticalSplitCloneSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123/cell-1 [vertical split clone] status"
	ev := &VerticalSplitClone{
		Cell:          "cell-1",
		Keyspace:      "keyspace-123",
		Shard:         "shard-123",
		StatusUpdater: base.StatusUpdater{Status: "status"},
	}
	gotSev, gotMsg := ev.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
