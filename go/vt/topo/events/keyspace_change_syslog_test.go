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

package events

import (
	"log/syslog"
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestKeyspaceChangeSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123 [keyspace] status value: sharding_column_name:\"sharded_by_me\" "
	kc := &KeyspaceChange{
		KeyspaceName: "keyspace-123",
		Keyspace: &topodatapb.Keyspace{
			ShardingColumnName: "sharded_by_me",
		},
		Status: "status",
	}
	gotSev, gotMsg := kc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
