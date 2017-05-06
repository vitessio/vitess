/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package events

import (
	"log/syslog"
	"testing"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestShardChangeSyslog(t *testing.T) {
	wantSev, wantMsg := syslog.LOG_INFO, "keyspace-123/shard-123 [shard] status value: master_alias:<cell:\"test\" uid:123 > "
	sc := &ShardChange{
		KeyspaceName: "keyspace-123",
		ShardName:    "shard-123",
		Shard: &topodatapb.Shard{
			MasterAlias: &topodatapb.TabletAlias{
				Cell: "test",
				Uid:  123,
			},
		},
		Status: "status",
	}
	gotSev, gotMsg := sc.Syslog()

	if gotSev != wantSev {
		t.Errorf("wrong severity: got %v, want %v", gotSev, wantSev)
	}
	if gotMsg != wantMsg {
		t.Errorf("wrong message: got %v, want %v", gotMsg, wantMsg)
	}
}
