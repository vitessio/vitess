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

package tabletserver

import (
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func (t *mockSubscriber) SchemaUpdated(gtid string, ddl string, timestamp int64) error {
	t.gtids = append(t.gtids, gtid)
	t.ddls = append(t.ddls, ddl)
	t.timestamps = append(t.timestamps, timestamp)
	return nil
}

var _ schema.Subscriber = (*mockSubscriber)(nil)
var _ VStreamer = (*fakeVstreamer)(nil)
var _ tabletenv.Env = (*fakeEnv)(nil)

var env = &fakeEnv{}

func TestReplicationWatcher(t *testing.T) {
	testCases := []struct {
		name     string
		input    [][]*binlogdatapb.VEvent
		expected []string
	}{
		{
			name:     "empty",
			input:    [][]*binlogdatapb.VEvent{{}},
			expected: nil,
		}, {
			name: "single create table",
			input: [][]*binlogdatapb.VEvent{{{
				Type:        binlogdatapb.VEventType_DDL,
				Timestamp:   643,
				CurrentTime: 943,
				Gtid:        "gtid",
				Ddl:         "create table",
			}}},
			expected: []string{"create table"},
		}, {
			name: "mixed load",
			input: [][]*binlogdatapb.VEvent{{{
				Type:        binlogdatapb.VEventType_DDL,
				Timestamp:   643,
				CurrentTime: 943,
				Gtid:        "gtid",
				Ddl:         "create table",
			}, {
				Type:        binlogdatapb.VEventType_INSERT,
				Timestamp:   644,
				CurrentTime: 944,
				Gtid:        "gtid2",
				Ddl:         "insert",
			}, {
				Type:        binlogdatapb.VEventType_DDL,
				Timestamp:   645,
				CurrentTime: 945,
				Gtid:        "gtid3",
				Ddl:         "alter table",
			}}},
			expected: []string{"create table", "alter table"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			subscriber := &mockSubscriber{}
			streamer := &fakeVstreamer{
				events: testCase.input,
			}
			watcher := &ReplicationWatcher{
				env:              env,
				watchReplication: true,
				vs:               streamer,
				subscriber:       subscriber,
			}

			// when
			watcher.Open()
			time.Sleep(1 * time.Millisecond)
			watcher.Close()

			// then
			require.True(t, streamer.called, "streamer never called")
			utils.MustMatch(t, testCase.expected, subscriber.ddls, "didnt see ddls")
		})
	}
}

type mockSubscriber struct {
	gtids      []string
	ddls       []string
	timestamps []int64
}

type fakeVstreamer struct {
	called bool
	events [][]*binlogdatapb.VEvent
}

type fakeEnv struct{}

func (f *fakeEnv) CheckMySQL() {
}

func (f *fakeEnv) Config() *tabletenv.TabletConfig {
	return nil
}

func (f *fakeEnv) DBConfigs() *dbconfigs.DBConfigs {
	return nil
}

func (f *fakeEnv) Exporter() *servenv.Exporter {
	return nil
}

func (f *fakeEnv) Stats() *tabletenv.Stats {
	return nil
}

func (f *fakeEnv) LogError() {
}

func (f *fakeVstreamer) Stream(ctx context.Context, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	f.called = true
	for _, events := range f.events {
		err := send(events)
		if err != nil {
			return err
		}
	}
	return nil
}
