/*
Copyright 2020 The Vitess Authors.

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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestTracker(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t)
	defer cancel()

	gtid1 := "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10"
	ddl1 := "create table tracker_test (id int)"
	query := "CREATE TABLE IF NOT EXISTS _vt.schema_version.*"
	db.AddQueryPattern(query, &sqltypes.Result{})

	db.AddQueryPattern("insert into _vt.schema_version.*", &sqltypes.Result{})

	vs := &fakeVstreamer{
		done: make(chan struct{}),
		events: [][]*binlogdatapb.VEvent{{
			{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: gtid1,
			}, {
				Type: binlogdatapb.VEventType_DDL,
				Ddl:  ddl1,
			},
			{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: "",
			}, {
				Type: binlogdatapb.VEventType_DDL,
				Ddl:  ddl1,
			},
			{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: gtid1,
			}, {
				Type: binlogdatapb.VEventType_DDL,
				Ddl:  "",
			},
		}},
	}
	config := se.env.Config()
	config.TrackSchemaVersions = true
	env := tabletenv.NewEnv(config, "TrackerTest")
	initial := env.Stats().ErrorCounters.Counts()["INTERNAL"]
	tracker := NewTracker(env, vs, se)
	tracker.Open()
	<-vs.done
	cancel()
	tracker.Close()
	// Two of those events should have caused an error.
	final := env.Stats().ErrorCounters.Counts()["INTERNAL"]
	assert.Equal(t, initial+2, final)
}

var _ VStreamer = (*fakeVstreamer)(nil)

type fakeVstreamer struct {
	done   chan struct{}
	events [][]*binlogdatapb.VEvent
}

func (f *fakeVstreamer) Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	for _, events := range f.events {
		err := send(events)
		if err != nil {
			return err
		}
	}
	close(f.done)
	<-ctx.Done()
	return nil
}
