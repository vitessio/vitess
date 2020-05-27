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
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// VStreamer defines  the functions of VStreamer
// that the replicationWatcher needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error
}

// ReplicationWatcher is a tabletserver service that watches the
// replication stream.  It will trigger schema reloads if a DDL
// is encountered.
type ReplicationWatcher struct {
	env              tabletenv.Env
	watchReplication bool
	vs               VStreamer
	subscriber       schema.Subscriber

	cancel context.CancelFunc
}

// NewReplicationWatcher creates a new ReplicationWatcher.
func NewReplicationWatcher(env tabletenv.Env, vs VStreamer, config *tabletenv.TabletConfig, schemaTracker schema.Subscriber) *ReplicationWatcher {
	return &ReplicationWatcher{
		env:              env,
		vs:               vs,
		watchReplication: config.WatchReplication,
		subscriber:       schemaTracker,
	}
}

// Open starts the ReplicationWatcher service.
func (rpw *ReplicationWatcher) Open() {
	if rpw.cancel != nil || !rpw.watchReplication {
		return
	}

	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	rpw.cancel = cancel
	go rpw.Process(ctx)
}

// Close stops the ReplicationWatcher service.
func (rpw *ReplicationWatcher) Close() {
	if rpw.cancel == nil {
		return
	}
	rpw.cancel()
	rpw.cancel = nil
}

// Process processes the replication stream.
func (rpw *ReplicationWatcher) Process(ctx context.Context) {
	defer rpw.env.LogError()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	var gtid string
	for {
		// The tracker will reload the schema and save it into _vt.schema_tracking when the vstream encounters a DDL.
		err := rpw.vs.Stream(ctx, "current", nil, filter, func(events []*binlogdatapb.VEvent) error {
			for _, event := range events {
				if event.Type == binlogdatapb.VEventType_GTID {
					gtid = event.Gtid
				}
				if event.Type == binlogdatapb.VEventType_DDL {
					log.Infof("Calling schema updated for %s %s", gtid, event.Ddl)
					rpw.subscriber.SchemaUpdated(gtid, event.Ddl, event.Timestamp)
				}
			}
			return nil
		})
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		log.Infof("VStream ended: %v, retrying in 5 seconds", err)
		time.Sleep(5 * time.Second)
	}
}
