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
	"sync"
	"time"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// VStreamer defines  the functions of VStreamer
// that the BinlogWatcher needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error
}

// BinlogWatcher is a tabletserver service that watches the
// replication stream.  It will trigger schema reloads if a DDL
// is encountered.
type BinlogWatcher struct {
	env              tabletenv.Env
	watchReplication bool
	vs               VStreamer

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBinlogWatcher creates a new BinlogWatcher.
func NewBinlogWatcher(env tabletenv.Env, vs VStreamer, config *tabletenv.TabletConfig) *BinlogWatcher {
	return &BinlogWatcher{
		env:              env,
		vs:               vs,
		watchReplication: config.WatchReplication || config.TrackSchemaVersions,
	}
}

// Open starts the BinlogWatcher service.
func (blw *BinlogWatcher) Open() {
	if blw.cancel != nil || !blw.watchReplication {
		return
	}
	log.Info("Binlog Watcher: opening")

	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	blw.cancel = cancel
	blw.wg.Add(1)
	go blw.process(ctx)
}

// Close stops the BinlogWatcher service.
func (blw *BinlogWatcher) Close() {
	if blw.cancel == nil {
		return
	}
	blw.cancel()
	blw.cancel = nil
	blw.wg.Wait()
	log.Info("Binlog Watcher: closed")
}

func (blw *BinlogWatcher) process(ctx context.Context) {
	defer blw.env.LogError()
	defer blw.wg.Done()

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	for {
		// VStreamer will reload the schema when it encounters a DDL.
		err := blw.vs.Stream(ctx, "current", nil, filter, func(events []*binlogdatapb.VEvent) error {
			return nil
		})
		log.Infof("ReplicationWatcher VStream ended: %v, retrying in 5 seconds", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
