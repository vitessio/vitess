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

package tabletserver

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/binlog/eventtoken"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ReplicationWatcher is a tabletserver service that watches the
// replication stream. It can tell you the current event token,
// and it will trigger schema reloads if a DDL is encountered.
type ReplicationWatcher struct {
	dbconfigs *dbconfigs.DBConfigs

	// Life cycle management vars
	isOpen bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	watchReplication bool
	se               *schema.Engine

	mu         sync.Mutex
	eventToken *querypb.EventToken
}

// NewReplicationWatcher creates a new ReplicationWatcher.
func NewReplicationWatcher(env *servenv.Embedder, se *schema.Engine, config tabletenv.TabletConfig) *ReplicationWatcher {
	rpw := &ReplicationWatcher{
		watchReplication: config.WatchReplication,
		se:               se,
	}
	env.Publish("EventTokenPosition", stats.StringFunc(func() string {
		if e := rpw.EventToken(); e != nil {
			return e.Position
		}
		return ""
	}))
	env.NewGaugeFunc(
		"EventTokenTimestamp",
		"Replication watcher event token timestamp",
		func() int64 {
			if e := rpw.EventToken(); e != nil {
				return e.Timestamp
			}
			return 0
		})
	return rpw
}

// InitDBConfig must be called before Open.
func (rpw *ReplicationWatcher) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	rpw.dbconfigs = dbcfgs
}

// Open starts the ReplicationWatcher service.
func (rpw *ReplicationWatcher) Open() {
	if rpw.isOpen || !rpw.watchReplication {
		return
	}
	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	rpw.cancel = cancel
	rpw.wg.Add(1)
	go rpw.Process(ctx, rpw.dbconfigs)
	rpw.isOpen = true
}

// Close stops the ReplicationWatcher service.
func (rpw *ReplicationWatcher) Close() {
	if !rpw.isOpen {
		return
	}
	rpw.cancel()
	rpw.wg.Wait()
	rpw.isOpen = false
}

// Process processes the replication stream.
func (rpw *ReplicationWatcher) Process(ctx context.Context, dbconfigs *dbconfigs.DBConfigs) {
	defer func() {
		tabletenv.LogError()
		rpw.wg.Done()
	}()
	for {
		log.Infof("Starting a binlog Streamer from current replication position to monitor binlogs")
		streamer := binlog.NewStreamer(dbconfigs.DbaWithDB(), rpw.se, nil /*clientCharset*/, mysql.Position{}, 0 /*timestamp*/, func(eventToken *querypb.EventToken, statements []binlog.FullBinlogStatement) error {
			// Save the event token.
			rpw.mu.Lock()
			rpw.eventToken = eventToken
			rpw.mu.Unlock()

			// If it's a DDL, trigger a schema reload.
			for _, statement := range statements {
				if statement.Statement.Category != binlogdatapb.BinlogTransaction_Statement_BL_DDL {
					continue
				}
				err := rpw.se.Reload(ctx)
				log.Infof("Streamer triggered a schema reload, with result: %v", err)
				return nil
			}

			return nil
		})

		if err := streamer.Stream(ctx); err != nil {
			log.Infof("Streamer stopped: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// ComputeExtras returns the requested ResultExtras based on the supplied options.
func (rpw *ReplicationWatcher) ComputeExtras(options *querypb.ExecuteOptions) *querypb.ResultExtras {
	if options == nil {
		// No options passed in.
		return nil
	}

	if !options.IncludeEventToken && options.CompareEventToken == nil {
		// The flags that make extras exist are not there.
		return nil
	}

	et := rpw.EventToken()
	if et == nil {
		return nil
	}

	var extras *querypb.ResultExtras

	// See if we need to fill in EventToken.
	if options.IncludeEventToken {
		extras = &querypb.ResultExtras{
			EventToken: et,
		}
	}

	// See if we need to compare.
	if options.CompareEventToken != nil {
		if eventtoken.Fresher(et, options.CompareEventToken) >= 0 {
			// For a query, we are fresher if greater or equal
			// to the provided compare_event_token.
			if extras == nil {
				extras = &querypb.ResultExtras{
					Fresher: true,
				}
			} else {
				extras.Fresher = true
			}
		}
	}
	return extras
}

// EventToken returns the current event token.
func (rpw *ReplicationWatcher) EventToken() *querypb.EventToken {
	rpw.mu.Lock()
	defer rpw.mu.Unlock()
	return rpw.eventToken
}
