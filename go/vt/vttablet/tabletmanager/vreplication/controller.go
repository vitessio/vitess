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

package vreplication

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// deprecated flags (7.0)
	_          = flag.Duration("vreplication_healthcheck_topology_refresh", 30*time.Second, "refresh interval for re-reading the topology")
	_          = flag.Duration("vreplication_healthcheck_retry_delay", 5*time.Second, "healthcheck retry delay")
	_          = flag.Duration("vreplication_healthcheck_timeout", 1*time.Minute, "healthcheck retry delay")
	retryDelay = flag.Duration("vreplication_retry_delay", 5*time.Second, "delay before retrying a failed binlog connection")
)

// controller is created by Engine. Members are initialized upfront.
// There is no mutex within a controller becaust its members are
// either read-only or self-synchronized.
type controller struct {
	vre             *Engine
	dbClientFactory func() binlogplayer.DBClient
	mysqld          mysqlctl.MysqlDaemon
	blpStats        *binlogplayer.Stats

	id           uint32
	workflow     string
	source       binlogdatapb.BinlogSource
	stopPos      string
	tabletPicker *discovery.TabletPicker

	cancel context.CancelFunc
	done   chan struct{}

	// The following fields are updated after start. So, they need synchronization.
	sourceTablet sync2.AtomicString
}

// newController creates a new controller. Unless a stream is explicitly 'Stopped',
// this function launches a goroutine to perform continuous vreplication.
func newController(ctx context.Context, params map[string]string, dbClientFactory func() binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon, ts *topo.Server, cell, tabletTypesStr string, blpStats *binlogplayer.Stats, vre *Engine) (*controller, error) {
	if blpStats == nil {
		blpStats = binlogplayer.NewStats()
	}
	ct := &controller{
		vre:             vre,
		dbClientFactory: dbClientFactory,
		mysqld:          mysqld,
		blpStats:        blpStats,
		done:            make(chan struct{}),
	}
	log.Infof("creating controller with cell: %v, tabletTypes: %v, and params: %v", cell, tabletTypesStr, params)

	// id
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		return nil, err
	}
	ct.id = uint32(id)
	ct.workflow = params["workflow"]

	blpStats.State.Set(params["state"])
	// Nothing to do if replication is stopped.
	if params["state"] == binlogplayer.BlpStopped {
		ct.cancel = func() {}
		close(ct.done)
		return ct, nil
	}

	// source, stopPos
	if err := proto.UnmarshalText(params["source"], &ct.source); err != nil {
		return nil, err
	}
	ct.stopPos = params["stop_pos"]

	if ct.source.GetExternalMysql() == "" {
		// tabletPicker
		if v := params["cell"]; v != "" {
			cell = v
		}
		if v := params["tablet_types"]; v != "" {
			tabletTypesStr = v
		}
		log.Infof("creating tablet picker for source keyspace/shard %v/%v with cell: %v and tabletTypes: %v", ct.source.Keyspace, ct.source.Shard, cell, tabletTypesStr)
		cells := strings.Split(cell, ",")

		sourceTopo := ts
		if ct.source.ExternalCluster != "" {
			sourceTopo, err = sourceTopo.OpenExternalVitessClusterServer(ctx, ct.source.ExternalCluster)
			if err != nil {
				return nil, err
			}
		}
		tp, err := discovery.NewTabletPicker(sourceTopo, cells, ct.source.Keyspace, ct.source.Shard, tabletTypesStr)
		if err != nil {
			return nil, err
		}
		ct.tabletPicker = tp
	}

	// cancel
	ctx, ct.cancel = context.WithCancel(ctx)

	go ct.run(ctx)

	return ct, nil
}

func (ct *controller) run(ctx context.Context) {
	defer func() {
		log.Infof("stream %v: stopped", ct.id)
		close(ct.done)
	}()

	for {
		err := ct.runBlp(ctx)
		if err == nil {
			return
		}
		// Sometimes, canceled contexts get wrapped as errors.
		select {
		case <-ctx.Done():
			log.Warningf("context canceled: %s", err.Error())
			return
		default:
		}
		log.Errorf("stream %v: %v, retrying after %v", ct.id, err, *retryDelay)
		ct.blpStats.ErrorCounts.Add([]string{"Stream Error"}, 1)
		timer := time.NewTimer(*retryDelay)
		select {
		case <-ctx.Done():
			log.Warningf("context canceled: %s", err.Error())
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (ct *controller) runBlp(ctx context.Context) (err error) {
	defer func() {
		ct.sourceTablet.Set("")
		if x := recover(); x != nil {
			log.Errorf("stream %v: caught panic: %v\n%s", ct.id, x, tb.Stack(4))
			err = fmt.Errorf("panic: %v", x)
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	// Call this for youtube-specific customization.
	// This should be done every time, in case mysql was restarted.
	if err := ct.mysqld.EnableBinlogPlayback(); err != nil {
		return err
	}

	dbClient := ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return vterrors.Wrap(err, "can't connect to database")
	}
	defer dbClient.Close()

	var tablet *topodatapb.Tablet
	if ct.source.GetExternalMysql() == "" {
		log.Infof("trying to find a tablet eligible for vreplication. stream id: %v", ct.id)
		tablet, err = ct.tabletPicker.PickForStreaming(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
			default:
				ct.blpStats.ErrorCounts.Add([]string{"No Source Tablet Found"}, 1)
				ct.setMessage(dbClient, fmt.Sprintf("Error picking tablet: %s", err.Error()))
			}
			return err
		}
		ct.setMessage(dbClient, fmt.Sprintf("Picked source tablet: %s", tablet.Alias.String()))
		log.Infof("found a tablet eligible for vreplication. stream id: %v  tablet: %s", ct.id, tablet.Alias.String())
		ct.sourceTablet.Set(tablet.Alias.String())
	}
	switch {
	case len(ct.source.Tables) > 0:
		// Table names can have search patterns. Resolve them against the schema.
		tables, err := mysqlctl.ResolveTables(ctx, ct.mysqld, dbClient.DBName(), ct.source.Tables)
		if err != nil {
			ct.blpStats.ErrorCounts.Add([]string{"Invalid Source"}, 1)
			return vterrors.Wrap(err, "failed to resolve table names")
		}

		player := binlogplayer.NewBinlogPlayerTables(dbClient, tablet, tables, ct.id, ct.blpStats)
		return player.ApplyBinlogEvents(ctx)
	case ct.source.KeyRange != nil:
		player := binlogplayer.NewBinlogPlayerKeyRange(dbClient, tablet, ct.source.KeyRange, ct.id, ct.blpStats)
		return player.ApplyBinlogEvents(ctx)
	case ct.source.Filter != nil:
		// Timestamp fields from binlogs are always sent as UTC.
		// So, we should set the timezone to be UTC for those values to be correctly inserted.
		if _, err := dbClient.ExecuteFetch("set @@session.time_zone = '+00:00'", 10000); err != nil {
			return err
		}
		// Tables may have varying character sets. To ship the bits without interpreting them
		// we set the character set to be binary.
		if _, err := dbClient.ExecuteFetch("set names binary", 10000); err != nil {
			return err
		}

		var vsClient VStreamerClient
		var err error
		if name := ct.source.GetExternalMysql(); name != "" {
			vsClient, err = ct.vre.ec.Get(name)
			if err != nil {
				return err
			}
		} else {
			vsClient = newTabletConnector(tablet)
		}
		if err := vsClient.Open(ctx); err != nil {
			return err
		}
		defer vsClient.Close(ctx)

		vr := newVReplicator(ct.id, &ct.source, vsClient, ct.blpStats, dbClient, ct.mysqld, ct.vre)

		return vr.Replicate(ctx)
	}
	ct.blpStats.ErrorCounts.Add([]string{"Invalid Source"}, 1)
	return fmt.Errorf("missing source")
}

func (ct *controller) setMessage(dbClient binlogplayer.DBClient, message string) error {
	ct.blpStats.History.Add(&binlogplayer.StatsHistoryRecord{
		Time:    time.Now(),
		Message: message,
	})
	query := fmt.Sprintf("update _vt.vreplication set message=%v where id=%v", encodeString(binlogplayer.MessageTruncate(message)), ct.id)
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set message: %v: %v", query, err)
	}
	return nil
}
func (ct *controller) Stop() {
	ct.cancel()
	<-ct.done
}
