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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// How many times to retry tablet selection before we
	// give up and return an error message that the user
	// can see and act upon if needed.
	tabletPickerRetries = 5
)

// controller is created by Engine. Members are initialized upfront.
// There is no mutex within a controller becaust its members are
// either read-only or self-synchronized.
type controller struct {
	vre             *Engine
	dbClientFactory func() binlogplayer.DBClient
	mysqld          mysqlctl.MysqlDaemon
	blpStats        *binlogplayer.Stats

	id           int32
	workflow     string
	source       *binlogdatapb.BinlogSource
	stopPos      string
	tabletPicker *discovery.TabletPicker

	cancel context.CancelFunc
	done   chan struct{}

	// The following fields are updated after start. So, they need synchronization.
	sourceTablet atomic.Value

	lastWorkflowError *vterrors.LastError
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
		source:          &binlogdatapb.BinlogSource{},
	}
	ct.sourceTablet.Store("")
	log.Infof("creating controller with cell: %v, tabletTypes: %v, and params: %v", cell, tabletTypesStr, params)

	// id
	id, err := strconv.ParseInt(params["id"], 10, 32)
	if err != nil {
		return nil, err
	}
	ct.id = int32(id)
	ct.workflow = params["workflow"]
	ct.lastWorkflowError = vterrors.NewLastError(fmt.Sprintf("VReplication controller %d for workflow %q", ct.id, ct.workflow), maxTimeToRetryError)

	state := params["state"]
	blpStats.State.Store(state)
	// Nothing to do if replication is stopped or is known to have an unrecoverable error.
	if state == binlogplayer.BlpStopped || state == binlogplayer.BlpError {
		ct.cancel = func() {}
		close(ct.done)
		return ct, nil
	}

	// source, stopPos
	if err := prototext.Unmarshal([]byte(params["source"]), ct.source); err != nil {
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

		ct.blpStats.ErrorCounts.Add([]string{"Stream Error"}, 1)
		binlogplayer.LogError(fmt.Sprintf("error in stream %v, retrying after %v", ct.id, retryDelay), err)
		timer := time.NewTimer(retryDelay)
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
		ct.sourceTablet.Store("")
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
		tpCtx, tpCancel := context.WithTimeout(ctx, discovery.GetTabletPickerRetryDelay()*tabletPickerRetries)
		defer tpCancel()
		tablet, err = ct.tabletPicker.PickForStreaming(tpCtx)
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
		ct.sourceTablet.Store(tablet.Alias.String())
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
		if _, err := dbClient.ExecuteFetch("set names 'binary'", 10000); err != nil {
			return err
		}
		// We must apply AUTO_INCREMENT values precisely as we got them. This include the 0 value, which is not recommended in AUTO_INCREMENT, and yet is valid.
		if _, err := dbClient.ExecuteFetch("set @@session.sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')", 10000); err != nil {
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

		vr := newVReplicator(ct.id, ct.source, vsClient, ct.blpStats, dbClient, ct.mysqld, ct.vre)
		err = vr.Replicate(ctx)
		ct.lastWorkflowError.Record(err)
		// If this is a mysql error that we know needs manual intervention OR
		// we cannot identify this as non-recoverable, but it has persisted beyond the retry limit (maxTimeToRetryError)
		if isUnrecoverableError(err) || !ct.lastWorkflowError.ShouldRetry() {
			log.Errorf("vreplication stream %d going into error state due to %+v", ct.id, err)
			if errSetState := vr.setState(binlogplayer.BlpError, err.Error()); errSetState != nil {
				return err // yes, err and not errSetState.
			}
			return nil // this will cause vreplicate to quit the workflow
		}
		return err
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
