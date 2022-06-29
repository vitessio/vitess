/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/withddl"

	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

/*
  vdiff operation states: pending/started/completed/error/unknown
  vdiff table states: pending/started/completed/error/unknown
*/
type VDiffState string //nolint
const (
	PendingState    VDiffState = "pending"
	StartedState    VDiffState = "started"
	CompletedState  VDiffState = "completed"
	ErrorState      VDiffState = "error"
	UnknownState    VDiffState = ""
	TimestampFormat            = "2006-01-02 15:04:05"
)

type controller struct {
	id              int64 // id from row in _vt.vdiff
	uuid            string
	workflow        string
	cancel          context.CancelFunc
	dbClientFactory func() binlogplayer.DBClient
	ts              *topo.Server
	vde             *Engine // the singleton vdiff engine
	done            chan struct{}

	sources        map[string]*migrationSource // currently picked source tablets for this shard's data
	workflowFilter string
	sourceKeyspace string
	tmc            tmclient.TabletManagerClient

	targetShardStreamer *shardStreamer
	filter              *binlogdatapb.Filter            // vreplication row filter
	options             *tabletmanagerdata.VDiffOptions // options initially from vtctld command and later from _vt.vdiff
}

func newController(ctx context.Context, row sqltypes.RowNamedValues, dbClientFactory func() binlogplayer.DBClient,
	ts *topo.Server, vde *Engine, options *tabletmanagerdata.VDiffOptions) (*controller, error) {

	log.Infof("VDiff controller initializing for %+v", row)
	id, _ := row["id"].ToInt64()

	ct := &controller{
		id:              id,
		uuid:            row["vdiff_uuid"].String(),
		workflow:        row["workflow"].ToString(),
		dbClientFactory: dbClientFactory,
		ts:              ts,
		vde:             vde,
		done:            make(chan struct{}),
		tmc:             tmclient.NewTabletManagerClient(),
		sources:         make(map[string]*migrationSource),
		options:         options,
	}
	ctx, ct.cancel = context.WithCancel(ctx)
	go ct.run(ctx)

	return ct, nil
}

func (ct *controller) Stop() {
	ct.cancel()
	<-ct.done
}

func (ct *controller) run(ctx context.Context) {
	defer func() {
		log.Infof("vdiff stopped")
		close(ct.done)
	}()

	dbClient := ct.vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		log.Errorf("db connect error: %v", err)
		return
	}
	defer dbClient.Close()

	ct.vde.vdiffSchemaCreateOnce.Do(func() {
		_, _ = withDDL.ExecIgnore(ctx, withddl.QueryToTriggerWithDDL, dbClient.ExecuteFetch)
	})

	query := fmt.Sprintf(sqlGetVDiffByID, ct.id)
	qr, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch)
	if err != nil {
		log.Errorf(fmt.Sprintf("No data for %s", query), err)
		return
	}
	if len(qr.Rows) == 0 {
		log.Errorf("Missing vdiff row for %s", query)
		return
	}

	row := qr.Named().Row()
	state := VDiffState(strings.ToLower(row["state"].ToString()))
	switch state {
	case PendingState:
		log.Infof("Starting vdiff")
		if err := ct.start(ctx, dbClient); err != nil {
			log.Errorf("run() failed: %s", err)
			insertVDiffLog(ctx, dbClient, ct.id, fmt.Sprintf("Error: %s", err))
			if err := ct.updateState(dbClient, ErrorState); err != nil {
				return
			}
			return
		}
	default:
		log.Infof("run() done, state is %s", state)
	}
	log.Infof("run() has ended")
}

type migrationSource struct {
	*shardStreamer

	vrID     int64
	position mysql.Position
}

func (ct *controller) updateState(dbClient binlogplayer.DBClient, state VDiffState) error {
	extraCols := ""
	if state == StartedState {
		extraCols = ", started_at = utc_timestamp()"
	} else if state == CompletedState {
		extraCols = ", completed_at = utc_timestamp()"
	}
	query := fmt.Sprintf(sqlUpdateVDiffState, encodeString(string(state)), extraCols, ct.id)
	if _, err := withDDL.Exec(ct.vde.ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
		return err
	}
	insertVDiffLog(ct.vde.ctx, dbClient, ct.id, fmt.Sprintf("State changed to: %s", state))
	return nil
}

func (ct *controller) start(ctx context.Context, dbClient binlogplayer.DBClient) error {
	ct.workflowFilter = fmt.Sprintf("where workflow = %s and db_name = %s", encodeString(ct.workflow), encodeString(ct.vde.dbName))
	query := fmt.Sprintf(sqlGetVReplicationEntry, ct.workflowFilter)
	qr, err := withDDL.Exec(ct.vde.ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch)
	if err != nil {
		return err
	}
	log.Infof("Found %d vreplication streams for %s", len(qr.Rows), ct.workflow)
	for i, row := range qr.Named().Rows {
		source := newMigrationSource()
		sourceBytes, err := row["source"].ToBytes()
		if err != nil {
			return err
		}
		var bls binlogdatapb.BinlogSource
		if err := prototext.Unmarshal(sourceBytes, &bls); err != nil {
			log.Errorf("Failed to unmarshal vdiff binlog source: %v", err)
			return err
		}
		source.shard = bls.Shard
		source.vrID, _ = row["id"].ToInt64()

		ct.sources[source.shard] = source
		if i == 0 {
			ct.sourceKeyspace = bls.Keyspace
			ct.filter = bls.Filter
		}
	}

	if err := ct.validate(); err != nil {
		return err
	}

	wd, err := newWorkflowDiffer(ct, ct.options)
	if err != nil {
		return err
	}
	if err := ct.updateState(dbClient, StartedState); err != nil {
		return err
	}
	if err := wd.diff(ctx); err != nil {
		log.Infof("wd.diff error %v", err)
		return err
	}

	return nil
}

func newMigrationSource() *migrationSource {
	return &migrationSource{shardStreamer: &shardStreamer{}}
}

func (ct *controller) validate() error {
	// todo: check if vreplication workflow has errors, what else?
	return nil
}
