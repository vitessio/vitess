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
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// VDiff operation and table states:
// pending/started/stopped/completed/error/unknown
type VDiffState string //nolint
const (
	PendingState    VDiffState = "pending"
	StartedState    VDiffState = "started"
	StoppedState    VDiffState = "stopped"
	CompletedState  VDiffState = "completed"
	ErrorState      VDiffState = "error"
	UnknownState    VDiffState = ""
	TimestampFormat            = "2006-01-02 15:04:05"
)

type controller struct {
	id              int64 // id from the row in _vt.vdiff
	uuid            string
	workflow        string
	workflowType    binlogdatapb.VReplicationWorkflowType
	cancel          context.CancelFunc
	dbClientFactory func() binlogplayer.DBClient
	ts              *topo.Server
	vde             *Engine // The singleton vdiff engine
	done            chan struct{}

	sources        map[string]*migrationSource // Currently picked source tablets for this shard's data
	workflowFilter string
	sourceKeyspace string
	tmc            tmclient.TabletManagerClient

	targetShardStreamer *shardStreamer
	filter              *binlogdatapb.Filter            // VReplication row filter
	options             *tabletmanagerdata.VDiffOptions // Options initially from vtctld command and later from _vt.vdiff

	sourceTimeZone, targetTimeZone string // Named time zones if conversions are necessary for datetime values

	externalCluster string // For Mount+Migrate

	// Information used in vdiff stats/metrics.
	Errors                *stats.CountersWithMultiLabels
	TableDiffRowCounts    *stats.CountersWithMultiLabels
	TableDiffPhaseTimings *stats.Timings
}

func newController(ctx context.Context, row sqltypes.RowNamedValues, dbClientFactory func() binlogplayer.DBClient,
	ts *topo.Server, vde *Engine, options *tabletmanagerdata.VDiffOptions) (*controller, error) {

	log.Infof("VDiff controller initializing for %+v", row)
	id, _ := row["id"].ToInt64()

	ct := &controller{
		id:                    id,
		uuid:                  row["vdiff_uuid"].ToString(),
		workflow:              row["workflow"].ToString(),
		dbClientFactory:       dbClientFactory,
		ts:                    ts,
		vde:                   vde,
		done:                  make(chan struct{}),
		tmc:                   vde.tmClientFactory(),
		sources:               make(map[string]*migrationSource),
		options:               options,
		Errors:                stats.NewCountersWithMultiLabels("", "", []string{"Error"}),
		TableDiffRowCounts:    stats.NewCountersWithMultiLabels("", "", []string{"Rows"}),
		TableDiffPhaseTimings: stats.NewTimings("", "", "", "TablePhase"),
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
		log.Infof("Run finished for vdiff %s", ct.uuid)
		close(ct.done)
	}()

	dbClient := ct.vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		log.Errorf("Encountered an error connecting to database for vdiff %s: %v", ct.uuid, err)
		return
	}
	defer dbClient.Close()

	qr, err := ct.vde.getVDiffByID(ctx, dbClient, ct.id)
	if err != nil {
		log.Errorf("Encountered an error getting vdiff record for %s: %v", ct.uuid, err)
		return
	}

	row := qr.Named().Row()
	state := VDiffState(strings.ToLower(row["state"].ToString()))
	switch state {
	case PendingState, StartedState:
		action := "Starting"
		if state == StartedState {
			action = "Restarting"
		}
		log.Infof("%s vdiff %s", action, ct.uuid)
		if err := ct.start(ctx, dbClient); err != nil {
			log.Errorf("Encountered an error for vdiff %s: %s", ct.uuid, err)
			if err := ct.saveErrorState(ctx, err); err != nil {
				log.Errorf("Unable to save error state for vdiff %s; giving up because %s", ct.uuid, err.Error())
			}
		}
	default:
		log.Infof("VDiff %s was not marked as runnable (state: %s), doing nothing", ct.uuid, state)
	}
}

type migrationSource struct {
	*shardStreamer

	vrID     int32
	position replication.Position
}

func (ct *controller) updateState(dbClient binlogplayer.DBClient, state VDiffState, err error) error {
	extraCols := ""
	switch state {
	case StartedState:
		extraCols = ", started_at = utc_timestamp()"
	case CompletedState:
		extraCols = ", completed_at = utc_timestamp()"
	default:
	}
	if err == nil {
		// Clear out any previous error for the vdiff on this shard
		err = errors.New("")
	}
	query := sqlparser.BuildParsedQuery(sqlUpdateVDiffState,
		encodeString(string(state)),
		encodeString(err.Error()),
		extraCols,
		ct.id,
	)
	if _, err := dbClient.ExecuteFetch(query.Query, 1); err != nil {
		return err
	}
	insertVDiffLog(ct.vde.ctx, dbClient, ct.id, fmt.Sprintf("State changed to: %s", state))
	return nil
}

func (ct *controller) start(ctx context.Context, dbClient binlogplayer.DBClient) error {
	select {
	case <-ctx.Done():
		return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
	case <-ct.done:
		return ErrVDiffStoppedByUser
	default:
	}
	ct.workflowFilter = fmt.Sprintf("where workflow = %s and db_name = %s", encodeString(ct.workflow),
		encodeString(ct.vde.dbName))
	query := sqlparser.BuildParsedQuery(sqlGetVReplicationEntry, ct.workflowFilter)
	qr, err := dbClient.ExecuteFetch(query.Query, -1)
	if err != nil {
		return err
	}
	log.Infof("Found %d vreplication streams for %s", len(qr.Rows), ct.workflow)
	for i, row := range qr.Named().Rows {
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		case <-ct.done:
			return ErrVDiffStoppedByUser
		default:
		}
		source := newMigrationSource()
		sourceBytes, err := row["source"].ToBytes()
		if err != nil {
			return err
		}
		var bls binlogdatapb.BinlogSource
		if err := prototext.Unmarshal(sourceBytes, &bls); err != nil {
			log.Errorf("Encountered an error unmarshalling vdiff binlog source for %s: %v", ct.uuid, err)
			return err
		}
		source.shard = bls.Shard
		source.vrID, _ = row["id"].ToInt32()
		ct.sourceTimeZone = bls.SourceTimeZone
		ct.targetTimeZone = bls.TargetTimeZone

		if bls.ExternalCluster != "" {
			ct.externalCluster = bls.ExternalCluster
		}

		ct.sources[source.shard] = source
		if i == 0 {
			ct.sourceKeyspace = bls.Keyspace
			ct.filter = bls.Filter
		}

		workflowType, err := row["workflow_type"].ToInt64()
		if err != nil {
			return err
		}
		ct.workflowType = binlogdatapb.VReplicationWorkflowType(workflowType)
	}

	if err := ct.validate(); err != nil {
		return err
	}

	wd, err := newWorkflowDiffer(ct, ct.options, ct.vde.collationEnv)
	if err != nil {
		return err
	}
	if err := ct.updateState(dbClient, StartedState, nil); err != nil {
		return err
	}
	if err := wd.diff(ctx); err != nil {
		log.Errorf("Encountered an error performing workflow diff for vdiff %s: %v", ct.uuid, err)
		return err
	}

	return nil
}

// markStoppedByRequest records the fact that this VDiff was stopped via user
// request and resets the error generated by cancelling the context to stop it:
//
//	"vttablet: rpc error: code = Canceled desc = context canceled"
//
// This differentiates non-user requested stops that would occur e.g. during
// PlannedReparentShard or tablet restart, in those cases the error will be saved
// and will cause the VDiff to be retried ASAP -- which is NOT what we want here.
func (ct *controller) markStoppedByRequest() error {
	dbClient := ct.vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	query, err := sqlparser.ParseAndBind(sqlUpdateVDiffStopped, sqltypes.Int64BindVariable(ct.id))
	if err != nil {
		return err
	}
	var res *sqltypes.Result
	if res, err = dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	// We don't mark it as stopped if it's already completed
	if res.RowsAffected > 0 {
		insertVDiffLog(ct.vde.ctx, dbClient, ct.id, fmt.Sprintf("State changed to: %s (by user request)", StoppedState))
	}

	return nil
}

func newMigrationSource() *migrationSource {
	return &migrationSource{shardStreamer: &shardStreamer{}}
}

func (ct *controller) validate() error {
	// TODO: check if vreplication workflow has errors, what else?
	return nil
}

// saveErrorState saves the error state for the vdiff in the database.
// It never gives up trying to save the error state, unless the context
// has been cancelled or the done channel has been closed -- indicating
// that the engine is closing or the vdiff has been explicitly stopped.
// Note that when the engine is later opened the started vdiff will be
// restarted even though we were unable to save the error state.
// It uses exponential backoff with a factor of 1.5 to avoid creating
// too many database connections.
func (ct *controller) saveErrorState(ctx context.Context, saveErr error) error {
	retryDelay := 100 * time.Millisecond
	maxRetryDelay := 60 * time.Second
	save := func() error {
		dbClient := ct.vde.dbClientFactoryFiltered()
		if err := dbClient.Connect(); err != nil {
			return err
		}
		defer dbClient.Close()

		if err := ct.updateState(dbClient, ErrorState, saveErr); err != nil {
			return err
		}
		insertVDiffLog(ctx, dbClient, ct.id, fmt.Sprintf("Error: %s", saveErr))

		return nil
	}

	for {
		if err := save(); err != nil {
			log.Warningf("Failed to persist vdiff error state: %v. Will retry in %s", err, retryDelay.String())
			select {
			case <-ctx.Done():
				return vterrors.Errorf(vtrpcpb.Code_CANCELED, "engine is shutting down")
			case <-ct.done:
				return ErrVDiffStoppedByUser
			case <-time.After(retryDelay):
				if retryDelay < maxRetryDelay {
					retryDelay = time.Duration(float64(retryDelay) * 1.5)
					if retryDelay > maxRetryDelay {
						retryDelay = maxRetryDelay
					}
				}
				continue
			}
		}

		// Success
		return nil
	}
}
