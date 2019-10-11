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
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var (
	// idleTimeout is set to slightly above 1s, compared to heartbeatTime
	// set by VStreamer at slightly below 1s. This minimizes conflicts
	// between the two timeouts.
	idleTimeout         = 1100 * time.Millisecond
	dbLockRetryDelay    = 1 * time.Second
	relayLogMaxSize     = 30000
	relayLogMaxItems    = 1000
	copyTimeout         = 1 * time.Hour
	replicaLagTolerance = 10 * time.Second
)

type vreplicator struct {
	id       uint32
	dbClient *vdbClient
	// source
	source          *binlogdatapb.BinlogSource
	sourceVStreamer VStreamerClient

	// target
	stats *binlogplayer.Stats
	// sl is used to fetch the local schema.
	sl SchemasLoader

	tableKeys map[string][]string
}

type SchemasLoader interface {
	GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error)
}

// NewVReplicator creates a new vreplicator
func NewVReplicator(id uint32, source *binlogdatapb.BinlogSource, sourceVStreamer VStreamerClient, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, sl SchemasLoader) *vreplicator {
	return &vreplicator{
		id:              id,
		source:          source,
		sourceVStreamer: sourceVStreamer,
		stats:           stats,
		dbClient:        newVDBClient(dbClient, stats),
		sl:              sl,
	}
}

func (vr *vreplicator) Replicate(ctx context.Context) error {
	tableKeys, err := vr.buildTableKeys()
	if err != nil {
		return err
	}
	vr.tableKeys = tableKeys

	for {
		settings, numTablesToCopy, err := vr.readSettings(ctx)
		if err != nil {
			return fmt.Errorf("error reading VReplication settings: %v", err)
		}
		// If any of the operations below changed state to Stopped, we should return.
		if settings.State == binlogplayer.BlpStopped {
			return nil
		}

		// TODO: This will get remove once we use filename:pos flavor
		_, err = mysql.ParseFilePosition(settings.StartPos)
		isFilePos := err == nil

		switch {
		case numTablesToCopy != 0:
			if err := newVCopier(vr).copyNext(ctx, settings); err != nil {
				return err
			}
		case settings.GtidStartPos.IsZero() && !isFilePos:
			if err := newVCopier(vr).initTablesForCopy(ctx); err != nil {
				return err
			}
		default:
			if err := vr.setState(binlogplayer.BlpRunning, ""); err != nil {
				return err
			}
			return newVPlayer(vr, settings, nil, mysql.Position{}).play(ctx)
		}
	}
}

func (vr *vreplicator) buildTableKeys() (map[string][]string, error) {
	schema, err := vr.sl.GetSchema(vr.dbClient.DBName(), []string{"/.*/"}, nil, false)
	if err != nil {
		return nil, err
	}
	tableKeys := make(map[string][]string)
	for _, td := range schema.TableDefinitions {
		if len(td.PrimaryKeyColumns) != 0 {
			tableKeys[td.Name] = td.PrimaryKeyColumns
		} else {
			tableKeys[td.Name] = td.Columns
		}
	}
	return tableKeys, nil
}

func (vr *vreplicator) readSettings(ctx context.Context) (settings binlogplayer.VRSettings, numTablesToCopy int64, err error) {
	settings, err = binlogplayer.ReadVRSettings(vr.dbClient, vr.id)
	if err != nil {
		return settings, numTablesToCopy, fmt.Errorf("error reading VReplication settings: %v", err)
	}

	query := fmt.Sprintf("select count(*) from _vt.copy_state where vrepl_id=%d", vr.id)
	qr, err := vr.dbClient.Execute(query)
	if err != nil {
		// If it's a not found error, create it.
		merr, isSQLErr := err.(*mysql.SQLError)
		if !isSQLErr || !(merr.Num == mysql.ERNoSuchTable || merr.Num == mysql.ERBadDb) {
			return settings, numTablesToCopy, err
		}
		log.Info("Looks like _vt.copy_state table may not exist. Trying to create... ")
		if _, merr := vr.dbClient.Execute(createCopyState); merr != nil {
			log.Errorf("Failed to ensure _vt.copy_state table exists: %v", merr)
			return settings, numTablesToCopy, err
		}
		// Redo the read.
		qr, err = vr.dbClient.Execute(query)
		if err != nil {
			return settings, numTablesToCopy, err
		}
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return settings, numTablesToCopy, fmt.Errorf("unexpected result from %s: %v", query, qr)
	}
	numTablesToCopy, err = sqltypes.ToInt64(qr.Rows[0][0])
	if err != nil {
		return settings, numTablesToCopy, err
	}
	return settings, numTablesToCopy, nil
}

func (vr *vreplicator) setMessage(message string) error {
	vr.stats.History.Add(&binlogplayer.StatsHistoryRecord{
		Time:    time.Now(),
		Message: message,
	})
	query := fmt.Sprintf("update _vt.vreplication set message=%v where id=%v", encodeString(message), vr.id)
	if _, err := vr.dbClient.Execute(query); err != nil {
		return fmt.Errorf("could not set message: %v: %v", query, err)
	}
	return nil
}

func (vr *vreplicator) setState(state, message string) error {
	return binlogplayer.SetVReplicationState(vr.dbClient, vr.id, state, message)
}

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}
