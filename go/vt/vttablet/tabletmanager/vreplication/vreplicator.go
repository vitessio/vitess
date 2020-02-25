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
	"vitess.io/vitess/go/vt/mysqlctl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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

// vreplicator provides the core logic to start vreplication streams
type vreplicator struct {
	vre      *Engine
	id       uint32
	dbClient *vdbClient
	// source
	source          *binlogdatapb.BinlogSource
	sourceVStreamer VStreamerClient

	stats *binlogplayer.Stats
	// mysqld is used to fetch the local schema.
	mysqld    mysqlctl.MysqlDaemon
	tableKeys map[string][]string
}

// newVReplicator creates a new vreplicator
func newVReplicator(id uint32, source *binlogdatapb.BinlogSource, sourceVStreamer VStreamerClient, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon, vre *Engine) *vreplicator {
	return &vreplicator{
		vre:             vre,
		id:              id,
		source:          source,
		sourceVStreamer: sourceVStreamer,
		stats:           stats,
		dbClient:        newVDBClient(dbClient, stats),
		mysqld:          mysqld,
	}
}

// Replicate starts a vreplication stream.
func (vr *vreplicator) Replicate(ctx context.Context) error {
	tableKeys, err := vr.buildTableKeys()
	if err != nil {
		return err
	}
	vr.tableKeys = tableKeys

	for {
		// This rollback is a no-op. It's here for safety
		// in case the functions below leave transactions open.
		vr.dbClient.Rollback()

		settings, numTablesToCopy, err := vr.readSettings(ctx)
		if err != nil {
			return err
		}
		// If any of the operations below changed state to Stopped, we should return.
		if settings.State == binlogplayer.BlpStopped {
			return nil
		}

		switch {
		case numTablesToCopy != 0:
			if err := newVCopier(vr).copyNext(ctx, settings); err != nil {
				return err
			}
		case settings.StartPos.IsZero():
			if err := newVCopier(vr).initTablesForCopy(ctx); err != nil {
				return err
			}
		default:
			if vr.source.StopAfterCopy {
				return vr.setState(binlogplayer.BlpStopped, "Stopped after copy.")
			}
			if err := vr.setState(binlogplayer.BlpRunning, ""); err != nil {
				return err
			}
			return newVPlayer(vr, settings, nil, mysql.Position{}).play(ctx)
		}
	}
}

func (vr *vreplicator) buildTableKeys() (map[string][]string, error) {
	schema, err := vr.mysqld.GetSchema(vr.dbClient.DBName(), []string{"/.*/"}, nil, false)
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
	query := fmt.Sprintf("update _vt.vreplication set message=%v where id=%v", encodeString(binlogplayer.MessageTruncate(message)), vr.id)
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
