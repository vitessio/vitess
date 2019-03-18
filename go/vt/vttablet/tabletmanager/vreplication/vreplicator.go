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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// idleTimeout is set to slightly above 1s, compared to heartbeatTime
	// set by VStreamer at slightly below 1s. This minimizes conflicts
	// between the two timeouts.
	idleTimeout      = 1100 * time.Millisecond
	dbLockRetryDelay = 1 * time.Second
	relayLogMaxSize  = 10000
	relayLogMaxItems = 1000

	// CreateCopyState is the list of statements to execute for creating
	// the _vt.copy_state table
	CreateCopyState = []string{
		`create table if not exists _vt.copy_state (
  vrepl_id int,
  table_name varbinary(128),
  lastpk varbinary(2000),
  primary key (vrepl_id, table_name))`}
)

type vreplicator struct {
	id           uint32
	source       *binlogdatapb.BinlogSource
	sourceTablet *topodatapb.Tablet
	stats        *binlogplayer.Stats
	dbClient     *vdbClient
	// mysqld is used to fetch the local schema.
	mysqld mysqlctl.MysqlDaemon

	tableKeys map[string][]string
}

func newVReplicator(id uint32, source *binlogdatapb.BinlogSource, sourceTablet *topodatapb.Tablet, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon) *vreplicator {
	return &vreplicator{
		id:           id,
		source:       source,
		sourceTablet: sourceTablet,
		stats:        stats,
		dbClient:     newVDBClient(dbClient, stats),
		mysqld:       mysqld,
	}
}

func (vr *vreplicator) Replicate(ctx context.Context) error {
	tableKeys, err := vr.buildTableKeys()
	if err != nil {
		return err
	}
	vr.tableKeys = tableKeys

	settings, err := binlogplayer.ReadVRSettings(vr.dbClient, vr.id)
	if err != nil {
		return fmt.Errorf("error reading VReplication settings: %v", err)
	}

	if settings.State == binlogplayer.VReplicationInit {
		if err := newVCopier(vr).initTablesForCopy(ctx); err != nil {
			return err
		}
		settings.State = binlogplayer.VReplicationCopying
	}

	if settings.State == binlogplayer.VReplicationCopying {
		if err := newVCopier(vr).copyTables(ctx, settings); err != nil {
			return err
		}
		settings.State = binlogplayer.BlpRunning
		return nil
	}

	return newVPlayer(vr, settings, nil).play(ctx)
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

func (vr *vreplicator) setMessage(message string) error {
	vr.stats.History.Add(&binlogplayer.StatsHistoryRecord{
		Time:    time.Now(),
		Message: message,
	})
	query := fmt.Sprintf("update _vt.vreplication set message=%v where id=%v", encodeString(message), vr.id)
	if _, err := vr.dbClient.ExecuteFetch(query, 1); err != nil {
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
