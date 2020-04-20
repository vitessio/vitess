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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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

// newVReplicator creates a new vreplicator. The valid fields from the source are:
// Keyspce, Shard, Filter, OnDdl, ExternalMySql and StopAfterCopy.
// The Filter consists of Rules. Each Rule has a Match and an (inner) Filter field.
// The Match can be a table name or, if it begins with a "/", a wildcard.
// The Filter can be empty: get all rows and columns.
// The Filter can be a keyrange, like "-80": get all rows that are within the keyrange.
// The Filter can be a select expression. Examples.
//   "select * from t", same as an empty Filter,
//   "select * from t where in_keyrange('-80')", same as "-80",
//   "select * from t where in_keyrange(col1, 'hash', '-80')",
//   "select col1, col2 from t where...",
//   "select col1, keyspace_id() as ksid from t where...",
//   "select id, count(*), sum(price) from t group by id".
//   Only "in_keyrange" expressions are supported in the where clause.
//   The select expressions can be any valid non-aggregate expressions,
//   or count(*), or sum(col).
//   If the target column name does not match the source expression, an
//   alias like "a+b as targetcol" must be used.
//   More advanced constructs can be used. Please see the table plan builder
//   documentation for more info.
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

// Replicate starts a vreplication stream. It can be in one of three phases:
// 1. Init: If a request is issued with no starting position, we assume that the
// contents of the tables must be copied first. During this phase, the list of
// tables to be copied is inserted into the copy_state table. A successful insert
// gets us out of this phase.
// 2. Copy: If the copy_state table has rows, then we are in this phase. During this
// phase, we repeatedly invoke copyNext until all the tables are copied. After each
// table is successfully copied, it's removed from the copy_state table. We exit this
// phase when there are no rows left in copy_state.
// 3. Replicate: In this phase, we replicate binlog events indefinitely, unless
// a stop position was requested. This phase differs from the Init phase because
// there is a replication position.
// If a request had a starting position, then we go directly into phase 3.
// During these phases, the state of vreplication is reported as 'Init', 'Copying',
// or 'Running'. They all mean the same thing. The difference in the phases depends
// on the criteria defined above. The different states reported are mainly
// informational. The 'Stopped' state is, however, honored.
// All phases share the same plan building framework. We leverage the fact the
// row representation of a read (during copy) and a binlog event are identical.
// However, there are some subtle differences, explained in the plan builder
// code.
func (vr *vreplicator) Replicate(ctx context.Context) error {
	err := vr.replicate(ctx)
	if err != nil {
		if err := vr.setMessage(err.Error()); err != nil {
			log.Errorf("Failed to set error state: %v", err)
		}
	}
	return err
}

func (vr *vreplicator) replicate(ctx context.Context) error {
	tableKeys, err := vr.buildTableKeys()
	if err != nil {
		return err
	}
	vr.tableKeys = tableKeys

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
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
	numTablesToCopy, err = evalengine.ToInt64(qr.Rows[0][0])
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
	if message != "" {
		vr.stats.History.Add(&binlogplayer.StatsHistoryRecord{
			Time:    time.Now(),
			Message: message,
		})
	}
	vr.stats.State.Set(state)
	query := fmt.Sprintf("update _vt.vreplication set state='%v', message=%v where id=%v", state, encodeString(binlogplayer.MessageTruncate(message)), vr.id)
	if _, err := vr.dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set state: %v: %v", query, err)
	}
	return nil
}

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}
