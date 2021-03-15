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
	"strings"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

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
	idleTimeout = 1100 * time.Millisecond

	dbLockRetryDelay    = 1 * time.Second
	relayLogMaxSize     = flag.Int("relay_log_max_size", 250000, "Maximum buffer size (in bytes) for VReplication target buffering. If single rows are larger than this, a single row is buffered at a time.")
	relayLogMaxItems    = flag.Int("relay_log_max_items", 5000, "Maximum number of rows for VReplication target buffering.")
	copyTimeout         = 1 * time.Hour
	replicaLagTolerance = 10 * time.Second

	// vreplicationHeartbeatUpdateInterval determines how often the time_updated column is updated if there are no real events on the source and the source
	// vstream is only sending heartbeats for this long. Keep this low if you expect high QPS and are monitoring this column to alert about potential
	// outages. Keep this high if
	// 		you have too many streams the extra write qps or cpu load due to these updates are unacceptable
	//		you have too many streams and/or a large source field (lot of participating tables) which generates unacceptable increase in your binlog size
	vreplicationHeartbeatUpdateInterval = flag.Int("vreplication_heartbeat_update_interval", 1, "Frequency (in seconds, default 1, max 60) at which the time_updated column of a vreplication stream when idling")
	// vreplicationMinimumHeartbeatUpdateInterval overrides vreplicationHeartbeatUpdateInterval if the latter is higher than this
	// to ensure that it satisfies liveness criteria implicitly expected by internal processes like Online DDL
	vreplicationMinimumHeartbeatUpdateInterval = 60
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
	pkInfoMap map[string][]*PrimaryKeyInfo

	originalFKCheckSetting int64
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
	if *vreplicationHeartbeatUpdateInterval > vreplicationMinimumHeartbeatUpdateInterval {
		log.Warningf("the supplied value for vreplication_heartbeat_update_interval:%d seconds is larger than the maximum allowed:%d seconds, vreplication will fallback to %d",
			*vreplicationHeartbeatUpdateInterval, vreplicationMinimumHeartbeatUpdateInterval, vreplicationMinimumHeartbeatUpdateInterval)
	}
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
		log.Errorf("Replicate error: %s", err.Error())
		if err := vr.setMessage(fmt.Sprintf("Error: %s", err.Error())); err != nil {
			log.Errorf("Failed to set error state: %v", err)
		}
	}
	return err
}

func (vr *vreplicator) replicate(ctx context.Context) error {
	pkInfo, err := vr.buildPkInfoMap(ctx)
	if err != nil {
		return err
	}
	vr.pkInfoMap = pkInfo
	if err := vr.getSettingFKCheck(); err != nil {
		return err
	}
	//defensive guard, should be a no-op since it should happen after copy is done
	defer vr.resetFKCheckAfterCopy()
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
			if err := vr.clearFKCheck(); err != nil {
				log.Warningf("Unable to clear FK check %v", err)
				return err
			}
			if err := newVCopier(vr).copyNext(ctx, settings); err != nil {
				vr.stats.ErrorCounts.Add([]string{"Copy"}, 1)
				return err
			}
		case settings.StartPos.IsZero():
			if err := newVCopier(vr).initTablesForCopy(ctx); err != nil {
				vr.stats.ErrorCounts.Add([]string{"Copy"}, 1)
				return err
			}
		default:
			if err := vr.resetFKCheckAfterCopy(); err != nil {
				log.Warningf("Unable to reset FK check %v", err)
				return err
			}
			if vr.source.StopAfterCopy {
				return vr.setState(binlogplayer.BlpStopped, "Stopped after copy.")
			}
			if err := vr.setState(binlogplayer.BlpRunning, ""); err != nil {
				vr.stats.ErrorCounts.Add([]string{"Replicate"}, 1)
				return err
			}
			return newVPlayer(vr, settings, nil, mysql.Position{}, "replicate").play(ctx)
		}
	}
}

// PrimaryKeyInfo is used to store charset and collation for primary keys where applicable
type PrimaryKeyInfo struct {
	Name       string
	CharSet    string
	Collation  string
	DataType   string
	ColumnType string
}

func (vr *vreplicator) buildPkInfoMap(ctx context.Context) (map[string][]*PrimaryKeyInfo, error) {
	schema, err := vr.mysqld.GetSchema(ctx, vr.dbClient.DBName(), []string{"/.*/"}, nil, false)
	if err != nil {
		return nil, err
	}
	queryTemplate := "select character_set_name, collation_name, column_name, data_type, column_type from information_schema.columns where table_schema=%s and table_name=%s;"
	pkInfoMap := make(map[string][]*PrimaryKeyInfo)
	for _, td := range schema.TableDefinitions {

		query := fmt.Sprintf(queryTemplate, encodeString(vr.dbClient.DBName()), encodeString(td.Name))
		qr, err := vr.mysqld.FetchSuperQuery(ctx, query)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) == 0 {
			return nil, fmt.Errorf("no data returned from information_schema.columns")
		}

		var pks []string
		if len(td.PrimaryKeyColumns) != 0 {
			pks = td.PrimaryKeyColumns
		} else {
			pks = td.Columns
		}
		var pkInfos []*PrimaryKeyInfo
		for _, pk := range pks {
			charSet := ""
			collation := ""
			var dataType, columnType string
			for _, row := range qr.Rows {
				columnName := row[2].ToString()
				if strings.EqualFold(columnName, pk) {
					var currentField *querypb.Field
					for _, field := range td.Fields {
						if field.Name == pk {
							currentField = field
							break
						}
					}
					if currentField == nil {
						continue
					}
					dataType = row[3].ToString()
					columnType = row[4].ToString()
					if sqltypes.IsText(currentField.Type) {
						charSet = row[0].ToString()
						collation = row[1].ToString()
					}
					break
				}
			}
			if dataType == "" || columnType == "" {
				return nil, fmt.Errorf("no dataType/columnType found in information_schema.columns for table %s, column %s", td.Name, pk)
			}
			pkInfos = append(pkInfos, &PrimaryKeyInfo{
				Name:       pk,
				CharSet:    charSet,
				Collation:  collation,
				DataType:   dataType,
				ColumnType: columnType,
			})
		}
		pkInfoMap[td.Name] = pkInfos
	}
	return pkInfoMap, nil
}

func (vr *vreplicator) readSettings(ctx context.Context) (settings binlogplayer.VRSettings, numTablesToCopy int64, err error) {
	settings, err = binlogplayer.ReadVRSettings(vr.dbClient, vr.id)
	if err != nil {
		return settings, numTablesToCopy, fmt.Errorf("error reading VReplication settings: %v", err)
	}

	query := fmt.Sprintf("select count(*) from _vt.copy_state where vrepl_id=%d", vr.id)
	qr, err := withDDL.Exec(ctx, query, vr.dbClient.ExecuteFetch)
	if err != nil {
		return settings, numTablesToCopy, err
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

func (vr *vreplicator) getSettingFKCheck() error {
	qr, err := vr.dbClient.Execute("select @@foreign_key_checks;")
	if err != nil {
		return err
	}
	if len(qr.Rows) != 1 || len(qr.Fields) != 1 {
		return fmt.Errorf("unable to select @@foreign_key_checks")
	}
	vr.originalFKCheckSetting, err = evalengine.ToInt64(qr.Rows[0][0])
	if err != nil {
		return err
	}
	return nil
}

func (vr *vreplicator) resetFKCheckAfterCopy() error {
	_, err := vr.dbClient.Execute(fmt.Sprintf("set foreign_key_checks=%d;", vr.originalFKCheckSetting))
	return err
}

func (vr *vreplicator) clearFKCheck() error {
	_, err := vr.dbClient.Execute("set foreign_key_checks=0;")
	return err
}
