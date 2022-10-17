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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var (
	// idleTimeout is set to slightly above 1s, compared to heartbeatTime
	// set by VStreamer at slightly below 1s. This minimizes conflicts
	// between the two timeouts.
	idleTimeout = 1100 * time.Millisecond

	dbLockRetryDelay = 1 * time.Second

	// vreplicationMinimumHeartbeatUpdateInterval overrides vreplicationHeartbeatUpdateInterval if the latter is higher than this
	// to ensure that it satisfies liveness criteria implicitly expected by internal processes like Online DDL
	vreplicationMinimumHeartbeatUpdateInterval = 60

	vreplicationExperimentalFlagOptimizeInserts int64 = 1
)

const (
	getSQLModeQuery = `SELECT @@session.sql_mode AS sql_mode`
	// SQLMode should be used whenever performing a schema change as part of a vreplication
	// workflow to ensure that you set a permissive SQL mode as defined by
	// VReplication. We follow MySQL's model for recreating database objects
	// on a target -- using SQL statements generated from a source -- which
	// ensures that we can recreate them regardless of the sql_mode that was
	// in effect on the source when it was created:
	//   https://github.com/mysql/mysql-server/blob/3290a66c89eb1625a7058e0ef732432b6952b435/client/mysqldump.cc#L795-L818
	SQLMode          = "NO_AUTO_VALUE_ON_ZERO"
	StrictSQLMode    = "STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO"
	setSQLModeQueryf = `SET @@session.sql_mode='%s'`
)

type ComponentName string

const (
	VPlayerComponentName     ComponentName = "vplayer"
	VCopierComponentName     ComponentName = "vcopier"
	VStreamerComponentName   ComponentName = "vstreamer"
	RowStreamerComponentName ComponentName = "rowstreamer"
)

// vreplicator provides the core logic to start vreplication streams
type vreplicator struct {
	vre      *Engine
	id       uint32
	dbClient *vdbClient
	// source
	source          *binlogdatapb.BinlogSource
	sourceVStreamer VStreamerClient
	state           string
	stats           *binlogplayer.Stats
	// mysqld is used to fetch the local schema.
	mysqld     mysqlctl.MysqlDaemon
	colInfoMap map[string][]*ColumnInfo

	originalFKCheckSetting int64
	originalSQLMode        string

	WorkflowType int32
	WorkflowName string

	throttleUpdatesRateLimiter *timer.RateLimiter
}

// newVReplicator creates a new vreplicator. The valid fields from the source are:
// Keyspce, Shard, Filter, OnDdl, ExternalMySql and StopAfterCopy.
// The Filter consists of Rules. Each Rule has a Match and an (inner) Filter field.
// The Match can be a table name or, if it begins with a "/", a wildcard.
// The Filter can be empty: get all rows and columns.
// The Filter can be a keyrange, like "-80": get all rows that are within the keyrange.
// The Filter can be a select expression. Examples.
//
//	"select * from t", same as an empty Filter,
//	"select * from t where in_keyrange('-80')", same as "-80",
//	"select * from t where in_keyrange(col1, 'hash', '-80')",
//	"select col1, col2 from t where...",
//	"select col1, keyspace_id() as ksid from t where...",
//	"select id, count(*), sum(price) from t group by id",
//	"select * from t where customer_id=1 and val = 'newton'".
//	Only "in_keyrange" expressions, integer and string comparisons are supported in the where clause.
//	The select expressions can be any valid non-aggregate expressions,
//	or count(*), or sum(col).
//	If the target column name does not match the source expression, an
//	alias like "a+b as targetcol" must be used.
//	More advanced constructs can be used. Please see the table plan builder
//	documentation for more info.
func newVReplicator(id uint32, source *binlogdatapb.BinlogSource, sourceVStreamer VStreamerClient, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon, vre *Engine) *vreplicator {
	if vreplicationHeartbeatUpdateInterval > vreplicationMinimumHeartbeatUpdateInterval {
		log.Warningf("the supplied value for vreplication_heartbeat_update_interval:%d seconds is larger than the maximum allowed:%d seconds, vreplication will fallback to %d",
			vreplicationHeartbeatUpdateInterval, vreplicationMinimumHeartbeatUpdateInterval, vreplicationMinimumHeartbeatUpdateInterval)
	}
	return &vreplicator{
		vre:             vre,
		id:              id,
		source:          source,
		sourceVStreamer: sourceVStreamer,
		stats:           stats,
		dbClient:        newVDBClient(dbClient, stats),
		mysqld:          mysqld,

		throttleUpdatesRateLimiter: timer.NewRateLimiter(time.Second),
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
			binlogplayer.LogError("Failed to set error state", err)
		}
	}
	return err
}

func (vr *vreplicator) replicate(ctx context.Context) error {
	// Manage SQL_MODE in the same way that mysqldump does.
	// Save the original sql_mode, set it to a permissive mode,
	// and then reset it back to the original value at the end.
	resetFunc, err := vr.setSQLMode(ctx)
	defer resetFunc()
	if err != nil {
		return err
	}

	colInfo, err := vr.buildColInfoMap(ctx)
	if err != nil {
		return err
	}
	vr.colInfoMap = colInfo
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
		// If any of the operations below changed state to Stopped or Error, we should return.
		if settings.State == binlogplayer.BlpStopped || settings.State == binlogplayer.BlpError {
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
			settings, numTablesToCopy, err = vr.readSettings(ctx)
			if err != nil {
				return err
			}
			if numTablesToCopy == 0 {
				if err := vr.insertLog(LogCopyEnd, fmt.Sprintf("Copy phase completed at gtid %s", settings.StartPos)); err != nil {
					return err
				}
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

// ColumnInfo is used to store charset and collation
type ColumnInfo struct {
	Name        string
	CharSet     string
	Collation   string
	DataType    string
	ColumnType  string
	IsPK        bool
	IsGenerated bool
}

func (vr *vreplicator) buildColInfoMap(ctx context.Context) (map[string][]*ColumnInfo, error) {
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{"/.*/"}}
	schema, err := vr.mysqld.GetSchema(ctx, vr.dbClient.DBName(), req)
	if err != nil {
		return nil, err
	}
	queryTemplate := "select character_set_name, collation_name, column_name, data_type, column_type, extra from information_schema.columns where table_schema=%s and table_name=%s;"
	colInfoMap := make(map[string][]*ColumnInfo)
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
			// Use the PK
			pks = td.PrimaryKeyColumns
		} else {
			// Use a PK equivalent if one exists
			if pks, err = vr.mysqld.GetPrimaryKeyEquivalentColumns(ctx, vr.dbClient.DBName(), td.Name); err != nil {
				return nil, err
			}
			// Fall back to using every column in the table if there's no PK or PKE
			if len(pks) == 0 {
				pks = td.Columns
			}
		}
		var colInfo []*ColumnInfo
		for _, row := range qr.Rows {
			charSet := ""
			collation := ""
			columnName := ""
			isPK := false
			isGenerated := false
			var dataType, columnType string
			columnName = row[2].ToString()
			var currentField *querypb.Field
			for _, field := range td.Fields {
				if field.Name == columnName {
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
			if dataType == "" || columnType == "" {
				return nil, fmt.Errorf("no dataType/columnType found in information_schema.columns for table %s, column %s", td.Name, columnName)
			}
			for _, pk := range pks {
				if columnName == pk {
					isPK = true
				}
			}
			extra := strings.ToLower(row[5].ToString())
			if strings.Contains(extra, "stored generated") || strings.Contains(extra, "virtual generated") {
				isGenerated = true
			}
			colInfo = append(colInfo, &ColumnInfo{
				Name:        columnName,
				CharSet:     charSet,
				Collation:   collation,
				DataType:    dataType,
				ColumnType:  columnType,
				IsPK:        isPK,
				IsGenerated: isGenerated,
			})
		}
		colInfoMap[td.Name] = colInfo
	}
	return colInfoMap, nil
}

func (vr *vreplicator) readSettings(ctx context.Context) (settings binlogplayer.VRSettings, numTablesToCopy int64, err error) {
	settings, err = binlogplayer.ReadVRSettings(vr.dbClient, vr.id)
	if err != nil {
		return settings, numTablesToCopy, fmt.Errorf("error reading VReplication settings: %v", err)
	}

	query := fmt.Sprintf("select count(distinct table_name) from _vt.copy_state where vrepl_id=%d", vr.id)
	qr, err := withDDL.Exec(ctx, query, vr.dbClient.ExecuteFetch, vr.dbClient.ExecuteFetch)
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
	vr.WorkflowType = int32(settings.WorkflowType)
	vr.WorkflowName = settings.WorkflowName
	return settings, numTablesToCopy, nil
}

func (vr *vreplicator) setMessage(message string) error {
	message = binlogplayer.MessageTruncate(message)
	vr.stats.History.Add(&binlogplayer.StatsHistoryRecord{
		Time:    time.Now(),
		Message: message,
	})
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("update _vt.vreplication set message=%s where id=%s", encodeString(message), strconv.Itoa(int(vr.id)))
	query := buf.ParsedQuery().Query
	if _, err := vr.dbClient.Execute(query); err != nil {
		return fmt.Errorf("could not set message: %v: %v", query, err)
	}
	if err := insertLog(vr.dbClient, LogMessage, vr.id, vr.state, message); err != nil {
		return err
	}
	return nil
}

func (vr *vreplicator) insertLog(typ, message string) error {
	return insertLog(vr.dbClient, typ, vr.id, vr.state, message)
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
	if state == vr.state {
		return nil
	}
	if err := insertLog(vr.dbClient, LogStateChange, vr.id, state, message); err != nil {
		return err
	}
	vr.state = state

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

func (vr *vreplicator) setSQLMode(ctx context.Context) (func(), error) {
	resetFunc := func() {}
	// First save the original SQL mode if we have not already done so
	if vr.originalSQLMode == "" {
		res, err := vr.dbClient.Execute(getSQLModeQuery)
		if err != nil || len(res.Rows) != 1 {
			return resetFunc, fmt.Errorf("could not get the original sql_mode on target: %v", err)
		}
		vr.originalSQLMode = res.Named().Row().AsString("sql_mode", "")
	}

	// Create a callback function for resetting the original
	// SQL mode back at the end of the vreplication operation.
	// You should defer this callback wherever you call setSQLMode()
	resetFunc = func() {
		query := fmt.Sprintf(setSQLModeQueryf, vr.originalSQLMode)
		_, err := vr.dbClient.Execute(query)
		if err != nil {
			log.Warningf("could not reset sql_mode on target using %s: %v", query, err)
		}
	}
	vreplicationSQLMode := SQLMode
	settings, _, err := vr.readSettings(ctx)
	if err != nil {
		return resetFunc, err
	}
	if settings.WorkflowType == int32(binlogdatapb.VReplicationWorkflowType_OnlineDDL) {
		vreplicationSQLMode = StrictSQLMode
	}

	// Now set it to a permissive mode that will allow us to recreate
	// any database object that exists on the source in full on the
	// target
	query := fmt.Sprintf(setSQLModeQueryf, vreplicationSQLMode)
	if _, err := vr.dbClient.Execute(query); err != nil {
		return resetFunc, fmt.Errorf("could not set the permissive sql_mode on target using %s: %v", query, err)
	}

	return resetFunc, nil
}

// throttlerAppName returns the app name to be used by throttlerClient for this particular workflow
// example results:
//   - "vreplication" for most flows
//   - "vreplication:online-ddl" for online ddl flows.
//     Note that with such name, it's possible to throttle
//     the worflow by either /throttler/throttle-app?app=vreplication and/or /throttler/throttle-app?app=online-ddl
//     This is useful when we want to throttle all migrations. We throttle "online-ddl" and that applies to both vreplication
//     migrations as well as gh-ost migrations.
func (vr *vreplicator) throttlerAppName() string {
	names := []string{vr.WorkflowName, throttlerVReplicationAppName}
	if vr.WorkflowType == int32(binlogdatapb.VReplicationWorkflowType_OnlineDDL) {
		names = append(names, throttlerOnlineDDLAppName)
	}
	return strings.Join(names, ":")
}

func (vr *vreplicator) updateTimeThrottled(componentThrottled ComponentName) error {
	err := vr.throttleUpdatesRateLimiter.Do(func() error {
		tm := time.Now().Unix()
		update, err := binlogplayer.GenerateUpdateTimeThrottled(vr.id, tm, string(componentThrottled))
		if err != nil {
			return err
		}
		if _, err := withDDL.Exec(vr.vre.ctx, update, vr.dbClient.ExecuteFetch, vr.dbClient.ExecuteFetch); err != nil {
			return fmt.Errorf("error %v updating time throttled", err)
		}
		return nil
	})
	return err
}

func (vr *vreplicator) updateHeartbeatTime(tm int64) error {
	update, err := binlogplayer.GenerateUpdateHeartbeat(vr.id, tm)
	if err != nil {
		return err
	}
	if _, err := withDDL.Exec(vr.vre.ctx, update, vr.dbClient.ExecuteFetch, vr.dbClient.ExecuteFetch); err != nil {
		return fmt.Errorf("error %v updating time", err)
	}
	return nil
}

func (vr *vreplicator) clearFKCheck() error {
	_, err := vr.dbClient.Execute("set foreign_key_checks=0;")
	return err
}

func recalculatePKColsInfoByColumnNames(uniqueKeyColumnNames []string, colInfos []*ColumnInfo) (pkColInfos []*ColumnInfo) {
	pkColInfos = colInfos[:]
	columnOrderMap := map[string]int64{}
	for _, colInfo := range pkColInfos {
		columnOrderMap[colInfo.Name] = math.MaxInt64
	}

	isPKMap := map[string]bool{}
	for i, colName := range uniqueKeyColumnNames {
		columnOrderMap[colName] = int64(i)
		isPKMap[colName] = true
	}
	sort.SliceStable(pkColInfos, func(i, j int) bool { return columnOrderMap[pkColInfos[i].Name] < columnOrderMap[pkColInfos[j].Name] })
	for i := range pkColInfos {
		pkColInfos[i].IsPK = isPKMap[pkColInfos[i].Name]
	}
	return pkColInfos
}
