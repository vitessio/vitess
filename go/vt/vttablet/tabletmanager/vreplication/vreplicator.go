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
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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

	sqlCreatePostCopyAction = `insert into _vt.post_copy_action(vrepl_id, table_name, action)
	values(%a, %a, convert(%a using utf8mb4))`
	sqlGetPostCopyActions = `select id, action from _vt.post_copy_action where vrepl_id=%a and
	table_name=%a`
	// sqlGetPostCopyActionsForTable gets a write lock on all post_copy_action
	// rows for the table and should only be called from within an explicit
	// multi-statement transaction in order to hold those locks until the
	// related work is finished as this is a concurrency control mechanism.
	sqlGetAndLockPostCopyActionsForTable = `select id, vrepl_id, action from _vt.post_copy_action where id in
	(
		select pca.id from _vt.post_copy_action as pca inner join _vt.vreplication as vr on (pca.vrepl_id = vr.id)
		where pca.table_name=%a
	) for update`
	sqlGetPostCopyActionTaskByType = `select json_unquote(json_extract(action, '$.task')) as task from _vt.post_copy_action where
	json_unquote(json_extract(action, '$.type'))=%a and vrepl_id=%a and table_name=%a`
	sqlDeletePostCopyAction = `delete from _vt.post_copy_action where vrepl_id=%a and
	table_name=%a and id=%a`
)

// vreplicator provides the core logic to start vreplication streams
type vreplicator struct {
	vre      *Engine
	id       int32
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
// Keyspace, Shard, Filter, OnDdl, ExternalMySql and StopAfterCopy.
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
func newVReplicator(id int32, source *binlogdatapb.BinlogSource, sourceVStreamer VStreamerClient, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon, vre *Engine) *vreplicator {
	if vreplicationHeartbeatUpdateInterval > vreplicationMinimumHeartbeatUpdateInterval {
		log.Warningf("The supplied value for vreplication_heartbeat_update_interval:%d seconds is larger than the maximum allowed:%d seconds, vreplication will fallback to %d",
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

// We do not support "minimal" at the moment. "noblob" will provide significant performance improvements. Implementing
// "minimal" will result in a lot of edge cases which will not work, in Online DDL and Materialize. We will be
// soon supporting MySQL binlog compression which should provide some benefits similar to "minimal" in terms of storage
// and performance.
// To start with, we only allow "noblob" for MoveTables, Reshard and Online DDL. We need to identify edge cases for
// other workflow types like Materialize and add validations before we open it up for all workflow types.
func (vr *vreplicator) validateBinlogRowImage() error {
	rs, err := vr.dbClient.Execute("select @@binlog_row_image")
	if err != nil {
		return err
	}
	if len(rs.Rows) != 1 {
		return vterrors.New(vtrpcpb.Code_INTERNAL, fmt.Sprintf("'select @@binlog_row_image' returns an invalid result: %+v", rs.Rows))
	}

	binlogRowImage := strings.ToLower(rs.Rows[0][0].ToString())
	switch binlogRowImage {
	case "full":
	case "noblob":
		switch binlogdatapb.VReplicationWorkflowType(vr.WorkflowType) {
		case binlogdatapb.VReplicationWorkflowType_MoveTables,
			binlogdatapb.VReplicationWorkflowType_Reshard,
			binlogdatapb.VReplicationWorkflowType_OnlineDDL:
		case 0:
		// used in unit tests only
		default:
			return vterrors.New(vtrpcpb.Code_INTERNAL,
				fmt.Sprintf("noblob binlog_row_image is not supported for %s", binlogdatapb.VReplicationWorkflowType_name[vr.WorkflowType]))
		}
	default:
		return vterrors.New(vtrpcpb.Code_INTERNAL, fmt.Sprintf("%s binlog_row_image is not supported by Vitess VReplication", binlogRowImage))
	}
	return nil
}

func (vr *vreplicator) replicate(ctx context.Context) error {
	// Manage SQL_MODE in the same way that mysqldump does.
	// Save the original sql_mode, set it to a permissive mode,
	// and then reset it back to the original value at the end.
	resetFunc, err := vr.setSQLMode(ctx, vr.dbClient)
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
	defer vr.resetFKCheckAfterCopy(vr.dbClient)

	vr.throttleUpdatesRateLimiter = timer.NewRateLimiter(time.Second)
	defer vr.throttleUpdatesRateLimiter.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		// This rollback is a no-op. It's here for safety
		// in case the functions below leave transactions open.
		vr.dbClient.Rollback()

		settings, numTablesToCopy, err := vr.loadSettings(ctx, vr.dbClient)
		if err != nil {
			return err
		}

		if err := vr.validateBinlogRowImage(); err != nil {
			return err
		}

		// If any of the operations below changed state to Stopped or Error, we should return.
		if settings.State == binlogplayer.BlpStopped || settings.State == binlogplayer.BlpError {
			return nil
		}
		switch {
		case numTablesToCopy != 0:
			if err := vr.clearFKCheck(vr.dbClient); err != nil {
				log.Warningf("Unable to clear FK check %v", err)
				return err
			}
			if err := newVCopier(vr).copyNext(ctx, settings); err != nil {
				vr.stats.ErrorCounts.Add([]string{"Copy"}, 1)
				return err
			}
			settings, numTablesToCopy, err = vr.loadSettings(ctx, vr.dbClient)
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
			if err := vr.resetFKCheckAfterCopy(vr.dbClient); err != nil {
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
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{"/.*/"}, ExcludeTables: []string{"/" + schema.GCTableNameExpression + "/"}}
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

// Same as readSettings, but stores some of the results on this vr.
func (vr *vreplicator) loadSettings(ctx context.Context, dbClient *vdbClient) (settings binlogplayer.VRSettings, numTablesToCopy int64, err error) {
	settings, numTablesToCopy, err = vr.readSettings(ctx, dbClient)
	if err == nil {
		vr.WorkflowType = int32(settings.WorkflowType)
		vr.WorkflowName = settings.WorkflowName
	}
	return settings, numTablesToCopy, err
}

func (vr *vreplicator) readSettings(ctx context.Context, dbClient *vdbClient) (settings binlogplayer.VRSettings, numTablesToCopy int64, err error) {
	settings, err = binlogplayer.ReadVRSettings(dbClient, vr.id)
	if err != nil {
		return settings, numTablesToCopy, fmt.Errorf("error reading VReplication settings: %v", err)
	}

	query := fmt.Sprintf("select count(distinct table_name) from _vt.copy_state where vrepl_id=%d", vr.id)
	qr, err := vr.dbClient.ExecuteFetch(query, maxRows)
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
	vr.stats.State.Store(state)
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

func (vr *vreplicator) resetFKCheckAfterCopy(dbClient *vdbClient) error {
	_, err := dbClient.Execute(fmt.Sprintf("set foreign_key_checks=%d;", vr.originalFKCheckSetting))
	return err
}

func (vr *vreplicator) setSQLMode(ctx context.Context, dbClient *vdbClient) (func(), error) {
	resetFunc := func() {}
	// First save the original SQL mode if we have not already done so
	if vr.originalSQLMode == "" {
		res, err := dbClient.Execute(getSQLModeQuery)
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
		_, err := dbClient.Execute(query)
		if err != nil {
			log.Warningf("Could not reset sql_mode on target using %s: %v", query, err)
		}
	}
	vreplicationSQLMode := SQLMode
	settings, _, err := vr.readSettings(ctx, dbClient)
	if err != nil {
		return resetFunc, err
	}
	if settings.WorkflowType == binlogdatapb.VReplicationWorkflowType_OnlineDDL {
		vreplicationSQLMode = StrictSQLMode
	}

	// Now set it to a permissive mode that will allow us to recreate
	// any database object that exists on the source in full on the
	// target
	query := fmt.Sprintf(setSQLModeQueryf, vreplicationSQLMode)
	if _, err := dbClient.Execute(query); err != nil {
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
	names := []string{vr.WorkflowName, throttlerapp.VReplicationName.String()}
	if vr.WorkflowType == int32(binlogdatapb.VReplicationWorkflowType_OnlineDDL) {
		names = append(names, throttlerapp.OnlineDDLName.String())
	}
	return throttlerapp.Concatenate(names...)
}

func (vr *vreplicator) updateTimeThrottled(appThrottled throttlerapp.Name) error {
	err := vr.throttleUpdatesRateLimiter.Do(func() error {
		tm := time.Now().Unix()
		update, err := binlogplayer.GenerateUpdateTimeThrottled(vr.id, tm, appThrottled.String())
		if err != nil {
			return err
		}
		if _, err := vr.dbClient.ExecuteFetch(update, maxRows); err != nil {
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
	if _, err := vr.dbClient.ExecuteFetch(update, maxRows); err != nil {
		return fmt.Errorf("error %v updating time", err)
	}
	return nil
}

func (vr *vreplicator) clearFKCheck(dbClient *vdbClient) error {
	_, err := dbClient.Execute("set foreign_key_checks=0;")
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

// stashSecondaryKeys temporarily DROPs all secondary keys from the table schema
// and stashes an ALTER TABLE statement that will be used to recreate them at the
// end of the copy phase.
func (vr *vreplicator) stashSecondaryKeys(ctx context.Context, tableName string) error {
	if !vr.supportsDeferredSecondaryKeys() {
		return fmt.Errorf("deferring secondary key creation is not supported for %s workflows",
			binlogdatapb.VReplicationWorkflowType_name[vr.WorkflowType])
	}
	secondaryKeys, err := vr.getTableSecondaryKeys(ctx, tableName)
	if err != nil {
		return err
	}
	if len(secondaryKeys) > 0 {
		alterDrop := &sqlparser.AlterTable{
			Table: sqlparser.TableName{
				Qualifier: sqlparser.NewIdentifierCS(vr.dbClient.DBName()),
				Name:      sqlparser.NewIdentifierCS(tableName),
			},
		}
		alterReAdd := &sqlparser.AlterTable{
			Table: sqlparser.TableName{
				Qualifier: sqlparser.NewIdentifierCS(vr.dbClient.DBName()),
				Name:      sqlparser.NewIdentifierCS(tableName),
			},
		}
		for _, secondaryKey := range secondaryKeys {
			// Primary should never happen. Fulltext keys are
			// not supported for deferral and retained during
			// the copy phase as they have some unique
			// behaviors and constraints:
			// - Adding a fulltext key requires a full table
			//   rebuild to add the internal FTS_DOC_ID field
			//   to each record.
			// - You can not add/remove multiple fulltext keys
			//   in a single ALTER statement.
			if secondaryKey.Info.Primary || secondaryKey.Info.Fulltext {
				continue
			}
			alterDrop.AlterOptions = append(alterDrop.AlterOptions,
				&sqlparser.DropKey{
					Name: secondaryKey.Info.Name,
					Type: sqlparser.NormalKeyType,
				},
			)
			alterReAdd.AlterOptions = append(alterReAdd.AlterOptions,
				&sqlparser.AddIndexDefinition{
					IndexDefinition: secondaryKey,
				},
			)
		}
		action, err := json.Marshal(PostCopyAction{
			Type: PostCopyActionSQL,
			Task: sqlparser.String(alterReAdd),
		})
		if err != nil {
			return err
		}
		insert, err := sqlparser.ParseAndBind(sqlCreatePostCopyAction, sqltypes.Int32BindVariable(vr.id),
			sqltypes.StringBindVariable(tableName), sqltypes.StringBindVariable(string(action)))
		if err != nil {
			return err
		}
		// Use a new DB client to avoid interfering with open transactions
		// in the shared client as DDL includes an implied commit.
		// We're also NOT using a DBA connection here because we want to
		// be sure that the commit fails if the instance is somehow in
		// READ-ONLY mode.
		dbClient, err := vr.newClientConnection(ctx)
		if err != nil {
			log.Errorf("Unable to connect to the database when saving secondary keys for deferred creation on the %q table in the %q VReplication workflow: %v",
				tableName, vr.WorkflowName, err)
			return vterrors.Wrap(err, "unable to connect to the database when saving secondary keys for deferred creation")
		}
		defer dbClient.Close()
		if _, err := dbClient.ExecuteFetch(insert, 1); err != nil {
			return err
		}
		if _, err := dbClient.ExecuteFetch(sqlparser.String(alterDrop), 1); err != nil {
			// If they've already been dropped, e.g. by another controller running on the tablet
			// when doing a shard merge, then we can ignore the error.
			if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Num == mysql.ERCantDropFieldOrKey {
				secondaryKeys, err := vr.getTableSecondaryKeys(ctx, tableName)
				if err == nil && len(secondaryKeys) == 0 {
					return nil
				}
			}
			return err
		}
	}

	return nil
}

func (vr *vreplicator) getTableSecondaryKeys(ctx context.Context, tableName string) ([]*sqlparser.IndexDefinition, error) {
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{tableName}}
	schema, err := vr.mysqld.GetSchema(ctx, vr.dbClient.DBName(), req)
	if err != nil {
		return nil, err
	}
	// schema should never be nil, but check to be extra safe.
	if schema == nil || len(schema.TableDefinitions) != 1 {
		return nil, fmt.Errorf("unexpected number of table definitions returned from GetSchema call for table %q: %d",
			tableName, len(schema.TableDefinitions))
	}
	tableSchema := schema.TableDefinitions[0].Schema
	var secondaryKeys []*sqlparser.IndexDefinition
	parsedDDL, err := sqlparser.ParseStrictDDL(tableSchema)
	if err != nil {
		return secondaryKeys, err
	}
	createTable, ok := parsedDDL.(*sqlparser.CreateTable)
	// createTable or createTable.TableSpec should never be nil
	// if it was a valid cast, but check to be extra safe.
	if !ok || createTable == nil || createTable.GetTableSpec() == nil {
		return nil, fmt.Errorf("could not determine CREATE TABLE statement from table schema %q", tableSchema)
	}

	for _, index := range createTable.GetTableSpec().Indexes {
		if !index.Info.Primary {
			secondaryKeys = append(secondaryKeys, index)
		}
	}
	return secondaryKeys, err
}

func (vr *vreplicator) execPostCopyActions(ctx context.Context, tableName string) error {
	defer vr.stats.PhaseTimings.Record("postCopyActions", time.Now())

	// Use a new DB client to avoid interfering with open transactions
	// in the shared client as DDL includes an implied commit.
	// We're also NOT using a DBA connection here because we want to be
	// sure that the work fails if the instance is somehow in READ-ONLY
	// mode.
	dbClient, err := vr.newClientConnection(ctx)
	if err != nil {
		log.Errorf("Unable to connect to the database when executing post copy actions on the %q table in the %q VReplication workflow: %v",
			tableName, vr.WorkflowName, err)
		return vterrors.Wrap(err, "unable to connect to the database when executing post copy actions")
	}
	defer dbClient.Close()

	query, err := sqlparser.ParseAndBind(sqlGetPostCopyActions, sqltypes.Int32BindVariable(vr.id),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return err
	}
	qr, err := dbClient.ExecuteFetch(query, -1)
	if err != nil {
		return err
	}
	// qr should never be nil, but check anyway to be extra safe.
	if qr == nil || len(qr.Rows) == 0 {
		return nil
	}

	if err := vr.insertLog(LogCopyStart, fmt.Sprintf("Executing %d post copy action(s) for %s table",
		len(qr.Rows), tableName)); err != nil {
		return err
	}

	// Save our connection ID so we can use it to easily KILL any
	// running SQL action we may perform later if needed.
	idqr, err := dbClient.ExecuteFetch("select connection_id()", 1)
	if err != nil {
		return err
	}
	// qr should never be nil, but check anyway to be extra safe.
	if idqr == nil || len(idqr.Rows) != 1 {
		return fmt.Errorf("unexpected number of rows returned (%d) from connection_id() query", len(idqr.Rows))
	}
	connID, err := idqr.Rows[0][0].ToInt64()
	if err != nil || connID == 0 {
		return fmt.Errorf("unexpected result (%d) from connection_id() query, error: %v", connID, err)
	}

	deleteAction := func(dbc *vdbClient, id int64, vid int32, tn string) error {
		delq, err := sqlparser.ParseAndBind(sqlDeletePostCopyAction, sqltypes.Int32BindVariable(vid),
			sqltypes.StringBindVariable(tn), sqltypes.Int64BindVariable(id))
		if err != nil {
			return err
		}
		if _, err := dbc.ExecuteFetch(delq, 1); err != nil {
			return fmt.Errorf("failed to delete post copy action for the %q table with id %d: %v",
				tableName, id, err)
		}
		return nil
	}

	// This could take hours so we start a monitoring goroutine to
	// listen for context cancellation, which would indicate that
	// the controller is stopping due to engine shutdown (tablet
	// shutdown or transition). If that happens we attempt to KILL
	// the running ALTER statement using a DBA connection.
	// If we don't do this then we could e.g. cause a PRS to fail as
	// the running ALTER will block setting [super_]read_only.
	// A failed/killed ALTER will be tried again when the copy
	// phase starts up again on the (new) PRIMARY.
	var action PostCopyAction
	done := make(chan struct{})
	defer close(done)
	killAction := func(ak PostCopyAction) error {
		// If we're executing an SQL query then KILL the
		// connection being used to execute it.
		if ak.Type == PostCopyActionSQL {
			if connID < 1 {
				return fmt.Errorf("invalid connection ID found (%d) when attempting to kill %q", connID, ak.Task)
			}
			killdbc := vr.vre.dbClientFactoryDba()
			if err := killdbc.Connect(); err != nil {
				return fmt.Errorf("unable to connect to the database when attempting to kill %q: %v", ak.Task, err)
			}
			defer killdbc.Close()
			_, err = killdbc.ExecuteFetch(fmt.Sprintf("kill %d", connID), 1)
			return err
		}
		// We may support non-SQL actions in the future.
		return nil
	}
	go func() {
		select {
		// Only cancel an ongoing ALTER if the engine is closing.
		case <-vr.vre.ctx.Done():
			log.Infof("Copy of the %q table stopped when performing the following post copy action in the %q VReplication workflow: %+v",
				tableName, vr.WorkflowName, action)
			if err := killAction(action); err != nil {
				log.Errorf("Failed to kill post copy action on the %q table in the %q VReplication workflow: %v",
					tableName, vr.WorkflowName, err)
			}
			return
		case <-done:
			// We're done, so no longer need to listen for cancellation.
			return
		}
	}()

	for _, row := range qr.Named().Rows {
		select {
		// Stop any further actions if the vreplicator's context is
		// cancelled -- most likely due to hitting the
		// vreplication_copy_phase_duration
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
		}
		id, err := row["id"].ToInt64()
		if err != nil {
			return err
		}
		action = PostCopyAction{}
		actionBytes, err := row["action"].ToBytes()
		if err != nil {
			return err
		}
		if err := json.Unmarshal(actionBytes, &action); err != nil {
			return err
		}

		// There can be multiple vreplicators/controllers running on
		// the same tablet for the same table e.g. when doing shard
		// merges. Let's check for that and if there are still others
		// that have not finished the copy phase for the table, with
		// the same action, then we skip executing it as an individual
		// action on a table should only be done by the last vreplicator
		// to finish. We use a transaction because we select matching
		// rows with FOR UPDATE in order to serialize the execution of
		// the post copy actions for the same workflow and table.
		// This ensures that the actions are only performed once after
		// all streams have completed the copy phase for the table.
		redundant := false
		_, err = dbClient.ExecuteFetch("start transaction", 1)
		if err != nil {
			return err
		}
		vrsq, err := sqlparser.ParseAndBind(sqlGetAndLockPostCopyActionsForTable, sqltypes.StringBindVariable(tableName))
		if err != nil {
			return err
		}
		vrsres, err := dbClient.ExecuteFetch(vrsq, -1)
		if err != nil {
			return fmt.Errorf("failed to get post copy actions for the %q table: %v", tableName, err)
		}
		if vrsres != nil && len(vrsres.Rows) > 1 {
			// We have more than one planned post copy action on the table.
			for _, row := range vrsres.Named().Rows {
				vrid, err := row["vrepl_id"].ToInt32()
				if err != nil {
					return err
				}
				ctlaction := row["action"].ToString()
				// Let's make sure that it's a different controller/vreplicator
				// and that the action is the same.
				if vrid != vr.id && strings.EqualFold(ctlaction, string(actionBytes)) {
					// We know that there's another controller/vreplicator yet
					// to finish its copy phase for the table and it will perform
					// the same action on the same table when it completes, so we
					// skip doing the action and simply delete our action record
					// to mark this controller/vreplicator's post copy action work
					// as being done for the table before it finishes the copy
					// phase for the table.
					if err := deleteAction(dbClient, id, vr.id, tableName); err != nil {
						return err
					}
					redundant = true
					break
				}
			}
		}
		_, err = dbClient.ExecuteFetch("commit", 1)
		if err != nil {
			return err
		}
		if redundant {
			// Skip this action as it will be executed by a later vreplicator.
			continue
		}

		switch action.Type {
		case PostCopyActionSQL:
			log.Infof("Executing post copy SQL action on the %q table in the %q VReplication workflow: %s",
				tableName, vr.WorkflowName, action.Task)
			// This will return an io.EOF / MySQL CRServerLost (errno 2013)
			// error if it is killed by the monitoring goroutine.
			if _, err := dbClient.ExecuteFetch(action.Task, -1); err != nil {
				failedAlterErr := err
				// It's possible that we previously executed the ALTER but
				// the subsequent DELETE of the post_copy_action record failed.
				// For example, the context could be cancelled in-between.
				// It's also possible that the user has modified the schema on
				// the target side.
				// If we get a duplicate key/index error then let's see if the
				// index definitions that we would have added already exist in
				// the table schema and if so move forward and delete the
				// post_copy_action record.
				if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.ERDupKeyName {
					stmt, err := sqlparser.ParseStrictDDL(action.Task)
					if err != nil {
						return failedAlterErr
					}
					alterStmt, ok := stmt.(*sqlparser.AlterTable)
					if !ok {
						return failedAlterErr
					}
					currentSKs, err := vr.getTableSecondaryKeys(ctx, tableName)
					if err != nil {
						return failedAlterErr
					}
					if len(currentSKs) < len(alterStmt.AlterOptions) {
						return failedAlterErr
					}
					for _, alterOption := range alterStmt.AlterOptions {
						addKey, ok := alterOption.(*sqlparser.AddIndexDefinition)
						if !ok {
							return failedAlterErr
						}
						found := false
						for _, currentSK := range currentSKs {
							if sqlparser.Equals.RefOfIndexDefinition(addKey.IndexDefinition, currentSK) {
								found = true
								break
							}
						}
						if !found {
							return failedAlterErr
						}
					}
					// All of the keys we wanted to add in the ALTER already
					// exist in the live table schema.
				} else {
					return failedAlterErr
				}
			}
			if err := deleteAction(dbClient, id, vr.id, tableName); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported post copy action type: %v", action.Type)
		}
	}

	if err := vr.insertLog(LogCopyStart, fmt.Sprintf("Completed all post copy actions for %s table",
		tableName)); err != nil {
		return err
	}

	return nil
}

// supportsDeferredSecondaryKeys tells you if related work should be done
// for the workflow. Deferring secondary index generation is only supported
// with MoveTables, Migrate, and Reshard.
func (vr *vreplicator) supportsDeferredSecondaryKeys() bool {
	return vr.WorkflowType == int32(binlogdatapb.VReplicationWorkflowType_MoveTables) ||
		vr.WorkflowType == int32(binlogdatapb.VReplicationWorkflowType_Migrate) ||
		vr.WorkflowType == int32(binlogdatapb.VReplicationWorkflowType_Reshard)
}

func (vr *vreplicator) newClientConnection(ctx context.Context) (*vdbClient, error) {
	dbc := vr.vre.dbClientFactoryFiltered()
	if err := dbc.Connect(); err != nil {
		return nil, vterrors.Wrap(err, "can't connect to database")
	}
	dbClient := newVDBClient(dbc, vr.stats)
	if _, err := vr.setSQLMode(ctx, dbClient); err != nil {
		return nil, vterrors.Wrap(err, "failed to set sql_mode")
	}
	if err := vr.clearFKCheck(dbClient); err != nil {
		return nil, vterrors.Wrap(err, "failed to clear foreign key check")
	}
	return dbClient, nil
}
