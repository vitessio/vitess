/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	sqlTurnoffBinlog   = "set @@session.sql_log_bin = 0"
	sqlCreateSidecarDB = "create database if not exists %s"

	sqlDropLegacy1 = "drop table if exists %s.redo_log_transaction"
	sqlDropLegacy2 = "drop table if exists %s.redo_log_statement"
	sqlDropLegacy3 = "drop table if exists %s.transaction"
	sqlDropLegacy4 = "drop table if exists %s.participant"

	// RedoStateFailed represents the Failed state for redo_state.
	RedoStateFailed = 0
	// RedoStatePrepared represents the Prepared state for redo_state.
	RedoStatePrepared       = 1
	sqlCreateTableRedoState = `create table if not exists %s.redo_state(
  dtid varbinary(512),
  state bigint,
  time_created bigint,
  primary key(dtid)
	) engine=InnoDB`

	sqlCreateTableRedoStatement = `create table if not exists %s.redo_statement(
  dtid varbinary(512),
  id bigint,
  statement mediumblob,
  primary key(dtid, id)
	) engine=InnoDB`

	// DTStatePrepare represents the PREPARE state for dt_state.
	DTStatePrepare = querypb.TransactionState_PREPARE
	// DTStateCommit represents the COMMIT state for dt_state.
	DTStateCommit = querypb.TransactionState_COMMIT
	// DTStateRollback represents the ROLLBACK state for dt_state.
	DTStateRollback       = querypb.TransactionState_ROLLBACK
	sqlCreateTableDTState = `create table if not exists %s.dt_state(
  dtid varbinary(512),
  state bigint,
  time_created bigint,
  primary key(dtid)
	) engine=InnoDB`

	sqlCreateTableDTParticipant = `create table if not exists %s.dt_participant(
  dtid varbinary(512),
	id bigint,
	keyspace varchar(256),
	shard varchar(256),
  primary key(dtid, id)
	) engine=InnoDB`

	sqlReadAllRedo = `select t.dtid, t.state, t.time_created, s.statement
	from %s.redo_state t
  join %s.redo_statement s on t.dtid = s.dtid
	order by t.dtid, s.id`

	sqlReadAllTransactions = `select t.dtid, t.state, t.time_created, p.keyspace, p.shard
	from %s.dt_state t
  join %s.dt_participant p on t.dtid = p.dtid
	order by t.dtid, p.id`
)

// TwoPC performs 2PC metadata management (MM) functions.
type TwoPC struct {
	readPool *connpool.Pool

	insertRedoTx        *sqlparser.ParsedQuery
	insertRedoStmt      *sqlparser.ParsedQuery
	updateRedoTx        *sqlparser.ParsedQuery
	deleteRedoTx        *sqlparser.ParsedQuery
	deleteRedoStmt      *sqlparser.ParsedQuery
	readAllRedo         string
	countUnresolvedRedo *sqlparser.ParsedQuery

	insertTransaction   *sqlparser.ParsedQuery
	insertParticipants  *sqlparser.ParsedQuery
	transition          *sqlparser.ParsedQuery
	deleteTransaction   *sqlparser.ParsedQuery
	deleteParticipants  *sqlparser.ParsedQuery
	readTransaction     *sqlparser.ParsedQuery
	readParticipants    *sqlparser.ParsedQuery
	readAbandoned       *sqlparser.ParsedQuery
	readAllTransactions string
}

// NewTwoPC creates a TwoPC variable.
func NewTwoPC(readPool *connpool.Pool) *TwoPC {
	return &TwoPC{readPool: readPool}
}

// Init initializes TwoPC. If the metadata database or tables
// are not present, they are created.
func (tpc *TwoPC) Init(sidecarDBName string, dbaparams *mysql.ConnParams) error {
	dbname := sqlparser.Backtick(sidecarDBName)
	conn, err := dbconnpool.NewDBConnection(dbaparams, stats.NewTimings(""))
	if err != nil {
		return err
	}
	defer conn.Close()
	statements := []string{
		sqlTurnoffBinlog,
		fmt.Sprintf(sqlCreateSidecarDB, dbname),
		fmt.Sprintf(sqlDropLegacy1, dbname),
		fmt.Sprintf(sqlDropLegacy2, dbname),
		fmt.Sprintf(sqlDropLegacy3, dbname),
		fmt.Sprintf(sqlDropLegacy4, dbname),
		fmt.Sprintf(sqlCreateTableRedoState, dbname),
		fmt.Sprintf(sqlCreateTableRedoStatement, dbname),
		fmt.Sprintf(sqlCreateTableDTState, dbname),
		fmt.Sprintf(sqlCreateTableDTParticipant, dbname),
	}
	for _, s := range statements {
		if _, err := conn.ExecuteFetch(s, 0, false); err != nil {
			return err
		}
	}
	tpc.insertRedoTx = sqlparser.BuildParsedQuery(
		"insert into %s.redo_state(dtid, state, time_created) values (%a, %a, %a)",
		dbname, ":dtid", ":state", ":time_created")
	tpc.insertRedoStmt = sqlparser.BuildParsedQuery(
		"insert into %s.redo_statement(dtid, id, statement) values %a",
		dbname, ":vals")
	tpc.updateRedoTx = sqlparser.BuildParsedQuery(
		"update %s.redo_state set state = %a where dtid = %a",
		dbname, ":state", ":dtid")
	tpc.deleteRedoTx = sqlparser.BuildParsedQuery(
		"delete from %s.redo_state where dtid = %a",
		dbname, ":dtid")
	tpc.deleteRedoStmt = sqlparser.BuildParsedQuery(
		"delete from %s.redo_statement where dtid = %a",
		dbname, ":dtid")
	tpc.readAllRedo = fmt.Sprintf(sqlReadAllRedo, dbname, dbname)
	tpc.countUnresolvedRedo = sqlparser.BuildParsedQuery(
		"select count(*) from %s.redo_state where time_created < %a",
		dbname, ":time_created")

	tpc.insertTransaction = sqlparser.BuildParsedQuery(
		"insert into %s.dt_state(dtid, state, time_created) values (%a, %a, %a)",
		dbname, ":dtid", ":state", ":cur_time")
	tpc.insertParticipants = sqlparser.BuildParsedQuery(
		"insert into %s.dt_participant(dtid, id, keyspace, shard) values %a",
		dbname, ":vals")
	tpc.transition = sqlparser.BuildParsedQuery(
		"update %s.dt_state set state = %a where dtid = %a and state = %a",
		dbname, ":state", ":dtid", ":prepare")
	tpc.deleteTransaction = sqlparser.BuildParsedQuery(
		"delete from %s.dt_state where dtid = %a",
		dbname, ":dtid")
	tpc.deleteParticipants = sqlparser.BuildParsedQuery(
		"delete from %s.dt_participant where dtid = %a",
		dbname, ":dtid")
	tpc.readTransaction = sqlparser.BuildParsedQuery(
		"select dtid, state, time_created from %s.dt_state where dtid = %a",
		dbname, ":dtid")
	tpc.readParticipants = sqlparser.BuildParsedQuery(
		"select keyspace, shard from %s.dt_participant where dtid = %a",
		dbname, ":dtid")
	tpc.readAbandoned = sqlparser.BuildParsedQuery(
		"select dtid, time_created from %s.dt_state where time_created < %a",
		dbname, ":time_created")
	tpc.readAllTransactions = fmt.Sprintf(sqlReadAllTransactions, dbname, dbname)
	return nil
}

// Open starts the TwoPC service.
func (tpc *TwoPC) Open(dbconfigs dbconfigs.DBConfigs) {
	tpc.readPool.Open(&dbconfigs.App, &dbconfigs.Dba, &dbconfigs.AppDebug)
}

// Close closes the TwoPC service.
func (tpc *TwoPC) Close() {
	tpc.readPool.Close()
}

// SaveRedo saves the statements in the redo log using the supplied connection.
func (tpc *TwoPC) SaveRedo(ctx context.Context, conn *TxConnection, dtid string, queries []string) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid":         sqltypes.StringBindVariable(dtid),
		"state":        sqltypes.Int64BindVariable(RedoStatePrepared),
		"time_created": sqltypes.Int64BindVariable(time.Now().UnixNano()),
	}
	_, err := tpc.exec(ctx, conn, tpc.insertRedoTx, bindVars)
	if err != nil {
		return err
	}

	rows := make([][]sqltypes.Value, len(queries))
	for i, query := range queries {
		rows[i] = []sqltypes.Value{
			sqltypes.NewVarBinary(dtid),
			sqltypes.NewInt64(int64(i + 1)),
			sqltypes.NewVarBinary(query),
		}
	}
	extras := map[string]sqlparser.Encodable{
		"vals": sqlparser.InsertValues(rows),
	}
	b, err := tpc.insertRedoStmt.GenerateQuery(nil, extras)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, hack.String(b), 1, false)
	return err
}

// UpdateRedo changes the state of the redo log for the dtid.
func (tpc *TwoPC) UpdateRedo(ctx context.Context, conn *TxConnection, dtid string, state int) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid":  sqltypes.StringBindVariable(dtid),
		"state": sqltypes.Int64BindVariable(int64(state)),
	}
	_, err := tpc.exec(ctx, conn, tpc.updateRedoTx, bindVars)
	return err
}

// DeleteRedo deletes the redo log for the dtid.
func (tpc *TwoPC) DeleteRedo(ctx context.Context, conn *TxConnection, dtid string) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid": sqltypes.StringBindVariable(dtid),
	}
	_, err := tpc.exec(ctx, conn, tpc.deleteRedoTx, bindVars)
	if err != nil {
		return err
	}
	_, err = tpc.exec(ctx, conn, tpc.deleteRedoStmt, bindVars)
	return err
}

// PreparedTx represents a displayable version of a prepared transaction.
type PreparedTx struct {
	Dtid    string
	Queries []string
	Time    time.Time
}

// ReadAllRedo returns all the prepared transactions from the redo logs.
func (tpc *TwoPC) ReadAllRedo(ctx context.Context) (prepared, failed []*PreparedTx, err error) {
	conn, err := tpc.readPool.Get(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Recycle()

	qr, err := conn.Exec(ctx, tpc.readAllRedo, 10000, false)
	if err != nil {
		return nil, nil, err
	}

	var curTx *PreparedTx
	for _, row := range qr.Rows {
		dtid := row[0].ToString()
		if curTx == nil || dtid != curTx.Dtid {
			// Initialize the new element.
			// A failure in time parsing will show up as a very old time,
			// which is harmless.
			tm, _ := sqltypes.ToInt64(row[2])
			curTx = &PreparedTx{
				Dtid: dtid,
				Time: time.Unix(0, tm),
			}
			st, err := sqltypes.ToInt64(row[1])
			if err != nil {
				log.Errorf("Error parsing state for dtid %s: %v.", dtid, err)
			}
			switch st {
			case RedoStatePrepared:
				prepared = append(prepared, curTx)
			default:
				if st != RedoStateFailed {
					log.Errorf("Unexpected state for dtid %s: %d. Treating it as a failure.", dtid, st)
				}
				failed = append(failed, curTx)
			}
		}
		curTx.Queries = append(curTx.Queries, row[3].ToString())
	}
	return prepared, failed, nil
}

// CountUnresolvedRedo returns the number of prepared transactions that are still unresolved.
func (tpc *TwoPC) CountUnresolvedRedo(ctx context.Context, unresolvedTime time.Time) (int64, error) {
	conn, err := tpc.readPool.Get(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Recycle()

	bindVars := map[string]*querypb.BindVariable{
		"time_created": sqltypes.Int64BindVariable(unresolvedTime.UnixNano()),
	}
	qr, err := tpc.read(ctx, conn, tpc.countUnresolvedRedo, bindVars)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) < 1 {
		return 0, nil
	}
	v, _ := sqltypes.ToInt64(qr.Rows[0][0])
	return v, nil
}

// CreateTransaction saves the metadata of a 2pc transaction as Prepared.
func (tpc *TwoPC) CreateTransaction(ctx context.Context, conn *TxConnection, dtid string, participants []*querypb.Target) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid":     sqltypes.StringBindVariable(dtid),
		"state":    sqltypes.Int64BindVariable(int64(DTStatePrepare)),
		"cur_time": sqltypes.Int64BindVariable(time.Now().UnixNano()),
	}
	_, err := tpc.exec(ctx, conn, tpc.insertTransaction, bindVars)
	if err != nil {
		return err
	}

	rows := make([][]sqltypes.Value, len(participants))
	for i, participant := range participants {
		rows[i] = []sqltypes.Value{
			sqltypes.NewVarBinary(dtid),
			sqltypes.NewInt64(int64(i + 1)),
			sqltypes.NewVarBinary(participant.Keyspace),
			sqltypes.NewVarBinary(participant.Shard),
		}
	}
	extras := map[string]sqlparser.Encodable{
		"vals": sqlparser.InsertValues(rows),
	}
	b, err := tpc.insertParticipants.GenerateQuery(nil, extras)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, hack.String(b), 1, false)
	return err
}

// Transition performs a transition from Prepare to the specified state.
// If the transaction is not a in the Prepare state, an error is returned.
func (tpc *TwoPC) Transition(ctx context.Context, conn *TxConnection, dtid string, state querypb.TransactionState) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid":    sqltypes.StringBindVariable(dtid),
		"state":   sqltypes.Int64BindVariable(int64(state)),
		"prepare": sqltypes.Int64BindVariable(int64(querypb.TransactionState_PREPARE)),
	}
	qr, err := tpc.exec(ctx, conn, tpc.transition, bindVars)
	if err != nil {
		return err
	}
	if qr.RowsAffected != 1 {
		return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "could not transition to %v: %s", state, dtid)
	}
	return nil
}

// DeleteTransaction deletes the metadata for the specified transaction.
func (tpc *TwoPC) DeleteTransaction(ctx context.Context, conn *TxConnection, dtid string) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid": sqltypes.StringBindVariable(dtid),
	}
	_, err := tpc.exec(ctx, conn, tpc.deleteTransaction, bindVars)
	if err != nil {
		return err
	}
	_, err = tpc.exec(ctx, conn, tpc.deleteParticipants, bindVars)
	return err
}

// ReadTransaction returns the metadata for the transaction.
func (tpc *TwoPC) ReadTransaction(ctx context.Context, dtid string) (*querypb.TransactionMetadata, error) {
	conn, err := tpc.readPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	result := &querypb.TransactionMetadata{}
	bindVars := map[string]*querypb.BindVariable{
		"dtid": sqltypes.StringBindVariable(dtid),
	}
	qr, err := tpc.read(ctx, conn, tpc.readTransaction, bindVars)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return result, nil
	}
	result.Dtid = qr.Rows[0][0].ToString()
	st, err := sqltypes.ToInt64(qr.Rows[0][1])
	if err != nil {
		return nil, fmt.Errorf("Error parsing state for dtid %s: %v", dtid, err)
	}
	result.State = querypb.TransactionState(st)
	if result.State < querypb.TransactionState_PREPARE || result.State > querypb.TransactionState_ROLLBACK {
		return nil, fmt.Errorf("Unexpected state for dtid %s: %v", dtid, result.State)
	}
	// A failure in time parsing will show up as a very old time,
	// which is harmless.
	tm, _ := sqltypes.ToInt64(qr.Rows[0][2])
	result.TimeCreated = tm

	qr, err = tpc.read(ctx, conn, tpc.readParticipants, bindVars)
	if err != nil {
		return nil, err
	}
	participants := make([]*querypb.Target, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		participants = append(participants, &querypb.Target{
			Keyspace:   row[0].ToString(),
			Shard:      row[1].ToString(),
			TabletType: topodatapb.TabletType_MASTER,
		})
	}
	result.Participants = participants
	return result, nil
}

// ReadAbandoned returns the list of abandoned transactions
// and their associated start time.
func (tpc *TwoPC) ReadAbandoned(ctx context.Context, abandonTime time.Time) (map[string]time.Time, error) {
	conn, err := tpc.readPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	bindVars := map[string]*querypb.BindVariable{
		"time_created": sqltypes.Int64BindVariable(abandonTime.UnixNano()),
	}
	qr, err := tpc.read(ctx, conn, tpc.readAbandoned, bindVars)
	if err != nil {
		return nil, err
	}
	txs := make(map[string]time.Time, len(qr.Rows))
	for _, row := range qr.Rows {
		t, err := sqltypes.ToInt64(row[1])
		if err != nil {
			return nil, err
		}
		txs[row[0].ToString()] = time.Unix(0, t)
	}
	return txs, nil
}

// DistributedTx is similar to querypb.TransactionMetadata, but
// is display friendly.
type DistributedTx struct {
	Dtid         string
	State        string
	Created      time.Time
	Participants []querypb.Target
}

// ReadAllTransactions returns info about all distributed transactions.
func (tpc *TwoPC) ReadAllTransactions(ctx context.Context) ([]*DistributedTx, error) {
	conn, err := tpc.readPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	qr, err := conn.Exec(ctx, tpc.readAllTransactions, 10000, false)
	if err != nil {
		return nil, err
	}

	var curTx *DistributedTx
	var distributed []*DistributedTx
	for _, row := range qr.Rows {
		dtid := row[0].ToString()
		if curTx == nil || dtid != curTx.Dtid {
			// Initialize the new element.
			// A failure in time parsing will show up as a very old time,
			// which is harmless.
			tm, _ := sqltypes.ToInt64(row[2])
			st, err := sqltypes.ToInt64(row[1])
			// Just log on error and continue. The state will show up as UNKNOWN
			// on the display.
			if err != nil {
				log.Errorf("Error parsing state for dtid %s: %v.", dtid, err)
			}
			protostate := querypb.TransactionState(st)
			if protostate < querypb.TransactionState_UNKNOWN || protostate > querypb.TransactionState_ROLLBACK {
				log.Errorf("Unexpected state for dtid %s: %v.", dtid, protostate)
			}
			curTx = &DistributedTx{
				Dtid:    dtid,
				State:   querypb.TransactionState(st).String(),
				Created: time.Unix(0, tm),
			}
			distributed = append(distributed, curTx)
		}
		curTx.Participants = append(curTx.Participants, querypb.Target{
			Keyspace: row[3].ToString(),
			Shard:    row[4].ToString(),
		})
	}
	return distributed, nil
}

func (tpc *TwoPC) exec(ctx context.Context, conn *TxConnection, pq *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	b, err := pq.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, hack.String(b), 1, false)
}

func (tpc *TwoPC) read(ctx context.Context, conn *connpool.DBConn, pq *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	b, err := pq.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, hack.String(b), 10000, false)
}
