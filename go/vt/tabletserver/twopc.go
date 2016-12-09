// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/sqlparser"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	sqlTurnoffBinlog   = "set @@session.sql_log_bin = 0"
	sqlCreateSidecarDB = "create database if not exists `%s`"

	sqlCreateTableRedoLogTransaction = `create table if not exists ` + "`%s`" + `.redo_log_transaction(
  dtid varbinary(512),
  state enum('Prepared', 'Resolved'),
  resolution enum ('Committed', 'RolledBack'),
  time_created bigint,
  primary key(dtid),
  index state_time_idx(state, time_created)
	) engine=InnoDB`

	// Due to possible legacy issues, do not modify the create
	// tablet statement. Alter it instead.
	sqlAlterTableRedoLogTransaction = `alter table ` + "`%s`" + `.redo_log_transaction
	modify state enum('Prepared', 'Failed')`

	sqlCreateTableRedoLogStatement = `create table if not exists ` + "`%s`" + `.redo_log_statement(
  dtid varbinary(512),
  id bigint,
  statement mediumblob,
  primary key(dtid, id)
	) engine=InnoDB`

	sqlCreateTableTransaction = `create table if not exists ` + "`%s`" + `.transaction(
  dtid varbinary(512),
  state enum('Prepare', 'Commit', 'Rollback'),
  time_created bigint,
  time_updated bigint,
  primary key(dtid)
	) engine=InnoDB`

	sqlCreateTableParticipant = `create table if not exists ` + "`%s`" + `.participant(
  dtid varbinary(512),
	id bigint,
	keyspace varchar(256),
	shard varchar(256),
  primary key(dtid, id)
	) engine=InnoDB`

	sqlReadAllRedo = `select t.dtid, t.state, t.time_created, s.statement
	from ` + "`%s`" + `.redo_log_transaction t
  join ` + "`%s`" + `.redo_log_statement s on t.dtid = s.dtid
	order by t.dtid, s.id`

	sqlReadAllTransactions = `select t.dtid, t.state, t.time_created, p.keyspace, p.shard
	from ` + "`%s`" + `.transaction t
  join ` + "`%s`" + `.participant p on t.dtid = p.dtid
	order by t.dtid, p.id`
)

// TwoPC performs 2PC metadata management (MM) functions.
type TwoPC struct {
	readPool *ConnPool

	insertRedoTx   *sqlparser.ParsedQuery
	insertRedoStmt *sqlparser.ParsedQuery
	updateRedoTx   *sqlparser.ParsedQuery
	deleteRedoTx   *sqlparser.ParsedQuery
	deleteRedoStmt *sqlparser.ParsedQuery
	readAllRedo    string

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
func NewTwoPC(readPool *ConnPool) *TwoPC {
	return &TwoPC{readPool: readPool}
}

// Init initializes TwoPC. If the metadata database or tables
// are not present, they are created.
func (tpc *TwoPC) Init(sidecarDBName string, dbaparams *sqldb.ConnParams) error {
	conn, err := dbconnpool.NewDBConnection(dbaparams, stats.NewTimings(""))
	if err != nil {
		return err
	}
	defer conn.Close()
	statements := []string{
		sqlTurnoffBinlog,
		fmt.Sprintf(sqlCreateSidecarDB, sidecarDBName),
		fmt.Sprintf(sqlCreateTableRedoLogTransaction, sidecarDBName),
		fmt.Sprintf(sqlAlterTableRedoLogTransaction, sidecarDBName),
		fmt.Sprintf(sqlCreateTableRedoLogStatement, sidecarDBName),
		fmt.Sprintf(sqlCreateTableTransaction, sidecarDBName),
		fmt.Sprintf(sqlCreateTableParticipant, sidecarDBName),
	}
	for _, s := range statements {
		if _, err := conn.ExecuteFetch(s, 0, false); err != nil {
			return NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err.Error())
		}
	}
	tpc.insertRedoTx = buildParsedQuery(
		"insert into `%s`.redo_log_transaction(dtid, state, time_created) values (%a, 'Prepared', %a)",
		sidecarDBName, ":dtid", ":time_created")
	tpc.insertRedoStmt = buildParsedQuery(
		"insert into `%s`.redo_log_statement(dtid, id, statement) values %a",
		sidecarDBName, ":vals")
	tpc.updateRedoTx = buildParsedQuery(
		"update `%s`.redo_log_transaction set state = %a where dtid = %a",
		sidecarDBName, ":state", ":dtid")
	tpc.deleteRedoTx = buildParsedQuery(
		"delete from `%s`.redo_log_transaction where dtid = %a",
		sidecarDBName, ":dtid")
	tpc.deleteRedoStmt = buildParsedQuery(
		"delete from `%s`.redo_log_statement where dtid = %a",
		sidecarDBName, ":dtid")
	tpc.readAllRedo = fmt.Sprintf(sqlReadAllRedo, sidecarDBName, sidecarDBName)

	tpc.insertTransaction = buildParsedQuery(
		"insert into `%s`.transaction(dtid, state, time_created, time_updated) values (%a, 'Prepare', %a, %a)",
		sidecarDBName, ":dtid", ":cur_time", ":cur_time")
	tpc.insertParticipants = buildParsedQuery(
		"insert into `%s`.participant(dtid, id, keyspace, shard) values %a",
		sidecarDBName, ":vals")
	tpc.transition = buildParsedQuery(
		"update `%s`.transaction set state = %a where dtid = %a and state = 'Prepare'",
		sidecarDBName, ":state", ":dtid")
	tpc.deleteTransaction = buildParsedQuery(
		"delete from `%s`.transaction where dtid = %a",
		sidecarDBName, ":dtid")
	tpc.deleteParticipants = buildParsedQuery(
		"delete from `%s`.participant where dtid = %a",
		sidecarDBName, ":dtid")
	tpc.readTransaction = buildParsedQuery(
		"select dtid, state, time_created, time_updated from `%s`.transaction where dtid = %a",
		sidecarDBName, ":dtid")
	tpc.readParticipants = buildParsedQuery(
		"select keyspace, shard from `%s`.participant where dtid = %a",
		sidecarDBName, ":dtid")
	tpc.readAbandoned = buildParsedQuery(
		"select dtid, time_created from `%s`.transaction where time_created < %a",
		sidecarDBName, ":time_created")
	tpc.readAllTransactions = fmt.Sprintf(sqlReadAllTransactions, sidecarDBName, sidecarDBName)
	return nil
}

// Open starts the TwoPC service.
func (tpc *TwoPC) Open(dbconfigs dbconfigs.DBConfigs) {
	tpc.readPool.Open(&dbconfigs.App, &dbconfigs.Dba)
}

// Close closes the TwoPC service.
func (tpc *TwoPC) Close() {
	tpc.readPool.Close()
}

func buildParsedQuery(in string, vars ...interface{}) *sqlparser.ParsedQuery {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(in, vars...)
	return buf.ParsedQuery()
}

// SaveRedo saves the statements in the redo log using the supplied connection.
func (tpc *TwoPC) SaveRedo(ctx context.Context, conn *TxConnection, dtid string, queries []string) error {
	bindVars := map[string]interface{}{
		"dtid":         dtid,
		"time_created": int64(time.Now().UnixNano()),
	}
	_, err := tpc.exec(ctx, conn, tpc.insertRedoTx, bindVars)
	if err != nil {
		return err
	}

	rows := make([][]sqltypes.Value, len(queries))
	for i, query := range queries {
		rows[i] = []sqltypes.Value{
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(dtid)),
			sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, int64(i+1), 10)),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(query)),
		}
	}
	bindVars = map[string]interface{}{
		"vals": rows,
	}
	_, err = tpc.exec(ctx, conn, tpc.insertRedoStmt, bindVars)
	return err
}

// UpdateRedo changes the state of the redo log for the dtid.
func (tpc *TwoPC) UpdateRedo(ctx context.Context, conn *TxConnection, dtid, state string) error {
	bindVars := map[string]interface{}{
		"dtid":  sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(dtid)),
		"state": sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(state)),
	}
	_, err := tpc.exec(ctx, conn, tpc.updateRedoTx, bindVars)
	return err
}

// DeleteRedo deletes the redo log for the dtid.
func (tpc *TwoPC) DeleteRedo(ctx context.Context, conn *TxConnection, dtid string) error {
	bindVars := map[string]interface{}{
		"dtid": sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(dtid)),
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
		dtid := row[0].String()
		if curTx == nil || dtid != curTx.Dtid {
			// Initialize the new element.
			v, _ := strconv.ParseInt(row[2].String(), 10, 64)
			curTx = &PreparedTx{
				Dtid: dtid,
				Time: time.Unix(0, v),
			}
			switch row[1].String() {
			case "Prepared":
				prepared = append(prepared, curTx)
			default:
				failed = append(failed, curTx)
			}
		}
		curTx.Queries = append(curTx.Queries, row[3].String())
	}
	return prepared, failed, nil
}

// CreateTransaction saves the metadata of a 2pc transaction as Prepared.
func (tpc *TwoPC) CreateTransaction(ctx context.Context, conn *TxConnection, dtid string, participants []*querypb.Target) error {
	bindVars := map[string]interface{}{
		"dtid":     dtid,
		"cur_time": int64(time.Now().UnixNano()),
	}
	_, err := tpc.exec(ctx, conn, tpc.insertTransaction, bindVars)
	if err != nil {
		return err
	}

	rows := make([][]sqltypes.Value, len(participants))
	for i, participant := range participants {
		rows[i] = []sqltypes.Value{
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(dtid)),
			sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, int64(i+1), 10)),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(participant.Keyspace)),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(participant.Shard)),
		}
	}
	bindVars = map[string]interface{}{
		"vals": rows,
	}
	_, err = tpc.exec(ctx, conn, tpc.insertParticipants, bindVars)
	return err
}

// Transition performs a transition from Prepare to the specified state.
// If the transaction is not a in the Prepare state, an error is returned.
func (tpc *TwoPC) Transition(ctx context.Context, conn *TxConnection, dtid, state string) error {
	bindVars := map[string]interface{}{
		"dtid":  dtid,
		"state": state,
	}
	qr, err := tpc.exec(ctx, conn, tpc.transition, bindVars)
	if err != nil {
		return err
	}
	if qr.RowsAffected != 1 {
		return NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "could not transition to %s: %s", state, dtid)
	}
	return nil
}

// DeleteTransaction deletes the metadata for the specified transaction.
func (tpc *TwoPC) DeleteTransaction(ctx context.Context, conn *TxConnection, dtid string) error {
	bindVars := map[string]interface{}{
		"dtid": dtid,
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
	bindVars := map[string]interface{}{
		"dtid": dtid,
	}
	qr, err := tpc.read(ctx, conn, tpc.readTransaction, bindVars)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return result, nil
	}
	result.Dtid = qr.Rows[0][0].String()
	switch qr.Rows[0][1].String() {
	case "Prepare":
		result.State = querypb.TransactionState_PREPARE
	case "Commit":
		result.State = querypb.TransactionState_COMMIT
	case "Rollback":
		result.State = querypb.TransactionState_ROLLBACK
	}
	v, err := qr.Rows[0][2].ParseInt64()
	if err != nil {
		return nil, err
	}
	result.TimeCreated = v
	v, err = qr.Rows[0][3].ParseInt64()
	if err != nil {
		return nil, err
	}
	result.TimeUpdated = v

	qr, err = tpc.read(ctx, conn, tpc.readParticipants, bindVars)
	if err != nil {
		return nil, err
	}
	participants := make([]*querypb.Target, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		participants = append(participants, &querypb.Target{
			Keyspace:   row[0].String(),
			Shard:      row[1].String(),
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

	bindVars := map[string]interface{}{
		"time_created": int64(abandonTime.UnixNano()),
	}
	qr, err := tpc.read(ctx, conn, tpc.readAbandoned, bindVars)
	if err != nil {
		return nil, err
	}
	txs := make(map[string]time.Time, len(qr.Rows))
	for _, row := range qr.Rows {
		t, err := strconv.ParseInt(row[1].String(), 10, 64)
		if err != nil {
			return nil, err
		}
		txs[row[0].String()] = time.Unix(0, t)
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
		dtid := row[0].String()
		if curTx == nil || dtid != curTx.Dtid {
			// Initialize the new element.
			v, _ := strconv.ParseInt(row[2].String(), 10, 64)
			curTx = &DistributedTx{
				Dtid:    dtid,
				State:   row[1].String(),
				Created: time.Unix(0, v),
			}
			distributed = append(distributed, curTx)
		}
		curTx.Participants = append(curTx.Participants, querypb.Target{
			Keyspace: row[3].String(),
			Shard:    row[4].String(),
		})
	}
	return distributed, nil
}

func (tpc *TwoPC) exec(ctx context.Context, conn *TxConnection, pq *sqlparser.ParsedQuery, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	b, err := pq.GenerateQuery(bindVars)
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, hack.String(b), 1, false)
}

func (tpc *TwoPC) read(ctx context.Context, conn *DBConn, pq *sqlparser.ParsedQuery, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	b, err := pq.GenerateQuery(bindVars)
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, hack.String(b), 10000, false)
}
