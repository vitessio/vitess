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

package tabletserver

import (
	"context"
	"fmt"
	"time"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
)

const (
	// RedoStateFailed represents the Failed state for redo_state.
	RedoStateFailed = 0
	// RedoStatePrepared represents the Prepared state for redo_state.
	RedoStatePrepared = 1
	// DTStatePrepare represents the PREPARE state for dt_state.
	DTStatePrepare = querypb.TransactionState_PREPARE
	// DTStateCommit represents the COMMIT state for dt_state.
	DTStateCommit = querypb.TransactionState_COMMIT
	// DTStateRollback represents the ROLLBACK state for dt_state.
	DTStateRollback = querypb.TransactionState_ROLLBACK

	readAllRedo = `select t.dtid, t.state, t.time_created, s.statement
	from %s.redo_state t
    join %s.redo_statement s on t.dtid = s.dtid
	order by t.dtid, s.id`

	readAllTransactions = `select t.dtid, t.state, t.time_created, p.keyspace, p.shard
	from %s.dt_state t
    join %s.dt_participant p on t.dtid = p.dtid
	order by t.dtid, p.id`

	// Ordering by state in descending order to retrieve COMMIT, ROLLBACK, PREPARE in that sequence.
	// Resolving COMMIT first is crucial because we need to address transactions where a commit decision has already been made but remains unresolved.
	// For transactions with a commit decision, applications are already aware of the outcome and are waiting for the resolution.
	// By addressing these first, we ensure atomic commits and improve user experience. For other transactions, the decision is typically to rollback.
	readUnresolvedTransactions = `select t.dtid, t.state, p.keyspace, p.shard
	from %s.dt_state t
    join %s.dt_participant p on t.dtid = p.dtid
    where time_created < %a
	order by t.state desc, t.dtid`

	countUnresolvedTransactions = `select count(*) from %s.dt_state where time_created < %a`
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

	insertTransaction          *sqlparser.ParsedQuery
	insertParticipants         *sqlparser.ParsedQuery
	transition                 *sqlparser.ParsedQuery
	deleteTransaction          *sqlparser.ParsedQuery
	deleteParticipants         *sqlparser.ParsedQuery
	readTransaction            *sqlparser.ParsedQuery
	readParticipants           *sqlparser.ParsedQuery
	readUnresolvedTransactions *sqlparser.ParsedQuery
	readAllTransactions        string
	countUnresolvedTransaction *sqlparser.ParsedQuery
}

// NewTwoPC creates a TwoPC variable.
func NewTwoPC(readPool *connpool.Pool) *TwoPC {
	tpc := &TwoPC{readPool: readPool}
	return tpc
}

func (tpc *TwoPC) initializeQueries() {
	dbname := sidecar.GetIdentifier()
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
	tpc.readAllRedo = fmt.Sprintf(readAllRedo, dbname, dbname)
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
	tpc.readAllTransactions = fmt.Sprintf(readAllTransactions, dbname, dbname)
	tpc.readUnresolvedTransactions = sqlparser.BuildParsedQuery(readUnresolvedTransactions,
		dbname, dbname, ":time_created")
	tpc.countUnresolvedTransaction = sqlparser.BuildParsedQuery(countUnresolvedTransactions,
		dbname, ":time_created")
}

// Open starts the TwoPC service.
func (tpc *TwoPC) Open(dbconfigs *dbconfigs.DBConfigs) error {
	conn, err := dbconnpool.NewDBConnection(context.TODO(), dbconfigs.DbaWithDB())
	if err != nil {
		return err
	}
	defer conn.Close()
	tpc.readPool.Open(dbconfigs.AppWithDB(), dbconfigs.DbaWithDB(), dbconfigs.DbaWithDB())
	tpc.initializeQueries()
	log.Infof("TwoPC: Engine open succeeded")
	return nil
}

// Close closes the TwoPC service.
func (tpc *TwoPC) Close() {
	tpc.readPool.Close()
}

// SaveRedo saves the statements in the redo log using the supplied connection.
func (tpc *TwoPC) SaveRedo(ctx context.Context, conn *StatefulConnection, dtid string, queries []tx.Query) error {
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
			sqltypes.NewVarBinary(query.Sql),
		}
	}
	extras := map[string]sqlparser.Encodable{
		"vals": sqlparser.InsertValues(rows),
	}
	q, err := tpc.insertRedoStmt.GenerateQuery(nil, extras)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, q, 1, false)
	return err
}

// UpdateRedo changes the state of the redo log for the dtid.
func (tpc *TwoPC) UpdateRedo(ctx context.Context, conn *StatefulConnection, dtid string, state int) error {
	bindVars := map[string]*querypb.BindVariable{
		"dtid":  sqltypes.StringBindVariable(dtid),
		"state": sqltypes.Int64BindVariable(int64(state)),
	}
	_, err := tpc.exec(ctx, conn, tpc.updateRedoTx, bindVars)
	return err
}

// DeleteRedo deletes the redo log for the dtid.
func (tpc *TwoPC) DeleteRedo(ctx context.Context, conn *StatefulConnection, dtid string) error {
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

// ReadAllRedo returns all the prepared transactions from the redo logs.
func (tpc *TwoPC) ReadAllRedo(ctx context.Context) (prepared, failed []*tx.PreparedTx, err error) {
	conn, err := tpc.readPool.Get(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Recycle()

	qr, err := conn.Conn.Exec(ctx, tpc.readAllRedo, 10000, false)
	if err != nil {
		return nil, nil, err
	}

	var curTx *tx.PreparedTx
	for _, row := range qr.Rows {
		dtid := row[0].ToString()
		if curTx == nil || dtid != curTx.Dtid {
			// Initialize the new element.
			// A failure in time parsing will show up as a very old time,
			// which is harmless.
			tm, _ := row[2].ToCastInt64()
			curTx = &tx.PreparedTx{
				Dtid: dtid,
				Time: time.Unix(0, tm),
			}
			st, err := row[1].ToCastInt64()
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

// CountUnresolvedRedo returns the number of prepared transaction recovery log that are older than the supplied time.
func (tpc *TwoPC) CountUnresolvedRedo(ctx context.Context, unresolvedTime time.Time) (int64, error) {
	conn, err := tpc.readPool.Get(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer conn.Recycle()

	bindVars := map[string]*querypb.BindVariable{
		"time_created": sqltypes.Int64BindVariable(unresolvedTime.UnixNano()),
	}
	qr, err := tpc.read(ctx, conn.Conn, tpc.countUnresolvedRedo, bindVars)
	if err != nil {
		return 0, err
	}
	// executed query is a scalar aggregation, so we can safely assume that the result is a single row.
	v, _ := qr.Rows[0][0].ToCastInt64()
	return v, nil
}

// CreateTransaction saves the metadata of a 2pc transaction as Prepared.
func (tpc *TwoPC) CreateTransaction(ctx context.Context, conn *StatefulConnection, dtid string, participants []*querypb.Target) error {
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
	q, err := tpc.insertParticipants.GenerateQuery(nil, extras)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, q, 1, false)
	return err
}

// Transition performs a transition from Prepare to the specified state.
// If the transaction is not a in the Prepare state, an error is returned.
func (tpc *TwoPC) Transition(ctx context.Context, conn *StatefulConnection, dtid string, state querypb.TransactionState) error {
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
func (tpc *TwoPC) DeleteTransaction(ctx context.Context, conn *StatefulConnection, dtid string) error {
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
	conn, err := tpc.readPool.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	result := &querypb.TransactionMetadata{}
	bindVars := map[string]*querypb.BindVariable{
		"dtid": sqltypes.StringBindVariable(dtid),
	}
	qr, err := tpc.read(ctx, conn.Conn, tpc.readTransaction, bindVars)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return result, nil
	}
	result.Dtid = qr.Rows[0][0].ToString()
	st, err := qr.Rows[0][1].ToCastInt64()
	if err != nil {
		return nil, vterrors.Wrapf(err, "error parsing state for dtid %s", dtid)
	}
	result.State = querypb.TransactionState(st)
	if result.State < DTStatePrepare || result.State > DTStateCommit {
		return nil, fmt.Errorf("unexpected state for dtid %s: %v", dtid, result.State)
	}
	// A failure in time parsing will show up as a very old time,
	// which is harmless.
	tm, _ := qr.Rows[0][2].ToCastInt64()
	result.TimeCreated = tm

	qr, err = tpc.read(ctx, conn.Conn, tpc.readParticipants, bindVars)
	if err != nil {
		return nil, err
	}
	participants := make([]*querypb.Target, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		participants = append(participants, &querypb.Target{
			Keyspace:   row[0].ToString(),
			Shard:      row[1].ToString(),
			TabletType: topodatapb.TabletType_PRIMARY,
		})
	}
	result.Participants = participants
	return result, nil
}

// ReadAllTransactions returns info about all distributed transactions.
func (tpc *TwoPC) ReadAllTransactions(ctx context.Context) ([]*tx.DistributedTx, error) {
	conn, err := tpc.readPool.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	qr, err := conn.Conn.Exec(ctx, tpc.readAllTransactions, 10000, false)
	if err != nil {
		return nil, err
	}

	var curTx *tx.DistributedTx
	var distributed []*tx.DistributedTx
	for _, row := range qr.Rows {
		dtid := row[0].ToString()
		if curTx == nil || dtid != curTx.Dtid {
			// Initialize the new element.
			// A failure in time parsing will show up as a very old time,
			// which is harmless.
			tm, _ := row[2].ToCastInt64()
			st, err := row[1].ToCastInt64()
			// Just log on error and continue. The state will show up as UNKNOWN
			// on the display.
			if err != nil {
				log.Errorf("Error parsing state for dtid %s: %v.", dtid, err)
			}
			protostate := querypb.TransactionState(st)
			if protostate < DTStatePrepare || protostate > DTStateCommit {
				log.Errorf("Unexpected state for dtid %s: %v.", dtid, protostate)
			}
			curTx = &tx.DistributedTx{
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

func (tpc *TwoPC) exec(ctx context.Context, conn *StatefulConnection, pq *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	q, err := pq.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, q, 1, false)
}

func (tpc *TwoPC) read(ctx context.Context, conn *connpool.Conn, pq *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	q, err := pq.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	// TODO: check back for limit, 10k is too high already to have unresolved transactions.
	return conn.Exec(ctx, q, 10000, false)
}

// UnresolvedTransactions returns the list of unresolved transactions
// the list from database is retrieved as
// dtid | state   | keyspace | shard
// 1    | PREPARE | ks       | 40-80
// 1    | PREPARE | ks       | 80-c0
// 2    | COMMIT  | ks       | -40
// Here there are 2 dtids with 2 participants for dtid:1 and 1 participant for dtid:2.
func (tpc *TwoPC) UnresolvedTransactions(ctx context.Context, abandonTime time.Time) ([]*querypb.TransactionMetadata, error) {
	conn, err := tpc.readPool.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	bindVars := map[string]*querypb.BindVariable{
		"time_created": sqltypes.Int64BindVariable(abandonTime.UnixNano()),
	}
	qr, err := tpc.read(ctx, conn.Conn, tpc.readUnresolvedTransactions, bindVars)
	if err != nil {
		return nil, err
	}

	var (
		txs       []*querypb.TransactionMetadata
		currentTx *querypb.TransactionMetadata
	)

	appendCurrentTx := func() {
		if currentTx != nil {
			txs = append(txs, currentTx)
		}
	}

	for _, row := range qr.Rows {
		// Extract the distributed transaction ID from the row
		dtid := row[0].ToString()

		// Check if we are starting a new transaction
		if currentTx == nil || currentTx.Dtid != dtid {
			// If we have an existing transaction, append it to the list
			appendCurrentTx()

			// Extract the transaction state and initialize a new TransactionMetadata
			stateID, _ := row[1].ToInt()
			currentTx = &querypb.TransactionMetadata{
				Dtid:         dtid,
				State:        querypb.TransactionState(stateID),
				Participants: []*querypb.Target{},
			}
		}

		// Add the current participant (keyspace and shard) to the transaction
		currentTx.Participants = append(currentTx.Participants, &querypb.Target{
			Keyspace:   row[2].ToString(),
			Shard:      row[3].ToString(),
			TabletType: topodatapb.TabletType_PRIMARY,
		})
	}

	// Append the last transaction if it exists
	appendCurrentTx()

	return txs, nil
}

// CountUnresolvedTransaction returns the number of transaction record that are older than the given time.
func (tpc *TwoPC) CountUnresolvedTransaction(ctx context.Context, unresolvedTime time.Time) (int64, error) {
	conn, err := tpc.readPool.Get(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer conn.Recycle()

	bindVars := map[string]*querypb.BindVariable{
		"time_created": sqltypes.Int64BindVariable(unresolvedTime.UnixNano()),
	}
	qr, err := tpc.read(ctx, conn.Conn, tpc.countUnresolvedTransaction, bindVars)
	if err != nil {
		return 0, err
	}
	// executed query is a scalar aggregation, so we can safely assume that the result is a single row.
	v, _ := qr.Rows[0][0].ToCastInt64()
	return v, nil
}
