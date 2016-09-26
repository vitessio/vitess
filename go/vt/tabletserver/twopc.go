// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strconv"
	"time"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"golang.org/x/net/context"
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

	sqlInsertRedoTx   = "insert into `%s`.redo_log_transaction(dtid, state, resolution, time_created) values %a"
	sqlInsertRedoStmt = "insert into `%s`.redo_log_statement(dtid, id, statement) values %a"
	sqlDeleteRedoTx   = "delete from `%s`.redo_log_transaction where dtid = %a"
	sqlDeleteRedoStmt = "delete from `%s`.redo_log_statement where dtid = %a"
)

// TwoPC performs 2PC metadata management (MM) functions.
type TwoPC struct {
	txpool        *TxPool
	sidecarDBName string

	// Parsed queries for efficient code generation.
	insertRedoTx   *sqlparser.ParsedQuery
	insertRedoStmt *sqlparser.ParsedQuery
	deleteRedoTx   *sqlparser.ParsedQuery
	deleteRedoStmt *sqlparser.ParsedQuery
}

// NewTwoPC creates a TwoPC variable. It requires a TxPool to
// perform its operations.
func NewTwoPC(txpool *TxPool) *TwoPC {
	return &TwoPC{
		txpool: txpool,
	}
}

// Open starts the 2PC MM service. If the metadata database or tables
// are not present, they are created.
func (tpc *TwoPC) Open(sidecardbname string, dbaparams *sqldb.ConnParams) {
	tpc.sidecarDBName = sidecardbname
	conn, err := dbconnpool.NewDBConnection(dbaparams, stats.NewTimings(""))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	statements := []string{
		sqlTurnoffBinlog,
		fmt.Sprintf(sqlCreateSidecarDB, tpc.sidecarDBName),
		fmt.Sprintf(sqlCreateTableRedoLogTransaction, tpc.sidecarDBName),
		fmt.Sprintf(sqlCreateTableRedoLogStatement, tpc.sidecarDBName),
		fmt.Sprintf(sqlCreateTableTransaction, tpc.sidecarDBName),
		fmt.Sprintf(sqlCreateTableParticipant, tpc.sidecarDBName),
	}
	for _, s := range statements {
		if _, err := conn.ExecuteFetch(s, 0, false); err != nil {
			panic(NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err.Error()))
		}
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(sqlInsertRedoTx, tpc.sidecarDBName, ":vals")
	tpc.insertRedoTx = buf.ParsedQuery()
	buf = sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(sqlInsertRedoStmt, tpc.sidecarDBName, ":vals")
	tpc.insertRedoStmt = buf.ParsedQuery()
	buf = sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(sqlDeleteRedoTx, tpc.sidecarDBName, ":dtid")
	tpc.deleteRedoTx = buf.ParsedQuery()
	buf = sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(sqlDeleteRedoStmt, tpc.sidecarDBName, ":dtid")
	tpc.deleteRedoStmt = buf.ParsedQuery()
}

// Close shuts down the 2PC MM service.
func (tpc *TwoPC) Close() {
	tpc.sidecarDBName = ""
}

// SaveRedo saves the statements in the redo log using the supplied connection.
func (tpc *TwoPC) SaveRedo(ctx context.Context, conn *TxConnection, dtid string, queries []string) error {
	bindVars := map[string]interface{}{
		"vals": [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(dtid)),
			sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("Prepared")),
			sqltypes.NULL,
			sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, int64(time.Now().UnixNano()), 10)),
		}},
	}
	err := tpc.exec(ctx, conn, tpc.insertRedoTx, bindVars)
	if err != nil {
		return err
	}

	if len(queries) == 0 {
		return nil
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
	return tpc.exec(ctx, conn, tpc.insertRedoStmt, bindVars)
}

// DeleteRedo deletes the redo log for the dtid.
func (tpc *TwoPC) DeleteRedo(ctx context.Context, conn *TxConnection, dtid string) error {
	bindVars := map[string]interface{}{
		"dtid": sqltypes.MakeTrusted(sqltypes.VarBinary, []byte(dtid)),
	}
	err := tpc.exec(ctx, conn, tpc.deleteRedoTx, bindVars)
	if err != nil {
		return err
	}
	return tpc.exec(ctx, conn, tpc.deleteRedoStmt, bindVars)
}

func (tpc *TwoPC) exec(ctx context.Context, conn *TxConnection, pq *sqlparser.ParsedQuery, bindVars map[string]interface{}) error {
	b, err := pq.GenerateQuery(bindVars)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, hack.String(b), 1, false)
	return err
}
