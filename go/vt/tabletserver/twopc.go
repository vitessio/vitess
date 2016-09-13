// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconnpool"
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
)

// TwoPC performs 2PC metadata management (MM) functions.
type TwoPC struct {
	txpool        *TxPool
	sidecarDBName string
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
}

// Close shuts down the 2PC MM service.
func (tpc *TwoPC) Close() {
	tpc.sidecarDBName = ""
}
