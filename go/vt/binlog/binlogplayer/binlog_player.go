// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package binlogplayer contains the code that plays a filtered replication
// stream on a client database. It usually runs inside the destination master
// vttablet process.
package binlogplayer

import (
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var (
	// we will log anything that's higher than that
	SLOW_QUERY_THRESHOLD = time.Duration(100 * time.Millisecond)

	// keys for the stats map
	BLPL_QUERY       = "Query"
	BLPL_TRANSACTION = "Transaction"

	// flags for the blp_checkpoint table. The database entry is just
	// a join(",") of these flags.
	BLP_FLAG_DONT_START = "DontStart"
)

// BinlogPlayerStats is the internal stats of a player. It is a different
// structure that is passed in so stats can be collected over the life
// of multiple individual players.
type BinlogPlayerStats struct {
	// Stats about the player, keys used are BLPL_QUERY and BLPL_TRANSACTION
	Timings *stats.Timings
	Rates   *stats.Rates

	// Last saved status
	lastGTID            myproto.GTID
	lastGTIDMutex       sync.RWMutex
	SecondsBehindMaster sync2.AtomicInt64
}

func (bps *BinlogPlayerStats) SetLastGTID(gtid myproto.GTID) {
	bps.lastGTIDMutex.Lock()
	defer bps.lastGTIDMutex.Unlock()
	bps.lastGTID = gtid
}

func (bps *BinlogPlayerStats) GetLastGTID() myproto.GTID {
	bps.lastGTIDMutex.RLock()
	defer bps.lastGTIDMutex.RUnlock()
	return bps.lastGTID
}

// NewBinlogPlayerStats creates a new BinlogPlayerStats structure
func NewBinlogPlayerStats() *BinlogPlayerStats {
	bps := &BinlogPlayerStats{}
	bps.Timings = stats.NewTimings("")
	bps.Rates = stats.NewRates("", bps.Timings, 15, 60e9)
	return bps
}

// BinlogPlayer is handling reading a stream of updates from BinlogServer
type BinlogPlayer struct {
	addr     string
	dbClient VtClient

	// for key range base requests
	keyspaceIdType key.KeyspaceIdType
	keyRange       key.KeyRange

	// for table base requests
	tables []string

	// common to all
	blpPos     proto.BlpPosition
	stopAtGTID myproto.GTID
	blplStats  *BinlogPlayerStats
}

// NewBinlogPlayerKeyRange returns a new BinlogPlayer pointing at the server
// replicating the provided keyrange, starting at the startPosition.GTID,
// and updating _vt.blp_checkpoint with uid=startPosition.Uid.
// If stopAtGTID != nil, it will stop when reaching that GTID.
func NewBinlogPlayerKeyRange(dbClient VtClient, addr string, keyspaceIdType key.KeyspaceIdType, keyRange key.KeyRange, startPosition *proto.BlpPosition, stopAtGTID myproto.GTID, blplStats *BinlogPlayerStats) *BinlogPlayer {
	return &BinlogPlayer{
		addr:           addr,
		dbClient:       dbClient,
		keyspaceIdType: keyspaceIdType,
		keyRange:       keyRange,
		blpPos:         *startPosition,
		stopAtGTID:     stopAtGTID,
		blplStats:      blplStats,
	}
}

// NewBinlogPlayerTables returns a new BinlogPlayer pointing at the server
// replicating the provided tables, starting at the startPosition.GTID,
// and updating _vt.blp_checkpoint with uid=startPosition.Uid.
// If stopAtGTID != nil, it will stop when reaching that GTID.
func NewBinlogPlayerTables(dbClient VtClient, addr string, tables []string, startPosition *proto.BlpPosition, stopAtGTID myproto.GTID, blplStats *BinlogPlayerStats) *BinlogPlayer {
	return &BinlogPlayer{
		addr:       addr,
		dbClient:   dbClient,
		tables:     tables,
		blpPos:     *startPosition,
		stopAtGTID: stopAtGTID,
		blplStats:  blplStats,
	}
}

// writeRecoveryPosition will write the current GTID as the recovery position
// for the next transaction.
// We will also try to get the timestamp for the transaction. Two cases:
// - we have statements, and they start with a SET TIMESTAMP that we
//   can parse: then we update transaction_timestamp in blp_checkpoint
//   with it, and set SecondsBehindMaster to now() - transaction_timestamp
// - otherwise (the statements are probably filtered out), we leave
//   transaction_timestamp alone (keeping the old value), and we don't
//   change SecondsBehindMaster
func (blp *BinlogPlayer) writeRecoveryPosition(tx *proto.BinlogTransaction) error {
	now := time.Now().Unix()

	blp.blpPos.GTID = tx.GTID
	updateRecovery := UpdateBlpCheckpoint(blp.blpPos.Uid, tx.GTID, now, tx.Timestamp)

	qr, err := blp.exec(updateRecovery)
	if err != nil {
		return fmt.Errorf("Error %v in writing recovery info %v", err, updateRecovery)
	}
	if qr.RowsAffected != 1 {
		return fmt.Errorf("Cannot update blp_recovery table, affected %v rows", qr.RowsAffected)
	}
	blp.blplStats.SetLastGTID(tx.GTID)
	if tx.Timestamp != 0 {
		blp.blplStats.SecondsBehindMaster.Set(now - tx.Timestamp)
	}
	return nil
}

// ReadStartPosition will return the current start position and the flags for
// the provided binlog player.
func ReadStartPosition(dbClient VtClient, uid uint32) (*proto.BlpPosition, string, error) {
	selectRecovery := QueryBlpCheckpoint(uid)
	qr, err := dbClient.ExecuteFetch(selectRecovery, 1, true)
	if err != nil {
		return nil, "", fmt.Errorf("error %v in selecting from recovery table %v", err, selectRecovery)
	}
	if qr.RowsAffected != 1 {
		return nil, "", fmt.Errorf("checkpoint information not available in db for %v", uid)
	}
	gtid, err := myproto.DecodeGTID(qr.Rows[0][0].String())
	if err != nil {
		return nil, "", err
	}
	return &proto.BlpPosition{
		Uid:  uid,
		GTID: myproto.GTIDField{gtid},
	}, string(qr.Rows[0][1].Raw()), nil
}

func (blp *BinlogPlayer) processTransaction(tx *proto.BinlogTransaction) (ok bool, err error) {
	txnStartTime := time.Now()
	if err = blp.dbClient.Begin(); err != nil {
		return false, fmt.Errorf("failed query BEGIN, err: %s", err)
	}
	if err = blp.writeRecoveryPosition(tx); err != nil {
		return false, err
	}
	for _, stmt := range tx.Statements {
		if _, err = blp.exec(string(stmt.Sql)); err == nil {
			continue
		}
		if sqlErr, ok := err.(*mysql.SqlError); ok && sqlErr.Number() == 1213 {
			// Deadlock: ask for retry
			log.Infof("Deadlock: %v", err)
			if err = blp.dbClient.Rollback(); err != nil {
				return false, err
			}
			return false, nil
		}
		return false, err
	}
	if err = blp.dbClient.Commit(); err != nil {
		return false, fmt.Errorf("failed query COMMIT, err: %s", err)
	}
	blp.blplStats.Timings.Record(BLPL_TRANSACTION, txnStartTime)
	return true, nil
}

func (blp *BinlogPlayer) exec(sql string) (*mproto.QueryResult, error) {
	queryStartTime := time.Now()
	qr, err := blp.dbClient.ExecuteFetch(sql, 0, false)
	blp.blplStats.Timings.Record(BLPL_QUERY, queryStartTime)
	if time.Now().Sub(queryStartTime) > SLOW_QUERY_THRESHOLD {
		log.Infof("SLOW QUERY '%s'", sql)
	}
	return qr, err
}

// ApplyBinlogEvents makes a gob rpc request to BinlogServer
// and processes the events. It will return nil if 'interrupted'
// was closed, or if we reached the stopping point.
// It will return io.EOF if the server stops sending us updates.
// It may return any other error it encounters.
func (blp *BinlogPlayer) ApplyBinlogEvents(interrupted chan struct{}) error {
	if len(blp.tables) > 0 {
		log.Infof("BinlogPlayer client %v for tables %v starting @ '%v', server: %v",
			blp.blpPos.Uid,
			blp.tables,
			blp.blpPos.GTID,
			blp.addr,
		)
	} else {
		log.Infof("BinlogPlayer client %v for keyrange '%v-%v' starting @ '%v', server: %v",
			blp.blpPos.Uid,
			blp.keyRange.Start.Hex(),
			blp.keyRange.End.Hex(),
			blp.blpPos.GTID,
			blp.addr,
		)
	}
	if blp.stopAtGTID != nil {
		// We need to stop at some point. Sanity check the point.
		//
		// TODO(enisoc): Stop trying to compare GTIDs, since we can't do that
		// reliably for some MySQL flavors.
		cmp, err := blp.blpPos.GTID.TryCompare(blp.stopAtGTID)
		if err != nil {
			return err
		}
		if cmp > 0 { // blp.blpPos.GTID > blp.stopAtGTID
			return fmt.Errorf("starting point %v greater than stopping point %v", blp.blpPos.GTID, blp.stopAtGTID)
		} else if cmp == 0 { // blp.blpPos.GTID == blp.stopAtGTID
			log.Infof("Not starting BinlogPlayer, we're already at the desired position %v", blp.stopAtGTID)
			return nil
		}
		log.Infof("Will stop player when reaching %v", blp.stopAtGTID)
	}

	binlogPlayerClientFactory, ok := binlogPlayerClientFactories[*binlogPlayerProtocol]
	if !ok {
		return fmt.Errorf("no binlog player client factory named %v", *binlogPlayerProtocol)
	}
	blplClient := binlogPlayerClientFactory()
	err := blplClient.Dial(blp.addr, *binlogPlayerConnTimeout)
	if err != nil {
		log.Errorf("Error dialing binlog server: %v", err)
		return fmt.Errorf("error dialing binlog server: %v", err)
	}
	defer blplClient.Close()

	responseChan := make(chan *proto.BinlogTransaction)
	var resp BinlogPlayerResponse
	if len(blp.tables) > 0 {
		req := &proto.TablesRequest{
			Tables: blp.tables,
			GTID:   blp.blpPos.GTID,
		}
		resp = blplClient.StreamTables(req, responseChan)
	} else {
		req := &proto.KeyRangeRequest{
			KeyspaceIdType: blp.keyspaceIdType,
			KeyRange:       blp.keyRange,
			GTID:           blp.blpPos.GTID,
		}
		resp = blplClient.StreamKeyRange(req, responseChan)
	}

processLoop:
	for {
		select {
		case response, ok := <-responseChan:
			if !ok {
				break processLoop
			}
			for {
				ok, err = blp.processTransaction(response)
				if err != nil {
					return fmt.Errorf("Error in processing binlog event %v", err)
				}
				if ok {
					if blp.stopAtGTID != nil {
						// TODO(enisoc): Stop trying to compare GTIDs.
						cmp, err := blp.blpPos.GTID.TryCompare(blp.stopAtGTID)
						if err != nil {
							return err
						}
						if cmp >= 0 { // blp.blpPos.GTID >= blp.stopAtGTID
							log.Infof("Reached stopping position, done playing logs")
							return nil
						}
					}
					break
				}
				log.Infof("Retrying txn")
				time.Sleep(1 * time.Second)
			}
		case <-interrupted:
			return nil
		}
	}
	if resp.Error() != nil {
		return fmt.Errorf("Error received from ServeBinlog %v", resp.Error())
	}
	return io.EOF
}

// CreateBlpCheckpoint returns the statements required to create
// the _vt.blp_checkpoint table
func CreateBlpCheckpoint() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		`CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (
  source_shard_uid INT(10) UNSIGNED NOT NULL,
  gtid VARCHAR(250) DEFAULT NULL,
  time_updated BIGINT UNSIGNED NOT NULL,
  transaction_timestamp BIGINT UNSIGNED NOT NULL,
  flags VARCHAR(250) DEFAULT NULL,
  PRIMARY KEY (source_shard_uid)) ENGINE=InnoDB`}
}

// PopulateBlpCheckpoint returns a statement to populate the first value into
// the _vt.blp_checkpoint table.
func PopulateBlpCheckpoint(index uint32, gtid myproto.GTID, timeUpdated int64, flags string) string {
	return fmt.Sprintf("INSERT INTO _vt.blp_checkpoint "+
		"(source_shard_uid, gtid, time_updated, transaction_timestamp, flags) "+
		"VALUES (%v, '%v', %v, 0, '%v')",
		index, myproto.EncodeGTID(gtid), timeUpdated, flags)
}

// UpdateBlpCheckpoint returns a statement to update a value in the
// _vt.blp_checkpoint table.
func UpdateBlpCheckpoint(uid uint32, gtid myproto.GTID, timeUpdated int64, txTimestamp int64) string {
	if txTimestamp != 0 {
		return fmt.Sprintf(
			"UPDATE _vt.blp_checkpoint "+
				"SET gtid='%v', time_updated=%v, transaction_timestamp=%v "+
				"WHERE source_shard_uid=%v",
			myproto.EncodeGTID(gtid), timeUpdated, txTimestamp, uid)
	} else {
		return fmt.Sprintf(
			"UPDATE _vt.blp_checkpoint "+
				"SET gtid='%v', time_updated=%v "+
				"WHERE source_shard_uid=%v",
			myproto.EncodeGTID(gtid), timeUpdated, uid)
	}
}

// QueryBlpCheckpoint returns a statement to query the gtid and flags for a
// given shard from the _vt.blp_checkpoint table.
func QueryBlpCheckpoint(index uint32) string {
	return fmt.Sprintf("SELECT gtid, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=%v", index)
}
