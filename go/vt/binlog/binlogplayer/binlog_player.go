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

// Package binlogplayer contains the code that plays a filtered replication
// stream on a client database. It usually runs inside the destination master
// vttablet process.
package binlogplayer

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/throttler"
)

var (
	// SlowQueryThreshold will cause we logging anything that's higher than it.
	SlowQueryThreshold = time.Duration(100 * time.Millisecond)

	// keys for the stats map

	// BlplQuery is the key for the stats map.
	BlplQuery = "Query"
	// BlplTransaction is the key for the stats map.
	BlplTransaction = "Transaction"

	// flags for the blp_checkpoint table. The database entry is just
	// a join(",") of these flags.

	// BlpFlagDontStart means don't start a BinlogPlayer
	BlpFlagDontStart = "DontStart"
)

// Stats is the internal stats of a player. It is a different
// structure that is passed in so stats can be collected over the life
// of multiple individual players.
type Stats struct {
	// Stats about the player, keys used are BlplQuery and BlplTransaction
	Timings *stats.Timings
	Rates   *stats.Rates

	// Last saved status
	lastPosition        mysql.Position
	lastPositionMutex   sync.RWMutex
	SecondsBehindMaster sync2.AtomicInt64
}

// SetLastPosition sets the last replication position.
func (bps *Stats) SetLastPosition(pos mysql.Position) {
	bps.lastPositionMutex.Lock()
	defer bps.lastPositionMutex.Unlock()
	bps.lastPosition = pos
}

// GetLastPosition gets the last replication position.
func (bps *Stats) GetLastPosition() mysql.Position {
	bps.lastPositionMutex.RLock()
	defer bps.lastPositionMutex.RUnlock()
	return bps.lastPosition
}

// NewStats creates a new Stats structure
func NewStats() *Stats {
	bps := &Stats{}
	bps.Timings = stats.NewTimings("")
	bps.Rates = stats.NewRates("", bps.Timings, 15, 60e9)
	return bps
}

// BinlogPlayer is handling reading a stream of updates from BinlogServer
type BinlogPlayer struct {
	tablet   *topodatapb.Tablet
	dbClient VtClient

	// for key range base requests
	keyRange *topodatapb.KeyRange

	// for table base requests
	tables []string

	// common to all
	uid            uint32
	position       mysql.Position
	stopPosition   mysql.Position
	blplStats      *Stats
	defaultCharset *binlogdatapb.Charset
	currentCharset *binlogdatapb.Charset
}

// NewBinlogPlayerKeyRange returns a new BinlogPlayer pointing at the server
// replicating the provided keyrange, starting at the startPosition,
// and updating _vt.blp_checkpoint with uid=startPosition.Uid.
// If !stopPosition.IsZero(), it will stop when reaching that position.
func NewBinlogPlayerKeyRange(dbClient VtClient, tablet *topodatapb.Tablet, keyRange *topodatapb.KeyRange, uid uint32, startPosition string, stopPosition string, blplStats *Stats) (*BinlogPlayer, error) {
	result := &BinlogPlayer{
		tablet:    tablet,
		dbClient:  dbClient,
		keyRange:  keyRange,
		uid:       uid,
		blplStats: blplStats,
	}
	var err error
	result.position, err = mysql.DecodePosition(startPosition)
	if err != nil {
		return nil, err
	}
	if stopPosition != "" {
		result.stopPosition, err = mysql.DecodePosition(stopPosition)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// NewBinlogPlayerTables returns a new BinlogPlayer pointing at the server
// replicating the provided tables, starting at the startPosition,
// and updating _vt.blp_checkpoint with uid=startPosition.Uid.
// If !stopPosition.IsZero(), it will stop when reaching that position.
func NewBinlogPlayerTables(dbClient VtClient, tablet *topodatapb.Tablet, tables []string, uid uint32, startPosition string, stopPosition string, blplStats *Stats) (*BinlogPlayer, error) {
	result := &BinlogPlayer{
		tablet:    tablet,
		dbClient:  dbClient,
		tables:    tables,
		uid:       uid,
		blplStats: blplStats,
	}
	var err error
	result.position, err = mysql.DecodePosition(startPosition)
	if err != nil {
		return nil, err
	}
	if stopPosition != "" {
		var err error
		result.stopPosition, err = mysql.DecodePosition(stopPosition)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
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
func (blp *BinlogPlayer) writeRecoveryPosition(tx *binlogdatapb.BinlogTransaction) error {
	position, err := mysql.DecodePosition(tx.EventToken.Position)
	if err != nil {
		return err
	}

	now := time.Now().Unix()

	blp.position = position
	updateRecovery := updateBlpCheckpoint(blp.uid, blp.position, now, tx.EventToken.Timestamp)

	qr, err := blp.exec(updateRecovery)
	if err != nil {
		return fmt.Errorf("Error %v in writing recovery info %v", err, updateRecovery)
	}
	if qr.RowsAffected != 1 {
		return fmt.Errorf("Cannot update blp_recovery table, affected %v rows", qr.RowsAffected)
	}
	blp.blplStats.SetLastPosition(blp.position)
	if tx.EventToken.Timestamp != 0 {
		blp.blplStats.SecondsBehindMaster.Set(now - tx.EventToken.Timestamp)
	}
	return nil
}

// ReadStartPosition will return the current start position and the flags for
// the provided binlog player.
func ReadStartPosition(dbClient VtClient, uid uint32) (string, string, error) {
	selectRecovery := QueryBlpCheckpoint(uid)
	qr, err := dbClient.ExecuteFetch(selectRecovery, 1)
	if err != nil {
		return "", "", fmt.Errorf("error %v in selecting from recovery table %v", err, selectRecovery)
	}
	if qr.RowsAffected != 1 {
		return "", "", fmt.Errorf("checkpoint information not available in db for %v", uid)
	}
	return qr.Rows[0][0].ToString(), qr.Rows[0][1].ToString(), nil
}

// readThrottlerSettings will retrieve the throttler settings for filtered
// replication from the checkpoint table.
func (blp *BinlogPlayer) readThrottlerSettings() (int64, int64, error) {
	selectThrottlerSettings := QueryBlpThrottlerSettings(blp.uid)
	qr, err := blp.dbClient.ExecuteFetch(selectThrottlerSettings, 1)
	if err != nil {
		return throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("error %v in selecting the throttler settings %v", err, selectThrottlerSettings)
	}

	if qr.RowsAffected != 1 {
		return throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("checkpoint information not available in db for %v", blp.uid)
	}

	maxTPS, err := sqltypes.ToInt64(qr.Rows[0][0])
	if err != nil {
		return throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("failed to parse max_tps column: %v", err)
	}
	maxReplicationLag, err := sqltypes.ToInt64(qr.Rows[0][1])
	if err != nil {
		return throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("failed to parse max_replication_lag column: %v", err)
	}

	return maxTPS, maxReplicationLag, nil
}

func (blp *BinlogPlayer) processTransaction(tx *binlogdatapb.BinlogTransaction) (ok bool, err error) {
	txnStartTime := time.Now()
	if err = blp.dbClient.Begin(); err != nil {
		return false, fmt.Errorf("failed query BEGIN, err: %s", err)
	}
	if err = blp.writeRecoveryPosition(tx); err != nil {
		return false, err
	}
	for i, stmt := range tx.Statements {
		// Make sure the statement is replayed in the proper charset.
		if dbClient, ok := blp.dbClient.(*DBClient); ok {
			var stmtCharset *binlogdatapb.Charset
			if stmt.Charset != nil {
				stmtCharset = stmt.Charset
			} else {
				// Streamer sends a nil Charset for statements that use the
				// charset we specified in the request.
				stmtCharset = blp.defaultCharset
			}
			if !proto.Equal(blp.currentCharset, stmtCharset) {
				// In regular MySQL replication, the charset is silently adjusted as
				// needed during event playback. Here we also adjust so that playback
				// proceeds, but in Vitess-land this usually means a misconfigured
				// server or a misbehaving client, so we spam the logs with warnings.
				log.Warningf("BinlogPlayer changing charset from %v to %v for statement %d in transaction %v", blp.currentCharset, stmtCharset, i, *tx)
				err = mysql.SetCharset(dbClient.dbConn, stmtCharset)
				if err != nil {
					return false, fmt.Errorf("can't set charset for statement %d in transaction %v: %v", i, *tx, err)
				}
				blp.currentCharset = stmtCharset
			}
		}
		if _, err = blp.exec(string(stmt.Sql)); err == nil {
			continue
		}
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == 1213 {
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
	blp.blplStats.Timings.Record(BlplTransaction, txnStartTime)
	return true, nil
}

func (blp *BinlogPlayer) exec(sql string) (*sqltypes.Result, error) {
	queryStartTime := time.Now()
	qr, err := blp.dbClient.ExecuteFetch(sql, 0)
	blp.blplStats.Timings.Record(BlplQuery, queryStartTime)
	if d := time.Now().Sub(queryStartTime); d > SlowQueryThreshold {
		log.Infof("SLOW QUERY (took %.2fs) '%s'", d.Seconds(), sql)
	}
	return qr, err
}

// ApplyBinlogEvents makes an RPC request to BinlogServer
// and processes the events. It will return nil if the provided context
// was canceled, or if we reached the stopping point.
// It will return io.EOF if the server stops sending us updates.
// It may return any other error it encounters.
func (blp *BinlogPlayer) ApplyBinlogEvents(ctx context.Context) error {
	// Instantiate the throttler based on the configuration stored in the db.
	maxTPS, maxReplicationLag, err := blp.readThrottlerSettings()
	if err != nil {
		log.Error(err)
		return err
	}
	t, err := throttler.NewThrottler(
		fmt.Sprintf("BinlogPlayer/%d", blp.uid), "transactions", 1 /* threadCount */, maxTPS, maxReplicationLag)
	if err != nil {
		err := fmt.Errorf("failed to instantiate throttler: %v", err)
		log.Error(err)
		return err
	}
	defer t.Close()

	// Log the mode of operation and when the player stops.
	if len(blp.tables) > 0 {
		log.Infof("BinlogPlayer client %v for tables %v starting @ '%v', server: %v",
			blp.uid,
			blp.tables,
			blp.position,
			blp.tablet,
		)
	} else {
		log.Infof("BinlogPlayer client %v for keyrange '%v-%v' starting @ '%v', server: %v",
			blp.uid,
			hex.EncodeToString(blp.keyRange.Start),
			hex.EncodeToString(blp.keyRange.End),
			blp.position,
			blp.tablet,
		)
	}
	if !blp.stopPosition.IsZero() {
		// We need to stop at some point. Sanity check the point.
		switch {
		case blp.position.Equal(blp.stopPosition):
			log.Infof("Not starting BinlogPlayer, we're already at the desired position %v", blp.stopPosition)
			return nil
		case blp.position.AtLeast(blp.stopPosition):
			return fmt.Errorf("starting point %v greater than stopping point %v", blp.position, blp.stopPosition)
		default:
			log.Infof("Will stop player when reaching %v", blp.stopPosition)
		}
	}

	clientFactory, ok := clientFactories[*binlogPlayerProtocol]
	if !ok {
		return fmt.Errorf("no binlog player client factory named %v", *binlogPlayerProtocol)
	}
	blplClient := clientFactory()
	err = blplClient.Dial(blp.tablet)
	if err != nil {
		err := fmt.Errorf("error dialing binlog server: %v", err)
		log.Error(err)
		return err
	}
	defer blplClient.Close()

	// Get the current charset of our connection, so we can ask the stream server
	// to check that they match. The streamer will also only send per-statement
	// charset data if that statement's charset is different from what we specify.
	if dbClient, ok := blp.dbClient.(*DBClient); ok {
		blp.defaultCharset, err = mysql.GetCharset(dbClient.dbConn)
		if err != nil {
			return fmt.Errorf("can't get charset to request binlog stream: %v", err)
		}
		log.Infof("original charset: %v", blp.defaultCharset)
		blp.currentCharset = blp.defaultCharset
		// Restore original charset when we're done.
		defer func() {
			// If the connection has been closed, there's no need to restore
			// this connection-specific setting.
			if dbClient.dbConn == nil {
				return
			}
			log.Infof("restoring original charset %v", blp.defaultCharset)
			if csErr := mysql.SetCharset(dbClient.dbConn, blp.defaultCharset); csErr != nil {
				log.Errorf("can't restore original charset %v: %v", blp.defaultCharset, csErr)
			}
		}()
	}

	var stream BinlogTransactionStream
	if len(blp.tables) > 0 {
		stream, err = blplClient.StreamTables(ctx, mysql.EncodePosition(blp.position), blp.tables, blp.defaultCharset)
	} else {
		stream, err = blplClient.StreamKeyRange(ctx, mysql.EncodePosition(blp.position), blp.keyRange, blp.defaultCharset)
	}
	if err != nil {
		err := fmt.Errorf("error sending streaming query to binlog server: %v", err)
		log.Error(err)
		return err
	}

	for {
		// Block if we are throttled.
		for {
			backoff := t.Throttle(0 /* threadID */)
			if backoff == throttler.NotThrottled {
				break
			}
			// We don't bother checking for context cancellation here because the
			// sleep will block only up to 1 second. (Usually, backoff is 1s / rate
			// e.g. a rate of 1000 TPS results into a backoff of 1 ms.)
			time.Sleep(backoff)
		}

		// get the response
		response, err := stream.Recv()
		if err != nil {
			switch err {
			case context.Canceled:
				return nil
			default:
				// if the context is canceled, we
				// return nil (some RPC
				// implementations will remap the
				// context error to their own errors)
				select {
				case <-ctx.Done():
					if ctx.Err() == context.Canceled {
						return nil
					}
				default:
				}
				return fmt.Errorf("Error received from Stream %v", err)
			}
		}

		// process the transaction
		for {
			ok, err = blp.processTransaction(response)
			if err != nil {
				return fmt.Errorf("Error in processing binlog event %v", err)
			}
			if ok {
				if !blp.stopPosition.IsZero() {
					if blp.position.AtLeast(blp.stopPosition) {
						log.Infof("Reached stopping position, done playing logs")
						return nil
					}
				}
				break
			}
			log.Infof("Retrying txn")
			time.Sleep(1 * time.Second)
		}
	}
}

// CreateBlpCheckpoint returns the statements required to create
// the _vt.blp_checkpoint table
func CreateBlpCheckpoint() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (
  source_shard_uid INT(10) UNSIGNED NOT NULL,
  pos VARBINARY(%v) DEFAULT NULL,
  max_tps BIGINT(20) NOT NULL,
  max_replication_lag BIGINT(20) NOT NULL,
  time_updated BIGINT(20) UNSIGNED NOT NULL,
  transaction_timestamp BIGINT(20) UNSIGNED NOT NULL,
  flags VARBINARY(250) DEFAULT NULL,
  PRIMARY KEY (source_shard_uid)
) ENGINE=InnoDB`, mysql.MaximumPositionSize)}
}

// PopulateBlpCheckpoint returns a statement to populate the first value into
// the _vt.blp_checkpoint table.
func PopulateBlpCheckpoint(index uint32, position string, maxTPS int64, maxReplicationLag int64, timeUpdated int64, flags string) string {
	return fmt.Sprintf("INSERT INTO _vt.blp_checkpoint "+
		"(source_shard_uid, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, flags) "+
		"VALUES (%v, '%v', %v, %v, %v, 0, '%v')",
		index, position, maxTPS, maxReplicationLag, timeUpdated, flags)
}

// updateBlpCheckpoint returns a statement to update a value in the
// _vt.blp_checkpoint table.
func updateBlpCheckpoint(uid uint32, pos mysql.Position, timeUpdated int64, txTimestamp int64) string {
	if txTimestamp != 0 {
		return fmt.Sprintf(
			"UPDATE _vt.blp_checkpoint "+
				"SET pos='%v', time_updated=%v, transaction_timestamp=%v "+
				"WHERE source_shard_uid=%v",
			mysql.EncodePosition(pos), timeUpdated, txTimestamp, uid)
	}

	return fmt.Sprintf(
		"UPDATE _vt.blp_checkpoint "+
			"SET pos='%v', time_updated=%v "+
			"WHERE source_shard_uid=%v",
		mysql.EncodePosition(pos), timeUpdated, uid)
}

// QueryBlpCheckpoint returns a statement to query the gtid and flags for a
// given shard from the _vt.blp_checkpoint table.
func QueryBlpCheckpoint(index uint32) string {
	return fmt.Sprintf("SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=%v", index)
}

// QueryBlpThrottlerSettings returns a statement to query the throttler settings
// (used by filtered replication) for a given shard from the_vt.blp_checkpoint
// table.
func QueryBlpThrottlerSettings(index uint32) string {
	return fmt.Sprintf("SELECT max_tps, max_replication_lag FROM _vt.blp_checkpoint WHERE source_shard_uid=%v", index)
}
