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

// Package binlogplayer contains the code that plays a vreplication
// stream on a client database. It usually runs inside the destination master
// vttablet process.
package binlogplayer

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/throttler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// SlowQueryThreshold will cause we logging anything that's higher than it.
	SlowQueryThreshold = time.Duration(100 * time.Millisecond)

	// keys for the stats map

	// BlplQuery is the key for the stats map.
	BlplQuery = "Query"
	// BlplTransaction is the key for the stats map.
	BlplTransaction = "Transaction"

	// BlpRunning is for the Running state.
	BlpRunning = "Running"
	// BlpStopped is for the Stopped state.
	BlpStopped = "Stopped"
	// BlpError is for the Error state.
	BlpError = "Error"
)

// Stats is the internal stats of a player. It is a different
// structure that is passed in so stats can be collected over the life
// of multiple individual players.
type Stats struct {
	// Stats about the player, keys used are BlplQuery and BlplTransaction
	Timings *stats.Timings
	Rates   *stats.Rates

	// Last saved status
	lastPositionMutex sync.Mutex
	lastPosition      mysql.Position

	SecondsBehindMaster sync2.AtomicInt64
	History             *history.History
}

// SetLastPosition sets the last replication position.
func (bps *Stats) SetLastPosition(pos mysql.Position) {
	bps.lastPositionMutex.Lock()
	defer bps.lastPositionMutex.Unlock()
	bps.lastPosition = pos
}

// LastPosition gets the last replication position.
func (bps *Stats) LastPosition() mysql.Position {
	bps.lastPositionMutex.Lock()
	defer bps.lastPositionMutex.Unlock()
	return bps.lastPosition
}

// MessageHistory gets all the messages, we store 3 at a time
func (bps *Stats) MessageHistory() []string {
	strs := make([]string, 0, 3)
	for _, h := range bps.History.Records() {
		h1, _ := h.(*StatsHistoryRecord)
		if h1 != nil {
			strs = append(strs, h1.Message)
		}
	}
	return strs
}

// NewStats creates a new Stats structure.
func NewStats() *Stats {
	bps := &Stats{}
	bps.Timings = stats.NewTimings("", "", "")
	bps.Rates = stats.NewRates("", bps.Timings, 15, 60e9)
	bps.History = history.New(3)
	return bps
}

// BinlogPlayer is for reading a stream of updates from BinlogServer.
type BinlogPlayer struct {
	tablet   *topodatapb.Tablet
	dbClient DBClient

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
	deadlockRetry  time.Duration
}

// NewBinlogPlayerKeyRange returns a new BinlogPlayer pointing at the server
// replicating the provided keyrange and updating _vt.vreplication
// with uid=startPosition.Uid.
// If !stopPosition.IsZero(), it will stop when reaching that position.
func NewBinlogPlayerKeyRange(dbClient DBClient, tablet *topodatapb.Tablet, keyRange *topodatapb.KeyRange, uid uint32, blplStats *Stats) *BinlogPlayer {
	result := &BinlogPlayer{
		tablet:        tablet,
		dbClient:      dbClient,
		keyRange:      keyRange,
		uid:           uid,
		blplStats:     blplStats,
		deadlockRetry: 1 * time.Second,
	}
	return result
}

// NewBinlogPlayerTables returns a new BinlogPlayer pointing at the server
// replicating the provided tables and updating _vt.vreplication
// with uid=startPosition.Uid.
// If !stopPosition.IsZero(), it will stop when reaching that position.
func NewBinlogPlayerTables(dbClient DBClient, tablet *topodatapb.Tablet, tables []string, uid uint32, blplStats *Stats) *BinlogPlayer {
	result := &BinlogPlayer{
		tablet:        tablet,
		dbClient:      dbClient,
		tables:        tables,
		uid:           uid,
		blplStats:     blplStats,
		deadlockRetry: 1 * time.Second,
	}
	return result
}

// ApplyBinlogEvents makes an RPC request to BinlogServer
// and processes the events. It returns nil if the provided context
// was canceled, or if we reached the stopping point.
// If an error is encountered, it updates the vreplication state to "Error".
// If a stop position was specifed, and reached, the state is updated to "Stopped".
func (blp *BinlogPlayer) ApplyBinlogEvents(ctx context.Context) error {
	if err := setVReplicationState(blp.dbClient, blp.uid, BlpRunning, ""); err != nil {
		log.Errorf("Error writing Running state: %v", err)
	}

	if err := blp.applyEvents(ctx); err != nil {
		msg := err.Error()
		blp.blplStats.History.Add(&StatsHistoryRecord{
			Time:    time.Now(),
			Message: msg,
		})
		if err := setVReplicationState(blp.dbClient, blp.uid, BlpError, msg); err != nil {
			log.Errorf("Error writing stop state: %v", err)
		}
		return err
	}
	return nil
}

// applyEvents returns a recordable status message on termination or an error otherwise.
func (blp *BinlogPlayer) applyEvents(ctx context.Context) error {
	// Read starting values for vreplication.
	pos, stopPos, maxTPS, maxReplicationLag, err := readVRSettings(blp.dbClient, blp.uid)
	if err != nil {
		log.Error(err)
		return err
	}
	blp.position, err = mysql.DecodePosition(pos)
	if err != nil {
		log.Error(err)
		return err
	}
	if stopPos != "" {
		blp.stopPosition, err = mysql.DecodePosition(stopPos)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	t, err := throttler.NewThrottler(
		fmt.Sprintf("BinlogPlayer/%d", blp.uid),
		"transactions",
		1, /* threadCount */
		maxTPS,
		maxReplicationLag,
	)
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
		switch {
		case blp.position.Equal(blp.stopPosition):
			msg := fmt.Sprintf("not starting BinlogPlayer, we're already at the desired position %v", blp.stopPosition)
			log.Info(msg)
			if err := setVReplicationState(blp.dbClient, blp.uid, BlpStopped, msg); err != nil {
				log.Errorf("Error writing stop state: %v", err)
			}
			return nil
		case blp.position.AtLeast(blp.stopPosition):
			msg := fmt.Sprintf("starting point %v greater than stopping point %v", blp.position, blp.stopPosition)
			log.Error(msg)
			if err := setVReplicationState(blp.dbClient, blp.uid, BlpStopped, msg); err != nil {
				log.Errorf("Error writing stop state: %v", err)
			}
			// Don't return an error. Otherwise, it will keep retrying.
			return nil
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
	if dbClient, ok := blp.dbClient.(*dbClientImpl); ok {
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
		// Check context before checking error, because canceled
		// contexts could be wrapped as regular errors.
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err != nil {
			return fmt.Errorf("error received from Stream %v", err)
		}

		// process the transaction
		for {
			ok, err = blp.processTransaction(response)
			if err != nil {
				log.Infof("transaction failed: %v", err)
				for _, stmt := range response.Statements {
					log.Infof("statement: %q", stmt.Sql)
				}
				return fmt.Errorf("error in processing binlog event %v", err)
			}
			if ok {
				if !blp.stopPosition.IsZero() {
					if blp.position.AtLeast(blp.stopPosition) {
						msg := "Reached stopping position, done playing logs"
						log.Info(msg)
						if err := setVReplicationState(blp.dbClient, blp.uid, BlpStopped, msg); err != nil {
							log.Errorf("Error writing stop state: %v", err)
						}
						return nil
					}
				}
				break
			}
			log.Infof("Retrying txn in %v.", blp.deadlockRetry)
			time.Sleep(blp.deadlockRetry)
		}
	}
}

func (blp *BinlogPlayer) processTransaction(tx *binlogdatapb.BinlogTransaction) (ok bool, err error) {
	txnStartTime := time.Now()
	if err = blp.dbClient.Begin(); err != nil {
		return false, fmt.Errorf("failed query BEGIN, err: %s", err)
	}
	for i, stmt := range tx.Statements {
		// Make sure the statement is replayed in the proper charset.
		if dbClient, ok := blp.dbClient.(*dbClientImpl); ok {
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
		_ = blp.dbClient.Rollback()
		return false, err
	}
	// Update recovery position after successful replay.
	// This also updates the blp's internal position.
	if err = blp.writeRecoveryPosition(tx); err != nil {
		_ = blp.dbClient.Rollback()
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

// writeRecoveryPosition writes the current GTID as the recovery position
// for the next transaction.
// It also tries to get the timestamp for the transaction. Two cases:
// - we have statements, and they start with a SET TIMESTAMP that we
//   can parse: then we update transaction_timestamp in vreplication
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
	updateRecovery := updateVReplicationPos(blp.uid, position, now, tx.EventToken.Timestamp)

	qr, err := blp.exec(updateRecovery)
	if err != nil {
		return fmt.Errorf("error %v in writing recovery info %v", err, updateRecovery)
	}
	if qr.RowsAffected != 1 {
		return fmt.Errorf("cannot update vreplication table, affected %v rows", qr.RowsAffected)
	}

	// Update position after successful write.
	blp.position = position
	blp.blplStats.SetLastPosition(blp.position)
	if tx.EventToken.Timestamp != 0 {
		blp.blplStats.SecondsBehindMaster.Set(now - tx.EventToken.Timestamp)
	}
	return nil
}

// CreateVReplicationTable returns the statements required to create
// the _vt.vreplication table.
// id: is an auto-increment column that identifies the stream.
// workflow: documents the creator/manager of the stream. Example: 'SplitClone'.
// source: contains a string proto representation of binlogpb.BinlogSource.
// pos: initially, a start position, and is updated to the current position by the binlog player.
// stop_pos: optional column that specifies the stop position.
// max_tps: max transactions per second.
// max_replication_lag: if replication lag exceeds this amount writing is throttled accordingly.
// cell: optional column that overrides the current cell to replicate from.
// tablet_types: optional column that overrides the tablet types to look to replicate from.
// time_update: last time an event was applied.
// transaction_timestamp: timestamp of the transaction (from the master).
// state: Running, Error or Stopped.
// message: Reason for current state.
func CreateVReplicationTable() []string {
	return []string{
		"CREATE DATABASE IF NOT EXISTS _vt",
		"DROP TABLE IF EXISTS _vt.blp_checkpoint",
		`CREATE TABLE IF NOT EXISTS _vt.vreplication (
  id INT AUTO_INCREMENT,
  workflow VARBINARY(1000),
  source VARBINARY(10000) NOT NULL,
  pos VARBINARY(10000) NOT NULL,
  stop_pos VARBINARY(10000) DEFAULT NULL,
  max_tps BIGINT(20) NOT NULL,
  max_replication_lag BIGINT(20) NOT NULL,
  cell VARBINARY(1000) DEFAULT NULL,
  tablet_types VARBINARY(100) DEFAULT NULL,
  time_updated BIGINT(20) NOT NULL,
  transaction_timestamp BIGINT(20) NOT NULL,
  state VARBINARY(100) NOT NULL,
  message VARBINARY(1000) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB`}
}

// setVReplicationState updates the state in the _vt.vreplication table.
func setVReplicationState(dbClient DBClient, uid uint32, state, message string) error {
	query := fmt.Sprintf("update _vt.vreplication set state='%v', message=%v where id=%v", state, encodeString(message), uid)
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set state: %v: %v", query, err)
	}
	return nil
}

// readVRSettings retrieves the throttler settings for
// vreplication from the checkpoint table.
func readVRSettings(dbClient DBClient, uid uint32) (pos, stopPos string, maxTPS, maxReplicationLag int64, err error) {
	query := fmt.Sprintf("select pos, stop_pos, max_tps, max_replication_lag from _vt.vreplication where id=%v", uid)
	qr, err := dbClient.ExecuteFetch(query, 1)
	if err != nil {
		return "", "", throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("error %v in selecting vreplication settings %v", err, query)
	}

	if qr.RowsAffected != 1 {
		return "", "", throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("checkpoint information not available in db for %v", uid)
	}

	maxTPS, err = sqltypes.ToInt64(qr.Rows[0][2])
	if err != nil {
		return "", "", throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("failed to parse max_tps column: %v", err)
	}
	maxReplicationLag, err = sqltypes.ToInt64(qr.Rows[0][3])
	if err != nil {
		return "", "", throttler.InvalidMaxRate, throttler.InvalidMaxReplicationLag, fmt.Errorf("failed to parse max_replication_lag column: %v", err)
	}

	return qr.Rows[0][0].ToString(), qr.Rows[0][1].ToString(), maxTPS, maxReplicationLag, nil
}

// CreateVReplication returns a statement to populate the first value into
// the _vt.vreplication table.
func CreateVReplication(workflow string, source *binlogdatapb.BinlogSource, position string, maxTPS, maxReplicationLag, timeUpdated int64) string {
	return fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state) "+
		"values (%v, %v, %v, %v, %v, %v, 0, '%v')",
		encodeString(workflow), encodeString(source.String()), encodeString(position), maxTPS, maxReplicationLag, timeUpdated, BlpRunning)
}

// CreateVReplicationStopped returns a statement to create a stopped vreplication.
func CreateVReplicationStopped(workflow string, source *binlogdatapb.BinlogSource, position string) string {
	return fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state) "+
		"values (%v, %v, %v, %v, %v, %v, 0, '%v')",
		encodeString(workflow), encodeString(source.String()), encodeString(position), throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, time.Now().Unix(), BlpStopped)
}

// updateVReplicationPos returns a statement to update a value in the
// _vt.vreplication table.
func updateVReplicationPos(uid uint32, pos mysql.Position, timeUpdated int64, txTimestamp int64) string {
	if txTimestamp != 0 {
		return fmt.Sprintf(
			"update _vt.vreplication set pos=%v, time_updated=%v, transaction_timestamp=%v where id=%v",
			encodeString(mysql.EncodePosition(pos)), timeUpdated, txTimestamp, uid)
	}

	return fmt.Sprintf(
		"update _vt.vreplication set pos=%v, time_updated=%v where id=%v",
		encodeString(mysql.EncodePosition(pos)), timeUpdated, uid)
}

// StartVReplication returns a statement to start the replication.
func StartVReplication(uid uint32) string {
	return fmt.Sprintf(
		"update _vt.vreplication set state='%v', stop_pos=NULL where id=%v",
		BlpRunning, uid)
}

// StartVReplicationUntil returns a statement to start the replication with a stop position.
func StartVReplicationUntil(uid uint32, pos string) string {
	return fmt.Sprintf(
		"update _vt.vreplication set state='%v', stop_pos=%v where id=%v",
		BlpRunning, encodeString(pos), uid)
}

// StopVReplication returns a statement to stop the replication.
func StopVReplication(uid uint32, message string) string {
	return fmt.Sprintf(
		"update _vt.vreplication set state='%v', message=%v where id=%v",
		BlpStopped, encodeString(message), uid)
}

// DeleteVReplication returns a statement to delete the replication.
func DeleteVReplication(uid uint32) string {
	return fmt.Sprintf("delete from _vt.vreplication where id=%v", uid)
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

// ReadVReplicationPos returns a statement to query the gtid for a
// given shard from the _vt.vreplication table.
func ReadVReplicationPos(index uint32) string {
	return fmt.Sprintf("select pos from _vt.vreplication where id=%v", index)
}

// StatsHistoryRecord is used to store a Message with timestamp
type StatsHistoryRecord struct {
	Time    time.Time
	Message string
}

// IsDuplicate implements history.Deduplicable
func (r *StatsHistoryRecord) IsDuplicate(other interface{}) bool {
	return false
}
