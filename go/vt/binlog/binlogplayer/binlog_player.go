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

// Package binlogplayer contains the code that plays a vreplication
// stream on a client database. It usually runs inside the destination primary
// vttablet process.
package binlogplayer

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
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
	// BlplMultiQuery is the key for the stats map.
	BlplMultiQuery = "MultiQuery"
	// BlplTransaction is the key for the stats map.
	BlplTransaction = "Transaction"
	// BlplBatchTransaction is the key for the stats map.
	BlplBatchTransaction = "BatchTransaction"
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
	lastPosition      replication.Position

	heartbeatMutex sync.Mutex
	heartbeat      int64

	ReplicationLagSeconds atomic.Int64
	History               *history.History

	State atomic.Value

	PhaseTimings       *stats.Timings
	QueryTimings       *stats.Timings
	QueryCount         *stats.CountersWithSingleLabel
	BulkQueryCount     *stats.CountersWithSingleLabel
	TrxQueryBatchCount *stats.CountersWithSingleLabel
	CopyRowCount       *stats.Counter
	CopyLoopCount      *stats.Counter
	ErrorCounts        *stats.CountersWithMultiLabels
	NoopQueryCount     *stats.CountersWithSingleLabel

	VReplicationLags     *stats.Timings
	VReplicationLagRates *stats.Rates

	TableCopyRowCounts *stats.CountersWithSingleLabel
	TableCopyTimings   *stats.Timings

	PartialQueryCount     *stats.CountersWithMultiLabels
	PartialQueryCacheSize *stats.CountersWithMultiLabels

	ThrottledCounts *stats.CountersWithMultiLabels // By throttler and component
}

// RecordHeartbeat updates the time the last heartbeat from vstreamer was seen
func (bps *Stats) RecordHeartbeat(tm int64) {
	bps.heartbeatMutex.Lock()
	defer bps.heartbeatMutex.Unlock()
	bps.heartbeat = tm
}

// Heartbeat gets the time the last heartbeat from vstreamer was seen
func (bps *Stats) Heartbeat() int64 {
	bps.heartbeatMutex.Lock()
	defer bps.heartbeatMutex.Unlock()
	return bps.heartbeat
}

// SetLastPosition sets the last replication position.
func (bps *Stats) SetLastPosition(pos replication.Position) {
	bps.lastPositionMutex.Lock()
	defer bps.lastPositionMutex.Unlock()
	bps.lastPosition = pos
}

// LastPosition gets the last replication position.
func (bps *Stats) LastPosition() replication.Position {
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

func (bps *Stats) Stop() {
	bps.Rates.Stop()
	bps.VReplicationLagRates.Stop()
}

// NewStats creates a new Stats structure.
func NewStats() *Stats {
	bps := &Stats{}
	bps.Timings = stats.NewTimings("", "", "")
	bps.Rates = stats.NewRates("", bps.Timings, 15*60/5, 5*time.Second)
	bps.History = history.New(3)
	bps.ReplicationLagSeconds.Store(math.MaxInt64)
	bps.PhaseTimings = stats.NewTimings("", "", "Phase")
	bps.QueryTimings = stats.NewTimings("", "", "Phase")
	bps.QueryCount = stats.NewCountersWithSingleLabel("", "", "Phase", "")
	bps.BulkQueryCount = stats.NewCountersWithSingleLabel("", "", "Statement", "")
	bps.TrxQueryBatchCount = stats.NewCountersWithSingleLabel("", "", "Statement", "")
	bps.CopyRowCount = stats.NewCounter("", "")
	bps.CopyLoopCount = stats.NewCounter("", "")
	bps.ErrorCounts = stats.NewCountersWithMultiLabels("", "", []string{"type"})
	bps.NoopQueryCount = stats.NewCountersWithSingleLabel("", "", "Statement", "")
	bps.VReplicationLags = stats.NewTimings("", "", "")
	bps.VReplicationLagRates = stats.NewRates("", bps.VReplicationLags, 15*60/5, 5*time.Second)
	bps.TableCopyRowCounts = stats.NewCountersWithSingleLabel("", "", "Table", "")
	bps.TableCopyTimings = stats.NewTimings("", "", "Table")
	bps.PartialQueryCacheSize = stats.NewCountersWithMultiLabels("", "", []string{"type"})
	bps.PartialQueryCount = stats.NewCountersWithMultiLabels("", "", []string{"type"})
	bps.ThrottledCounts = stats.NewCountersWithMultiLabels("", "", []string{"throttler", "component"})
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
	uid            int32
	position       replication.Position
	stopPosition   replication.Position
	blplStats      *Stats
	defaultCharset *binlogdatapb.Charset
	currentCharset *binlogdatapb.Charset
	deadlockRetry  time.Duration
}

// NewBinlogPlayerKeyRange returns a new BinlogPlayer pointing at the server
// replicating the provided keyrange and updating _vt.vreplication
// with uid=startPosition.Uid.
// If !stopPosition.IsZero(), it will stop when reaching that position.
func NewBinlogPlayerKeyRange(dbClient DBClient, tablet *topodatapb.Tablet, keyRange *topodatapb.KeyRange, uid int32, blplStats *Stats) *BinlogPlayer {
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
func NewBinlogPlayerTables(dbClient DBClient, tablet *topodatapb.Tablet, tables []string, uid int32, blplStats *Stats) *BinlogPlayer {
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
// If a stop position was specified, and reached, the state is updated to "Stopped".
func (blp *BinlogPlayer) ApplyBinlogEvents(ctx context.Context) error {
	if err := blp.setVReplicationState(binlogdatapb.VReplicationWorkflowState_Running, ""); err != nil {
		log.Errorf("Error writing Running state: %v", err)
	}

	if err := blp.applyEvents(ctx); err != nil {
		if err := blp.setVReplicationState(binlogdatapb.VReplicationWorkflowState_Error, err.Error()); err != nil {
			log.Errorf("Error writing stop state: %v", err)
		}
		return err
	}
	return nil
}

// applyEvents returns a recordable status message on termination or an error otherwise.
func (blp *BinlogPlayer) applyEvents(ctx context.Context) error {
	// Read starting values for vreplication.
	settings, err := ReadVRSettings(blp.dbClient, blp.uid)
	if err != nil {
		log.Error(err)
		return err
	}

	blp.position = settings.StartPos
	blp.stopPosition = settings.StopPos
	t, err := throttler.NewThrottler(
		fmt.Sprintf("BinlogPlayer/%d", blp.uid),
		"transactions",
		1, /* threadCount */
		settings.MaxTPS,
		settings.MaxReplicationLag,
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
			hex.EncodeToString(blp.keyRange.GetStart()),
			hex.EncodeToString(blp.keyRange.GetEnd()),
			blp.position,
			blp.tablet,
		)
	}
	if !blp.stopPosition.IsZero() {
		switch {
		case blp.position.Equal(blp.stopPosition):
			msg := fmt.Sprintf("not starting BinlogPlayer, we're already at the desired position %v", blp.stopPosition)
			log.Info(msg)
			if err := blp.setVReplicationState(binlogdatapb.VReplicationWorkflowState_Stopped, msg); err != nil {
				log.Errorf("Error writing stop state: %v", err)
			}
			return nil
		case blp.position.AtLeast(blp.stopPosition):
			msg := fmt.Sprintf("starting point %v greater than stopping point %v", blp.position, blp.stopPosition)
			log.Error(msg)
			if err := blp.setVReplicationState(binlogdatapb.VReplicationWorkflowState_Stopped, msg); err != nil {
				log.Errorf("Error writing stop state: %v", err)
			}
			// Don't return an error. Otherwise, it will keep retrying.
			return nil
		default:
			log.Infof("Will stop player when reaching %v", blp.stopPosition)
		}
	}

	clientFactory, ok := clientFactories[binlogPlayerProtocol]
	if !ok {
		return fmt.Errorf("no binlog player client factory named %v", binlogPlayerProtocol)
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
		stream, err = blplClient.StreamTables(ctx, replication.EncodePosition(blp.position), blp.tables, blp.defaultCharset)
	} else {
		stream, err = blplClient.StreamKeyRange(ctx, replication.EncodePosition(blp.position), blp.keyRange, blp.defaultCharset)
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
			blp.blplStats.ThrottledCounts.Add([]string{"trx", "binlogplayer"}, 1)
			// We don't bother checking for context cancellation here because the
			// sleep will block only up to 1 second. (Usually, backoff is 1s / rate
			// e.g. a rate of 1000 TPS results into a backoff of 1 ms.)
			time.Sleep(backoff)
		}

		// Get the response.
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
						if err := blp.setVReplicationState(binlogdatapb.VReplicationWorkflowState_Stopped, msg); err != nil {
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
				log.Warningf("BinlogPlayer changing charset from %v to %v for statement %d in transaction %v", blp.currentCharset, stmtCharset, i, tx)
				err = mysql.SetCharset(dbClient.dbConn, stmtCharset)
				if err != nil {
					return false, fmt.Errorf("can't set charset for statement %d in transaction %v: %v", i, tx, err)
				}
				blp.currentCharset = stmtCharset
			}
		}
		if _, err = blp.exec(string(stmt.Sql)); err == nil {
			continue
		}
		if sqlErr, ok := err.(*sqlerror.SQLError); ok && sqlErr.Number() == sqlerror.ERLockDeadlock {
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
	if d := time.Since(queryStartTime); d > SlowQueryThreshold {
		log.Infof("SLOW QUERY (took %.2fs) '%s'", d.Seconds(), sql)
	}
	return qr, err
}

// writeRecoveryPosition writes the current GTID as the recovery position
// for the next transaction.
// It also tries to get the timestamp for the transaction. Two cases:
//   - we have statements, and they start with a SET TIMESTAMP that we
//     can parse: then we update transaction_timestamp in vreplication
//     with it, and set ReplicationLagSeconds to now() - transaction_timestamp
//   - otherwise (the statements are probably filtered out), we leave
//     transaction_timestamp alone (keeping the old value), and we don't
//     change ReplicationLagSeconds
func (blp *BinlogPlayer) writeRecoveryPosition(tx *binlogdatapb.BinlogTransaction) error {
	position, err := DecodePosition(tx.EventToken.Position)
	if err != nil {
		return err
	}

	now := time.Now().Unix()
	updateRecovery := GenerateUpdatePos(blp.uid, position, now, tx.EventToken.Timestamp, blp.blplStats.CopyRowCount.Get(), false)

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
		blp.blplStats.ReplicationLagSeconds.Store(now - tx.EventToken.Timestamp)
	}
	return nil
}

func (blp *BinlogPlayer) setVReplicationState(state binlogdatapb.VReplicationWorkflowState, message string) error {
	if message != "" {
		blp.blplStats.History.Add(&StatsHistoryRecord{
			Time:    time.Now(),
			Message: message,
		})
	}
	blp.blplStats.State.Store(state.String())
	query := fmt.Sprintf("update _vt.vreplication set state='%v', message=%v where id=%v", state.String(), encodeString(MessageTruncate(message)), blp.uid)
	if _, err := blp.dbClient.ExecuteFetch(query, 1); err != nil {
		return fmt.Errorf("could not set state: %v: %v", query, err)
	}
	return nil
}

// VRSettings contains the settings of a vreplication table.
type VRSettings struct {
	StartPos           replication.Position
	StopPos            replication.Position
	MaxTPS             int64
	MaxReplicationLag  int64
	State              binlogdatapb.VReplicationWorkflowState
	WorkflowType       binlogdatapb.VReplicationWorkflowType
	WorkflowSubType    binlogdatapb.VReplicationWorkflowSubType
	WorkflowName       string
	DeferSecondaryKeys bool
}

// ReadVRSettings retrieves the throttler settings for
// vreplication from the checkpoint table.
func ReadVRSettings(dbClient DBClient, uid int32) (VRSettings, error) {
	query := fmt.Sprintf("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=%v", uid)
	qr, err := dbClient.ExecuteFetch(query, 1)
	if err != nil {
		return VRSettings{}, fmt.Errorf("error %v in selecting vreplication settings %v", err, query)
	}

	if len(qr.Rows) != 1 {
		return VRSettings{}, fmt.Errorf("checkpoint information not available in db for %v", uid)
	}
	vrRow := qr.Named().Row()

	maxTPS, err := vrRow.ToInt64("max_tps")
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse max_tps column: %v", err)
	}
	maxReplicationLag, err := vrRow.ToInt64("max_replication_lag")
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse max_replication_lag column: %v", err)
	}
	startPos, err := DecodePosition(vrRow.AsString("pos", ""))
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse pos column: %v", err)
	}
	stopPos, err := replication.DecodePosition(vrRow.AsString("stop_pos", ""))
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse stop_pos column: %v", err)
	}
	workflowType, err := vrRow.ToInt32("workflow_type")
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse workflow_type column: %v", err)
	}
	workflowSubType, err := vrRow.ToInt32("workflow_sub_type")
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse workflow_sub_type column: %v", err)
	}
	deferSecondaryKeys, err := vrRow.ToBool("defer_secondary_keys")
	if err != nil {
		return VRSettings{}, fmt.Errorf("failed to parse defer_secondary_keys column: %v", err)
	}
	return VRSettings{
		StartPos:           startPos,
		StopPos:            stopPos,
		MaxTPS:             maxTPS,
		MaxReplicationLag:  maxReplicationLag,
		State:              binlogdatapb.VReplicationWorkflowState(binlogdatapb.VReplicationWorkflowState_value[vrRow.AsString("state", "")]),
		WorkflowType:       binlogdatapb.VReplicationWorkflowType(workflowType),
		WorkflowName:       vrRow.AsString("workflow", ""),
		WorkflowSubType:    binlogdatapb.VReplicationWorkflowSubType(workflowSubType),
		DeferSecondaryKeys: deferSecondaryKeys,
	}, nil
}

// CreateVReplication returns a statement to populate the first value into
// the _vt.vreplication table.
func CreateVReplication(workflow string, source *binlogdatapb.BinlogSource, position string, maxTPS, maxReplicationLag, timeUpdated int64, dbName string,
	workflowType binlogdatapb.VReplicationWorkflowType, workflowSubType binlogdatapb.VReplicationWorkflowSubType, deferSecondaryKeys bool) string {
	protoutil.SortBinlogSourceTables(source)
	return fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys) "+
		"values (%v, %v, %v, %v, %v, %v, 0, '%v', %v, %d, %d, %v)",
		encodeString(workflow), encodeString(source.String()), encodeString(position), maxTPS, maxReplicationLag,
		timeUpdated, binlogdatapb.VReplicationWorkflowState_Running.String(), encodeString(dbName), workflowType, workflowSubType, deferSecondaryKeys)
}

// CreateVReplicationState returns a statement to create a stopped vreplication.
func CreateVReplicationState(workflow string, source *binlogdatapb.BinlogSource, position string, state binlogdatapb.VReplicationWorkflowState, dbName string,
	workflowType binlogdatapb.VReplicationWorkflowType, workflowSubType binlogdatapb.VReplicationWorkflowSubType) string {
	protoutil.SortBinlogSourceTables(source)
	return fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type) "+
		"values (%v, %v, %v, %v, %v, %v, 0, '%v', %v, %d, %d)",
		encodeString(workflow), encodeString(source.String()), encodeString(position), throttler.MaxRateModuleDisabled,
		throttler.ReplicationLagModuleDisabled, time.Now().Unix(), state.String(), encodeString(dbName),
		workflowType, workflowSubType)
}

// GenerateUpdatePos returns a statement to record the latest processed gtid in the _vt.vreplication table.
func GenerateUpdatePos(uid int32, pos replication.Position, timeUpdated int64, txTimestamp int64, rowsCopied int64, compress bool) string {
	strGTID := encodeString(replication.EncodePosition(pos))
	if compress {
		strGTID = fmt.Sprintf("compress(%s)", strGTID)
	}
	if txTimestamp != 0 {
		return fmt.Sprintf(
			"update _vt.vreplication set pos=%v, time_updated=%v, transaction_timestamp=%v, rows_copied=%v, message='' where id=%v",
			strGTID, timeUpdated, txTimestamp, rowsCopied, uid)
	}
	return fmt.Sprintf(
		"update _vt.vreplication set pos=%v, time_updated=%v, rows_copied=%v, message='' where id=%v", strGTID, timeUpdated, rowsCopied, uid)
}

// GenerateUpdateRowsCopied returns a statement to update the rows_copied value in the _vt.vreplication table.
func GenerateUpdateRowsCopied(uid int32, rowsCopied int64) string {
	return fmt.Sprintf("update _vt.vreplication set rows_copied=%v where id=%v", rowsCopied, uid)
}

// GenerateUpdateHeartbeat returns a statement to record the latest heartbeat in the _vt.vreplication table.
func GenerateUpdateHeartbeat(uid int32, timeUpdated int64) (string, error) {
	if timeUpdated == 0 {
		return "", fmt.Errorf("timeUpdated cannot be zero")
	}
	return fmt.Sprintf("update _vt.vreplication set time_updated=%v, time_heartbeat=%v where id=%v", timeUpdated, timeUpdated, uid), nil
}

// GenerateUpdateTimeThrottled returns a statement to record the latest throttle time in the _vt.vreplication table.
func GenerateUpdateTimeThrottled(uid int32, timeThrottledUnix int64, componentThrottled string) (string, error) {
	if timeThrottledUnix == 0 {
		return "", fmt.Errorf("timeUpdated cannot be zero")
	}
	return fmt.Sprintf("update _vt.vreplication set time_updated=%v, time_throttled=%v, component_throttled='%v' where id=%v", timeThrottledUnix, timeThrottledUnix, componentThrottled, uid), nil
}

// StartVReplicationUntil returns a statement to start the replication with a stop position.
func StartVReplicationUntil(uid int32, pos string) string {
	return fmt.Sprintf(
		"update _vt.vreplication set state='%v', stop_pos=%v where id=%v",
		binlogdatapb.VReplicationWorkflowState_Running.String(), encodeString(pos), uid)
}

// StopVReplication returns a statement to stop the replication.
func StopVReplication(uid int32, message string) string {
	return fmt.Sprintf(
		"update _vt.vreplication set state='%v', message=%v where id=%v",
		binlogdatapb.VReplicationWorkflowState_Stopped.String(), encodeString(MessageTruncate(message)), uid)
}

// DeleteVReplication returns a statement to delete the replication.
func DeleteVReplication(uid int32) string {
	return fmt.Sprintf("delete from _vt.vreplication where id=%v", uid)
}

// MessageTruncate truncates the message string to a safe length.
func MessageTruncate(msg string) string {
	// message length is 1000 bytes.
	return LimitString(msg, 950)
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

// ReadVReplicationPos returns a statement to query the gtid for a
// given stream from the _vt.vreplication table.
func ReadVReplicationPos(index int32) string {
	return fmt.Sprintf("select pos from _vt.vreplication where id=%v", index)
}

// ReadVReplicationStatus returns a statement to query the status fields for a
// given stream from the _vt.vreplication table.
func ReadVReplicationStatus(index int32) string {
	return fmt.Sprintf("select pos, state, message from _vt.vreplication where id=%v", index)
}

// MysqlUncompress will uncompress a binary string in the format stored by mysql's compress() function
// The first four bytes represent the size of the original string passed to compress()
// Remaining part is the compressed string using zlib, which we uncompress here using golang's zlib library
func MysqlUncompress(input string) []byte {
	// consistency check
	inputBytes := []byte(input)
	if len(inputBytes) < 5 {
		return nil
	}

	// determine length
	dataLength := uint32(inputBytes[0]) + uint32(inputBytes[1])<<8 + uint32(inputBytes[2])<<16 + uint32(inputBytes[3])<<24
	dataLengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataLengthBytes, dataLength)
	dataLength = binary.LittleEndian.Uint32(dataLengthBytes)

	// uncompress using zlib
	inputData := inputBytes[4:]
	inputDataBuf := bytes.NewBuffer(inputData)
	reader, err := zlib.NewReader(inputDataBuf)
	if err != nil {
		return nil
	}
	var outputBytes bytes.Buffer
	if _, err := io.Copy(&outputBytes, reader); err != nil {
		return nil
	}
	if outputBytes.Len() == 0 {
		return nil
	}
	if dataLength != uint32(outputBytes.Len()) { // double check that the stored and uncompressed lengths match
		return nil
	}
	return outputBytes.Bytes()
}

// DecodePosition attempts to uncompress the passed value first and if it fails tries to decode it as a valid GTID
func DecodePosition(gtid string) (replication.Position, error) {
	b := MysqlUncompress(gtid)
	if b != nil {
		gtid = string(b)
	}
	return replication.DecodePosition(gtid)
}

// StatsHistoryRecord is used to store a Message with timestamp
type StatsHistoryRecord struct {
	Time    time.Time
	Message string
}

// IsDuplicate implements history.Deduplicable
func (r *StatsHistoryRecord) IsDuplicate(other any) bool {
	return false
}

const binlogPlayerProtocolFlagName = "binlog_player_protocol"

// SetProtocol is a helper function to set the binlogplayer --binlog_player_protocol
// flag value for tests. If successful, it returns a function that, when called,
// returns the flag to its previous value.
//
// Note that because this variable is bound to a flag, the effects of this
// function are global, not scoped to the calling test-case. Therefore, it should
// not be used in conjunction with t.Parallel.
func SetProtocol(name string, protocol string) (reset func()) {
	var tmp []string
	tmp, os.Args = os.Args[:], []string{name}
	defer func() { os.Args = tmp }()

	servenv.OnParseFor(name, func(fs *pflag.FlagSet) {
		if fs.Lookup(binlogPlayerProtocolFlagName) != nil {
			return
		}

		registerFlags(fs)
	})
	servenv.ParseFlags(name)

	switch oldVal, err := pflag.CommandLine.GetString(binlogPlayerProtocolFlagName); err {
	case nil:
		reset = func() { SetProtocol(name, oldVal) }
	default:
		log.Errorf("failed to get string value for flag %q: %v", binlogPlayerProtocolFlagName, err)
		reset = func() {}
	}

	if err := pflag.Set(binlogPlayerProtocolFlagName, protocol); err != nil {
		msg := "failed to set flag %q to %q: %v"
		log.Errorf(msg, binlogPlayerProtocolFlagName, protocol, err)
		reset = func() {}
	}

	return reset
}
