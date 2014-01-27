// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"bytes"
	"fmt"
	"io"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var (
	SLOW_QUERY_THRESHOLD      = time.Duration(100 * time.Millisecond)
	BLPL_STREAM_COMMENT_START = []byte("/* _stream ")
	BLPL_SPACE                = []byte(" ")
)

// blplStats is the internal stats of this player
type blplStats struct {
	queryCount    *stats.Counters
	txnCount      *stats.Counters
	queriesPerSec *stats.Rates
	txnsPerSec    *stats.Rates
	txnTime       *stats.Timings
	queryTime     *stats.Timings
}

func NewBlplStats() *blplStats {
	bs := &blplStats{}
	bs.txnCount = stats.NewCounters("")
	bs.queryCount = stats.NewCounters("")
	bs.queriesPerSec = stats.NewRates("", bs.queryCount, 15, 60e9)
	bs.txnsPerSec = stats.NewRates("", bs.txnCount, 15, 60e9)
	bs.txnTime = stats.NewTimings("")
	bs.queryTime = stats.NewTimings("")
	return bs
}

// statsJSON returns a json encoded version of stats
func (bs *blplStats) statsJSON() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\n \"TxnCount\": %v,", bs.txnCount)
	fmt.Fprintf(buf, "\n \"QueryCount\": %v,", bs.queryCount)
	fmt.Fprintf(buf, "\n \"QueriesPerSec\": %v,", bs.queriesPerSec)
	fmt.Fprintf(buf, "\n \"TxnPerSec\": %v", bs.txnsPerSec)
	fmt.Fprintf(buf, "\n \"TxnTime\": %v,", bs.txnTime)
	fmt.Fprintf(buf, "\n \"QueryTime\": %v,", bs.queryTime)
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

// BinlogPlayer is handling reading a stream of updates from BinlogServer
type BinlogPlayer struct {
	addr          string
	dbClient      VtClient
	keyRange      key.KeyRange
	tables        []string
	blpPos        myproto.BlpPosition
	stopAtGroupId int64
	blplStats     *blplStats
}

// NewBinlogPlayerKeyRange returns a new BinlogPlayer pointing at the server
// replicating the provided keyrange, starting at the startPosition.GroupId,
// and updating _vt.blp_checkpoint with uid=startPosition.Uid.
// If stopAtGroupId != 0, it will stop when reaching that GroupId.
func NewBinlogPlayerKeyRange(dbClient VtClient, addr string, keyRange key.KeyRange, startPosition *myproto.BlpPosition, stopAtGroupId int64) *BinlogPlayer {
	return &BinlogPlayer{
		addr:          addr,
		dbClient:      dbClient,
		keyRange:      keyRange,
		blpPos:        *startPosition,
		stopAtGroupId: stopAtGroupId,
		blplStats:     NewBlplStats(),
	}
}

// NewBinlogPlayerTables returns a new BinlogPlayer pointing at the server
// replicating the provided tables, starting at the startPosition.GroupId,
// and updating _vt.blp_checkpoint with uid=startPosition.Uid.
// If stopAtGroupId != 0, it will stop when reaching that GroupId.
func NewBinlogPlayerTables(dbClient VtClient, addr string, tables []string, startPosition *myproto.BlpPosition, stopAtGroupId int64) *BinlogPlayer {
	return &BinlogPlayer{
		addr:          addr,
		dbClient:      dbClient,
		tables:        tables,
		blpPos:        *startPosition,
		stopAtGroupId: stopAtGroupId,
		blplStats:     NewBlplStats(),
	}
}

func (blp *BinlogPlayer) StatsJSON() string {
	return blp.blplStats.statsJSON()
}

func (blp *BinlogPlayer) writeRecoveryPosition(groupId int64) error {
	blp.blpPos.GroupId = groupId
	updateRecovery := fmt.Sprintf(
		"update _vt.blp_checkpoint set group_id=%v, time_updated=%v where source_shard_uid=%v",
		groupId,
		time.Now().Unix(),
		blp.blpPos.Uid)

	qr, err := blp.exec(updateRecovery)
	if err != nil {
		return fmt.Errorf("Error %v in writing recovery info %v", err, updateRecovery)
	}
	if qr.RowsAffected != 1 {
		return fmt.Errorf("Cannot update blp_recovery table, affected %v rows", qr.RowsAffected)
	}
	return nil
}

func ReadStartPosition(dbClient VtClient, uid uint32) (*myproto.BlpPosition, error) {
	selectRecovery := fmt.Sprintf(
		"select group_id from _vt.blp_checkpoint where source_shard_uid=%v",
		uid)
	qr, err := dbClient.ExecuteFetch(selectRecovery, 1, true)
	if err != nil {
		return nil, fmt.Errorf("error %v in selecting from recovery table %v", err, selectRecovery)
	}
	if qr.RowsAffected != 1 {
		return nil, fmt.Errorf("checkpoint information not available in db for %v", uid)
	}
	temp, err := qr.Rows[0][0].ParseInt64()
	if err != nil {
		return nil, err
	}
	return &myproto.BlpPosition{
		Uid:     uid,
		GroupId: temp,
	}, nil
}

func (blp *BinlogPlayer) processTransaction(tx *proto.BinlogTransaction) (ok bool, err error) {
	txnStartTime := time.Now()
	if err = blp.dbClient.Begin(); err != nil {
		return false, fmt.Errorf("failed query BEGIN, err: %s", err)
	}
	if err = blp.writeRecoveryPosition(tx.GroupId); err != nil {
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
	blp.blplStats.txnCount.Add("TxnCount", 1)
	blp.blplStats.txnTime.Record("TxnTime", txnStartTime)
	return true, nil
}

func (blp *BinlogPlayer) exec(sql string) (*mproto.QueryResult, error) {
	queryStartTime := time.Now()
	qr, err := blp.dbClient.ExecuteFetch(sql, 0, false)
	blp.blplStats.queryCount.Add("QueryCount", 1)
	blp.blplStats.queryTime.Record("QueryTime", queryStartTime)
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
			blp.blpPos.GroupId,
			blp.addr,
		)
	} else {
		log.Infof("BinlogPlayer client %v for keyrange '%v-%v' starting @ '%v', server: %v",
			blp.blpPos.Uid,
			blp.keyRange.Start.Hex(),
			blp.keyRange.End.Hex(),
			blp.blpPos.GroupId,
			blp.addr,
		)
	}
	if blp.stopAtGroupId > 0 {
		// we need to stop at some point
		// sanity check the point
		if blp.blpPos.GroupId > blp.stopAtGroupId {
			return fmt.Errorf("starting point %v greater than stopping point %v", blp.blpPos.GroupId, blp.stopAtGroupId)
		} else if blp.blpPos.GroupId == blp.stopAtGroupId {
			log.Infof("Not starting BinlogPlayer, we're already at the desired position %v", blp.stopAtGroupId)
			return nil
		}
		log.Infof("Will stop player when reaching %v", blp.stopAtGroupId)
	}

	binlogPlayerClientFactory, ok := binlogPlayerClientFactories[*binlogPlayerProtocol]
	if !ok {
		return fmt.Errorf("no binlog player client factory named %v", *binlogPlayerProtocol)
	}
	rpcClient := binlogPlayerClientFactory()
	err := rpcClient.Dial(blp.addr)
	if err != nil {
		log.Errorf("Error dialing binlog server: %v", err)
		return fmt.Errorf("error dialing binlog server: %v", err)
	}
	defer rpcClient.Close()

	responseChan := make(chan *proto.BinlogTransaction)
	var resp BinlogPlayerResponse
	if len(blp.tables) > 0 {
		req := &proto.TablesRequest{
			Tables:  blp.tables,
			GroupId: blp.blpPos.GroupId,
		}
		resp = rpcClient.StreamTables(req, responseChan)
	} else {
		req := &proto.KeyRangeRequest{
			KeyRange: blp.keyRange,
			GroupId:  blp.blpPos.GroupId,
		}
		resp = rpcClient.StreamKeyRange(req, responseChan)
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
					if blp.stopAtGroupId > 0 && blp.blpPos.GroupId >= blp.stopAtGroupId {
						log.Infof("Reached stopping position, done playing logs")
						return nil
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
