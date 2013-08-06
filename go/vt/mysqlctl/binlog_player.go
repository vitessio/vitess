// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/rpcplus"
	estats "github.com/youtube/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
	"github.com/youtube/vitess/go/vt/key"
	cproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var (
	SLOW_TXN_THRESHOLD        = time.Duration(100 * time.Millisecond)
	BLPL_STREAM_COMMENT_START = "/* _stream "
	UPDATE_RECOVERY           = "update _vt.blp_checkpoint set master_filename='%v', master_position=%v, group_id='%v', txn_timestamp=unix_timestamp(), time_updated=%v where keyrange_start='%v' and keyrange_end='%v'"
	SELECT_FROM_RECOVERY      = "select * from _vt.blp_checkpoint where keyrange_start='%v' and keyrange_end='%v'"
)

// binlogRecoveryState is the checkpoint data we read / save into
// _vt.blp_recovery table
type binlogRecoveryState struct {
	KeyrangeStart string //hex string
	KeyrangeEnd   string //hex string
	Addr          string
	Position      cproto.ReplicationCoordinates
}

// VtClient is a high level interface to the database
type VtClient interface {
	Connect() error
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error)
}

// DummyVtClient is a VtClient that writes to a writer instead of executing
// anything
type DummyVtClient struct {
	stdout *bufio.Writer
}

func NewDummyVtClient() *DummyVtClient {
	stdout := bufio.NewWriterSize(os.Stdout, 16*1024)
	return &DummyVtClient{stdout}
}

func (dc DummyVtClient) Connect() error {
	return nil
}

func (dc DummyVtClient) Begin() error {
	dc.stdout.WriteString("BEGIN;\n")
	return nil
}
func (dc DummyVtClient) Commit() error {
	dc.stdout.WriteString("COMMIT;\n")
	return nil
}
func (dc DummyVtClient) Rollback() error {
	dc.stdout.WriteString("ROLLBACK;\n")
	return nil
}
func (dc DummyVtClient) Close() {
	return
}

func (dc DummyVtClient) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error) {
	dc.stdout.WriteString(string(query) + ";\n")
	return nil, nil
}

// DBClient is a real VtClient backed by a mysql connection
type DBClient struct {
	dbConfig *mysql.ConnectionParams
	dbConn   *mysql.Connection
}

func NewDbClient(dbConfig *mysql.ConnectionParams) *DBClient {
	dbClient := &DBClient{}
	dbClient.dbConfig = dbConfig
	return dbClient
}

func (dc *DBClient) handleError(err error) {
	//relog.Error("in DBClient handleError %v", err.(error))
	if sqlErr, ok := err.(*mysql.SqlError); ok {
		if sqlErr.Number() >= 2000 && sqlErr.Number() <= 2018 { // mysql connection errors
			dc.Close()
		}
		if sqlErr.Number() == 1317 { // Query was interrupted
			dc.Close()
		}
	}
}

func (dc *DBClient) Connect() error {
	var err error
	dc.dbConn, err = mysql.Connect(*dc.dbConfig)
	if err != nil {
		return fmt.Errorf("error in connecting to mysql db, err %v", err)
	}
	return nil
}

func (dc *DBClient) Begin() error {
	_, err := dc.dbConn.ExecuteFetch(cproto.BEGIN, 1, false)
	if err != nil {
		relog.Error("BEGIN failed w/ error %v", err)
		dc.handleError(err)
	}
	return err
}

func (dc *DBClient) Commit() error {
	_, err := dc.dbConn.ExecuteFetch(cproto.COMMIT, 1, false)
	if err != nil {
		relog.Error("COMMIT failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *DBClient) Rollback() error {
	_, err := dc.dbConn.ExecuteFetch("rollback", 1, false)
	if err != nil {
		relog.Error("ROLLBACK failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc *DBClient) Close() {
	if dc.dbConn != nil {
		dc.dbConn.Close()
		dc.dbConn = nil
	}
}

func (dc *DBClient) ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		relog.Error("ExecuteFetch failed w/ error %v", err)
		dc.handleError(err)
		return nil, err
	}
	qr := proto.QueryResult(*mqr)
	return &qr, nil
}

// blplStats is the internal stats of this player
type blplStats struct {
	queryCount    *estats.Counters
	txnCount      *estats.Counters
	queriesPerSec *estats.Rates
	txnsPerSec    *estats.Rates
	txnTime       *estats.Timings
	queryTime     *estats.Timings
}

func NewBlplStats() *blplStats {
	bs := &blplStats{}
	bs.txnCount = estats.NewCounters("")
	bs.queryCount = estats.NewCounters("")
	bs.queriesPerSec = estats.NewRates("", bs.queryCount, 15, 60e9)
	bs.txnsPerSec = estats.NewRates("", bs.txnCount, 15, 60e9)
	bs.txnTime = estats.NewTimings("")
	bs.queryTime = estats.NewTimings("")
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
	keyrange       key.KeyRange
	recoveryState  binlogRecoveryState
	rpcClient      *rpcplus.Client
	inTxn          bool
	txnBuffer      []*cproto.BinlogResponse
	dbClient       VtClient
	tables         []string
	txnIndex       int
	batchStart     time.Time
	txnBatch       int
	maxTxnInterval time.Duration
	execDdl        bool
	blplStats      *blplStats
}

func NewBinlogPlayer(dbClient VtClient, startPosition *binlogRecoveryState, tables []string, txnBatch int, maxTxnInterval time.Duration, execDdl bool) (*BinlogPlayer, error) {
	if !startPositionValid(startPosition) {
		relog.Fatal("Invalid Start Position")
	}

	blp := new(BinlogPlayer)
	blp.recoveryState = *startPosition

	// convert start and end keyrange
	var err error
	blp.keyrange.Start, err = key.HexKeyspaceId(startPosition.KeyrangeStart).Unhex()
	if err != nil {
		return nil, fmt.Errorf("Error in Unhex for %v, '%v'", startPosition.KeyrangeStart, err)
	}
	blp.keyrange.End, err = key.HexKeyspaceId(startPosition.KeyrangeEnd).Unhex()
	if err != nil {
		return nil, fmt.Errorf("Error in Unhex for %v, '%v'", startPosition.KeyrangeEnd, err)
	}
	blp.txnIndex = 0
	blp.inTxn = false
	blp.txnBuffer = make([]*cproto.BinlogResponse, 0, MAX_TXN_BATCH)
	blp.dbClient = dbClient
	blp.tables = tables
	blp.blplStats = NewBlplStats()
	blp.batchStart = time.Now()
	blp.txnBatch = txnBatch
	blp.maxTxnInterval = maxTxnInterval
	blp.execDdl = execDdl
	return blp, nil
}

func (blp *BinlogPlayer) StatsJSON() string {
	return blp.blplStats.statsJSON()
}

func (blp *BinlogPlayer) WriteRecoveryPosition(currentPosition *cproto.ReplicationCoordinates) {
	blp.recoveryState.Position = *currentPosition
	updateRecovery := fmt.Sprintf(UPDATE_RECOVERY,
		currentPosition.MasterFilename,
		currentPosition.MasterPosition,
		currentPosition.GroupId,
		time.Now().Unix(),
		blp.recoveryState.KeyrangeStart,
		blp.recoveryState.KeyrangeEnd)

	queryStartTime := time.Now()
	if _, err := blp.dbClient.ExecuteFetch(updateRecovery, 0, false); err != nil {
		panic(fmt.Errorf("Error %v in writing recovery info %v", err, updateRecovery))
	}
	blp.blplStats.txnTime.Record("QueryTime", queryStartTime)
	if time.Now().Sub(queryStartTime) > SLOW_TXN_THRESHOLD {
		relog.Info("SLOW QUERY '%v'", updateRecovery)
	}
}

func startPositionValid(startPos *binlogRecoveryState) bool {
	if startPos.Addr == "" {
		relog.Error("Invalid connection params.")
		return false
	}
	if startPos.Position.MasterFilename == "" || startPos.Position.MasterPosition == 0 {
		relog.Error("Invalid start coordinates.")
		return false
	}
	//One of them can be empty for min or max key.
	if startPos.KeyrangeStart == "" && startPos.KeyrangeEnd == "" {
		relog.Error("Invalid keyrange endpoints.")
		return false
	}
	return true
}

func ReadStartPosition(dbClient VtClient, keyrangeStart, keyrangeEnd string) (*binlogRecoveryState, error) {
	brs := new(binlogRecoveryState)
	brs.KeyrangeStart = keyrangeStart
	brs.KeyrangeEnd = keyrangeEnd

	selectRecovery := fmt.Sprintf(SELECT_FROM_RECOVERY, keyrangeStart, keyrangeEnd)
	qr, err := dbClient.ExecuteFetch(selectRecovery, 1, true)
	if err != nil {
		panic(fmt.Errorf("Error %v in selecting from recovery table %v", err, selectRecovery))
	}
	if qr.RowsAffected != 1 {
		relog.Fatal("Checkpoint information not available in db for %v-%v", keyrangeStart, keyrangeEnd)
	}
	row := qr.Rows[0]
	for i, field := range qr.Fields {
		switch strings.ToLower(field.Name) {
		case "addr":
			val := row[i]
			if !val.IsNull() {
				brs.Addr = val.String()
			}
		case "master_filename":
			val := row[i]
			if !val.IsNull() {
				brs.Position.MasterFilename = val.String()
			}
		case "master_position":
			val := row[i]
			if !val.IsNull() {
				strVal := val.String()
				masterPos, err := strconv.ParseUint(strVal, 0, 64)
				if err != nil {
					return nil, fmt.Errorf("Couldn't obtain correct value for '%v'", field.Name)
				}
				brs.Position.MasterPosition = masterPos
			}
		case "group_id":
			val := row[i]
			if !val.IsNull() {
				brs.Position.GroupId = val.String()
			}
		default:
			continue
		}
	}
	return brs, nil
}

func handleError(err *error, blp *BinlogPlayer) {
	lastTxnPosition := blp.recoveryState.Position
	if x := recover(); x != nil {
		serr, ok := x.(error)
		if ok {
			*err = serr
			relog.Error("Last Txn Position '%v', error %v", lastTxnPosition, serr)
			return
		}
		relog.Error("uncaught panic %v", x)
		panic(x)
	}
}

func (blp *BinlogPlayer) flushTxnBatch() {
	for {
		txnOk := blp.handleTxn()
		if txnOk {
			break
		} else {
			relog.Info("Retrying txn")
			time.Sleep(1)
		}
	}
	blp.inTxn = false
	blp.txnBuffer = blp.txnBuffer[:0]
	blp.txnIndex = 0
}

func (blp *BinlogPlayer) processBinlogEvent(binlogResponse *cproto.BinlogResponse) (err error) {
	defer handleError(&err, blp)

	//Read event
	if binlogResponse.Error != "" {
		//This is to handle the terminal condition where the client is exiting but there
		//maybe pending transactions in the buffer.
		if strings.Contains(binlogResponse.Error, "EOF") {
			relog.Info("Flushing last few txns before exiting, txnIndex %v, len(txnBuffer) %v", blp.txnIndex, len(blp.txnBuffer))
			if blp.txnIndex > 0 && blp.txnBuffer[len(blp.txnBuffer)-1].Data.SqlType == cproto.COMMIT {
				blp.flushTxnBatch()
			}
		}
		if binlogResponse.Position.Position.MasterFilename != "" {
			panic(fmt.Errorf("Error encountered at position %v, err: '%v'", binlogResponse.Position.Position.String(), binlogResponse.Error))
		} else {
			panic(fmt.Errorf("Error encountered from server %v", binlogResponse.Error))
		}
	}

	switch binlogResponse.Data.SqlType {
	case cproto.DDL:
		if blp.txnIndex > 0 {
			relog.Info("Flushing before ddl, Txn Batch %v len %v", blp.txnIndex, len(blp.txnBuffer))
			blp.flushTxnBatch()
		}
		if blp.execDdl {
			blp.handleDdl(binlogResponse)
		}
	case cproto.BEGIN:
		if blp.txnIndex == 0 {
			if blp.inTxn {
				return fmt.Errorf("Invalid txn: txn already in progress, len(blp.txnBuffer) %v", len(blp.txnBuffer))
			}
			blp.txnBuffer = blp.txnBuffer[:0]
			blp.inTxn = true
			blp.batchStart = time.Now()
		}
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)
	case cproto.COMMIT:
		if !blp.inTxn {
			return fmt.Errorf("Invalid event: COMMIT event without a transaction.")
		}
		blp.txnIndex += 1
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)

		if time.Now().Sub(blp.batchStart) > blp.maxTxnInterval || blp.txnIndex == blp.txnBatch {
			//relog.Info("Txn Batch %v len %v", blp.txnIndex, len(blp.txnBuffer))
			blp.flushTxnBatch()
		}
	case cproto.DML:
		if !blp.inTxn {
			return fmt.Errorf("Invalid event: DML outside txn context.")
		}
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)
	default:
		return fmt.Errorf("Unknown SqlType %v '%v'", binlogResponse.Data.SqlType, binlogResponse.Data.Sql)
	}

	return nil
}

//DDL - apply the schema
func (blp *BinlogPlayer) handleDdl(ddlEvent *cproto.BinlogResponse) {
	for _, sql := range ddlEvent.Data.Sql {
		if sql == "" {
			continue
		}
		if _, err := blp.dbClient.ExecuteFetch(sql, 0, false); err != nil {
			panic(fmt.Errorf("Error %v in executing sql %v", err, sql))
		}
	}
	var err error
	if err = blp.dbClient.Begin(); err != nil {
		panic(fmt.Errorf("Failed query BEGIN, err: %s", err))
	}
	blp.WriteRecoveryPosition(&ddlEvent.Position.Position)
	if err = blp.dbClient.Commit(); err != nil {
		panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
	}
}

func (blp *BinlogPlayer) dmlTableMatch(sqlSlice []string) bool {
	if blp.tables == nil {
		return true
	}
	if len(blp.tables) == 0 {
		return true
	}
	var firstKw string
	for _, sql := range sqlSlice {
		firstKw = strings.TrimSpace(strings.Split(sql, " ")[0])
		if firstKw != "insert" && firstKw != "update" && firstKw != "delete" {
			continue
		}
		streamCommentIndex := strings.Index(sql, BLPL_STREAM_COMMENT_START)
		if streamCommentIndex == -1 {
			//relog.Warning("sql doesn't have stream comment '%v'", sql)
			//If sql doesn't have stream comment, don't match
			return false
		}
		tableName := strings.TrimSpace(strings.Split(sql[(streamCommentIndex+len(BLPL_STREAM_COMMENT_START)):], " ")[0])
		for _, table := range blp.tables {
			if tableName == table {
				return true
			}
		}
	}

	return false
}

// Since each batch of txn maybe not contain max txns, we
// flush till the last counter (blp.txnIndex).
// blp.TxnBuffer contains 'n' complete txns, we
// send one begin at the start and then ignore blp.txnIndex - 1 "Commit" events
// and commit the entire batch at the last commit.
func (blp *BinlogPlayer) handleTxn() bool {
	var err error

	dmlMatch := 0
	txnCount := 0
	var queryCount int64
	var txnStartTime, queryStartTime time.Time

	for _, dmlEvent := range blp.txnBuffer {
		switch dmlEvent.Data.SqlType {
		case cproto.BEGIN:
			continue
		case cproto.COMMIT:
			txnCount += 1
			if txnCount < blp.txnIndex {
				continue
			}
			blp.WriteRecoveryPosition(&dmlEvent.Position.Position)
			if err = blp.dbClient.Commit(); err != nil {
				panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
			}
			//added 1 for recovery dml
			queryCount += 2
			blp.blplStats.queryCount.Add("QueryCount", queryCount)
			blp.blplStats.txnCount.Add("TxnCount", int64(blp.txnIndex))
			blp.blplStats.txnTime.Record("TxnTime", txnStartTime)
		case cproto.DML:
			if blp.dmlTableMatch(dmlEvent.Data.Sql) {
				dmlMatch += 1
				if dmlMatch == 1 {
					if err = blp.dbClient.Begin(); err != nil {
						panic(fmt.Errorf("Failed query 'BEGIN', err: %s", err))
					}
					queryCount += 1
					txnStartTime = time.Now()
				}

				for _, sql := range dmlEvent.Data.Sql {
					queryStartTime = time.Now()
					if _, err = blp.dbClient.ExecuteFetch(sql, 0, false); err != nil {
						if sqlErr, ok := err.(*mysql.SqlError); ok {
							// Deadlock found when trying to get lock
							// Rollback this transaction and exit.
							if sqlErr.Number() == 1213 {
								relog.Info("Detected deadlock, returning")
								_ = blp.dbClient.Rollback()
								return false
							}
						}
						panic(err)
					}
					blp.blplStats.txnTime.Record("QueryTime", queryStartTime)
				}
				queryCount += int64(len(dmlEvent.Data.Sql))
			}
		default:
			panic(fmt.Errorf("Invalid SqlType %v", dmlEvent.Data.SqlType))
		}
	}
	return true
}

// ApplyBinlogEvents makes a bson rpc request to BinlogServer
// and processes the events.
func (blp *BinlogPlayer) ApplyBinlogEvents(interrupted chan struct{}) error {
	relog.Info("BinlogPlayer client for keyrange '%v:%v' starting @ '%v'",
		blp.recoveryState.KeyrangeStart,
		blp.recoveryState.KeyrangeEnd,
		blp.recoveryState.Position)

	var err error
	relog.Info("Dialing server @ %v", blp.recoveryState.Addr)
	blp.rpcClient, err = rpcplus.DialHTTP("tcp", blp.recoveryState.Addr)
	defer blp.rpcClient.Close()
	if err != nil {
		relog.Error("Error in dialing to vt_binlog_server, %v", err)
		return fmt.Errorf("Error in dialing to vt_binlog_server, %v", err)
	}

	responseChan := make(chan *cproto.BinlogResponse)
	relog.Info("making rpc request @ %v for keyrange %v:%v", blp.recoveryState.Position, blp.recoveryState.KeyrangeStart, blp.recoveryState.KeyrangeEnd)
	blServeRequest := &cproto.BinlogServerRequest{
		StartPosition: blp.recoveryState.Position,
		KeyspaceStart: blp.recoveryState.KeyrangeStart,
		KeyspaceEnd:   blp.recoveryState.KeyrangeEnd}
	resp := blp.rpcClient.StreamGo("BinlogServer.ServeBinlog", blServeRequest, responseChan)

processLoop:
	for {
		select {
		case response, ok := <-responseChan:
			if !ok {
				break processLoop
			}
			err = blp.processBinlogEvent(response)
			if err != nil {
				return fmt.Errorf("Error in processing binlog event %v", err)
			}
		case <-interrupted:
			return nil
		}
	}
	if resp.Error != nil {
		return fmt.Errorf("Error received from ServeBinlog %v", resp.Error)
	}
	return nil
}
