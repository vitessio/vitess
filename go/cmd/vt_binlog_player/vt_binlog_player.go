// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The vt_binlog_player reads data from the a remote host via vt_binlog_server.
This is mostly intended for online data migrations.
*/
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/umgmt"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	cproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/servenv"
)

var interrupted = make(chan struct{})

const (
	TXN_BATCH        = 10
	MAX_TXN_INTERVAL = 30
)

var (
	keyrangeStart  = flag.String("start", "", "keyrange start to use in hex")
	keyrangeEnd    = flag.String("end", "", "keyrange end to use in hex")
	port           = flag.Int("port", 0, "port for the server")
	txnBatch       = flag.Int("txn-batch", TXN_BATCH, "transaction batch size")
	maxTxnInterval = flag.Int("max-txn-interval", MAX_TXN_INTERVAL, "max txn interval")
	dbConfigFile   = flag.String("db-config-file", "", "json file for db credentials")
	debug          = flag.Bool("debug", true, "run a debug version - prints the sql statements rather than executing them")
	tables         = flag.String("tables", "", "tables to play back")
	execDdl        = flag.Bool("exec-ddl", false, "execute ddl")
)

var (
	SLOW_TXN_THRESHOLD   = time.Duration(100 * time.Millisecond)
	BEGIN                = "begin"
	COMMIT               = "commit"
	ROLLBACK             = "rollback"
	STREAM_COMMENT_START = "/* _stream "
	SPACE                = " "
	UPDATE_RECOVERY      = "update _vt.blp_checkpoint set master_filename='%v', master_position=%v, group_id='%v', txn_timestamp=unix_timestamp(), time_updated=%v where keyrange_start='%v' and keyrange_end='%v'"
	SELECT_FROM_RECOVERY = "select * from _vt.blp_checkpoint where uid=keyrange_start='%v' and keyrange_end='%v'"
)

// binlogRecoveryState is the checkpoint data we read / save into
// _vt.blp_recovery table
type binlogRecoveryState struct {
	KeyrangeStart string //hex string
	KeyrangeEnd   string //hex string
	Host          string
	Port          int
	Position      cproto.ReplicationCoordinates
}

type VtClient interface {
	Connect() (*mysql.Connection, error)
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error)
}

type dummyVtClient struct {
	stdout *bufio.Writer
}

func (dc dummyVtClient) Connect() (*mysql.Connection, error) {
	return nil, nil
}

func (dc dummyVtClient) Begin() error {
	dc.stdout.WriteString("BEGIN;\n")
	return nil
}
func (dc dummyVtClient) Commit() error {
	dc.stdout.WriteString("COMMIT;\n")
	return nil
}
func (dc dummyVtClient) Rollback() error {
	dc.stdout.WriteString("ROLLBACK;\n")
	return nil
}
func (dc dummyVtClient) Close() {
	return
}

func (dc dummyVtClient) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error) {
	dc.stdout.WriteString(string(query) + ";\n")
	return nil, nil
}

type DBClient struct {
	dbConfig *mysql.ConnectionParams
	dbConn   *mysql.Connection
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

func (dc DBClient) Connect() (*mysql.Connection, error) {
	return mysql.Connect(*dc.dbConfig)
}

func (dc DBClient) Begin() error {
	_, err := dc.dbConn.ExecuteFetch(BEGIN, 1, false)
	if err != nil {
		relog.Error("BEGIN failed w/ error %v", err)
		dc.handleError(err)
	}
	return err
}

func (dc DBClient) Commit() error {
	_, err := dc.dbConn.ExecuteFetch(COMMIT, 1, false)
	if err != nil {
		relog.Error("COMMIT failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc DBClient) Rollback() error {
	_, err := dc.dbConn.ExecuteFetch(ROLLBACK, 1, false)
	if err != nil {
		relog.Error("ROLLBACK failed w/ error %v", err)
		dc.dbConn.Close()
	}
	return err
}

func (dc DBClient) Close() {
	if dc.dbConn != nil {
		dc.dbConn.Close()
	}
}

func (dc DBClient) ExecuteFetch(query string, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		relog.Error("ExecuteFetch failed w/ error %v", err)
		dc.handleError(err)
		return nil, err
	}
	qr := proto.QueryResult(*mqr)
	return &qr, nil
}

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
	bs.txnCount = stats.NewCounters("TxnCount")
	bs.queryCount = stats.NewCounters("QueryCount")
	bs.queriesPerSec = stats.NewRates("QueriesPerSec", bs.queryCount, 15, 60e9)
	bs.txnsPerSec = stats.NewRates("TxnPerSec", bs.txnCount, 15, 60e9)
	bs.txnTime = stats.NewTimings("TxnTime")
	bs.queryTime = stats.NewTimings("QueryTime")
	return bs
}

type BinlogPlayer struct {
	keyrange       key.KeyRange
	recoveryState  binlogRecoveryState
	rpcClient      *rpcplus.Client
	inTxn          bool
	txnBuffer      []*cproto.BinlogResponse
	dbClient       VtClient
	debug          bool
	tables         []string
	txnIndex       int
	batchStart     time.Time
	txnBatch       int
	maxTxnInterval time.Duration
	execDdl        bool
	blplStats      *blplStats
}

func NewBinlogPlayer(startPosition *binlogRecoveryState) (*BinlogPlayer, error) {
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
	blp.txnBuffer = make([]*cproto.BinlogResponse, 0, mysqlctl.MAX_TXN_BATCH)
	blp.debug = false
	blp.blplStats = NewBlplStats()
	blp.batchStart = time.Now()
	return blp, nil
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

func main() {
	flag.Parse()
	servenv.Init("vt_binlog_player")

	if *dbConfigFile == "" {
		relog.Fatal("Cannot start without db-config-file")
	}

	blp, err := initBinlogPlayer(*dbConfigFile, *debug)
	if err != nil {
		relog.Fatal("Error in initializing binlog player - '%v'", err)
	}
	blp.txnBatch = *txnBatch
	blp.maxTxnInterval = time.Duration(*maxTxnInterval) * time.Second
	blp.execDdl = *execDdl

	if *tables != "" {
		tables := strings.Split(*tables, ",")
		blp.tables = make([]string, len(tables))
		for i, table := range tables {
			blp.tables[i] = strings.TrimSpace(table)
		}
		relog.Info("len tables %v tables %v", len(blp.tables), blp.tables)
	}

	relog.Info("BinlogPlayer client for keyrange '%v:%v' starting @ '%v'",
		blp.recoveryState.KeyrangeStart,
		blp.recoveryState.KeyrangeEnd,
		blp.recoveryState.Position)

	if *port != 0 {
		umgmt.AddStartupCallback(func() {
			umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
		})
	}
	umgmt.AddStartupCallback(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		go func() {
			for sig := range c {
				umgmt.SigTermHandler(sig)
			}
		}()
	})
	umgmt.AddCloseCallback(func() {
		close(interrupted)
	})

	//Make a request to the server and start processing the events.
	err = blp.applyBinlogEvents()
	if err != nil {
		relog.Error("Error in applying binlog events, err %v", err)
	}
	relog.Info("vt_binlog_player done")
}

func startPositionValid(startPos *binlogRecoveryState) bool {
	if startPos.Host == "" || startPos.Port == 0 {
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

func createDbClient(dbConfigFile string) (*DBClient, error) {
	dbConfigData, err := ioutil.ReadFile(dbConfigFile)
	if err != nil {
		return nil, fmt.Errorf("Error %s in reading db-config-file %s", err, dbConfigFile)
	}
	relog.Info("dbConfigData %v", string(dbConfigData))

	dbClient := &DBClient{}
	dbConfig := new(mysql.ConnectionParams)
	err = json.Unmarshal(dbConfigData, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshaling dbconfig data, err '%v'", err)
	}
	dbClient.dbConfig = dbConfig
	dbClient.dbConn, err = dbClient.Connect()
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mysql db, err %v", err)
	}
	return dbClient, nil
}

func getStartPosition(qr *proto.QueryResult, brs *binlogRecoveryState) error {
	row := qr.Rows[0]
	for i, field := range qr.Fields {
		switch strings.ToLower(field.Name) {
		case "host":
			val := row[i]
			if !val.IsNull() {
				brs.Host = val.String()
			}
		case "port":
			val := row[i]
			if !val.IsNull() {
				strVal := val.String()
				port, err := strconv.ParseUint(strVal, 0, 16)
				if err != nil {
					return fmt.Errorf("Couldn't obtain correct value for '%v'", field.Name)
				}
				brs.Port = int(port)
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
					return fmt.Errorf("Couldn't obtain correct value for '%v'", field.Name)
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
	return nil
}

func initBinlogPlayer(dbConfigFile string, debug bool) (*BinlogPlayer, error) {
	dbClient, err := createDbClient(dbConfigFile)
	if err != nil {
		return nil, err
	}

	startPosition := new(binlogRecoveryState)
	startPosition.KeyrangeStart = *keyrangeStart
	startPosition.KeyrangeEnd = *keyrangeEnd

	selectRecovery := fmt.Sprintf(SELECT_FROM_RECOVERY, startPosition.KeyrangeStart, startPosition.KeyrangeEnd)
	qr, err := dbClient.ExecuteFetch(selectRecovery, 1, true)
	if err != nil {
		panic(fmt.Errorf("Error %v in selecting from recovery table %v", err, selectRecovery))
	}
	if qr.RowsAffected != 1 {
		relog.Fatal("Checkpoint information not available in db")
	}
	if err := getStartPosition(qr, startPosition); err != nil {
		relog.Fatal("Error in obtaining checkpoint information")
	}
	if !startPositionValid(startPosition) {
		return nil, fmt.Errorf("Invalid Start Position")
	}

	binlogPlayer, err := NewBinlogPlayer(startPosition)
	if err != nil {
		return nil, err
	}

	if debug {
		stdout := bufio.NewWriterSize(os.Stdout, 16*1024)
		binlogPlayer.debug = true
		binlogPlayer.dbClient = dummyVtClient{stdout}
	} else {
		binlogPlayer.dbClient = *dbClient
	}

	return binlogPlayer, nil
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
			if blp.txnIndex > 0 && blp.txnBuffer[len(blp.txnBuffer)-1].Data.SqlType == mysqlctl.COMMIT {
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
	case mysqlctl.DDL:
		if blp.txnIndex > 0 {
			relog.Info("Flushing before ddl, Txn Batch %v len %v", blp.txnIndex, len(blp.txnBuffer))
			blp.flushTxnBatch()
		}
		if blp.execDdl {
			blp.handleDdl(binlogResponse)
		}
	case mysqlctl.BEGIN:
		if blp.txnIndex == 0 {
			if blp.inTxn {
				return fmt.Errorf("Invalid txn: txn already in progress, len(blp.txnBuffer) %v", len(blp.txnBuffer))
			}
			blp.txnBuffer = blp.txnBuffer[:0]
			blp.inTxn = true
			blp.batchStart = time.Now()
		}
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)
	case mysqlctl.COMMIT:
		if !blp.inTxn {
			return fmt.Errorf("Invalid event: COMMIT event without a transaction.")
		}
		blp.txnIndex += 1
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)

		if time.Now().Sub(blp.batchStart) > blp.maxTxnInterval || blp.txnIndex == blp.txnBatch {
			//relog.Info("Txn Batch %v len %v", blp.txnIndex, len(blp.txnBuffer))
			blp.flushTxnBatch()
		}
	case "insert", "update", "delete":
		if !blp.inTxn {
			return fmt.Errorf("Invalid event: DML outside txn context.")
		}
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)
	default:
		return fmt.Errorf("Unknown SqlType %v", binlogResponse.Data.SqlType, binlogResponse.Data.Sql)
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
		firstKw = strings.TrimSpace(strings.Split(sql, SPACE)[0])
		if firstKw != "insert" && firstKw != "update" && firstKw != "delete" {
			continue
		}
		streamCommentIndex := strings.Index(sql, STREAM_COMMENT_START)
		if streamCommentIndex == -1 {
			//relog.Warning("sql doesn't have stream comment '%v'", sql)
			//If sql doesn't have stream comment, don't match
			return false
		}
		tableName := strings.TrimSpace(strings.Split(sql[(streamCommentIndex+len(STREAM_COMMENT_START)):], SPACE)[0])
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
		case mysqlctl.BEGIN:
			continue
		case mysqlctl.COMMIT:
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
		case "update", "delete", "insert":
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

//This makes a bson rpc request to the vt_binlog_server
//and processes the events.
func (blp *BinlogPlayer) applyBinlogEvents() error {
	var err error
	connectionStr := fmt.Sprintf("%v:%v", blp.recoveryState.Host, blp.recoveryState.Port)
	relog.Info("Dialing server @ %v", connectionStr)
	blp.rpcClient, err = rpcplus.DialHTTP("tcp", connectionStr)
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
