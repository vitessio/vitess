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

	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/servenv"
)

var stdout *bufio.Writer
var interrupted = make(chan struct{})

const (
	TXN_BATCH        = 10
	MAX_TXN_INTERVAL = 30
	SERVER_PORT      = 6614
)

var (
	port             = flag.Int("port", 0, "port for the server")
	txnBatch         = flag.Int("txn-batch", TXN_BATCH, "transaction batch size")
	maxTxnInterval   = flag.Int("max-txn-interval", MAX_TXN_INTERVAL, "max txn interval")
	startPosFile     = flag.String("start-pos-file", "", "server address and start coordinates")
	useCheckpoint    = flag.Bool("use-checkpoint", false, "use the saved checkpoint to start")
	dbConfigFile     = flag.String("db-config-file", "", "json file for db credentials")
	lookupConfigFile = flag.String("lookup-config-file", "", "json file for lookup db credentials")
	debug            = flag.Bool("debug", true, "run a debug version - prints the sql statements rather than executing them")
	tables           = flag.String("tables", "", "tables to play back")
	dbCredFile       = flag.String("db-credentials-file", "", "db-creditials file to look up passwd to connect to lookup host")
	execDdl          = flag.Bool("exec-ddl", false, "execute ddl")
)

var (
	SLOW_TXN_THRESHOLD    = time.Duration(100 * time.Millisecond)
	BEGIN                 = "begin"
	COMMIT                = "commit"
	ROLLBACK              = "rollback"
	USERNAME_INDEX_INSERT = "insert into vt_username_map (username, user_id) values ('%v', %v)"
	USERNAME_INDEX_UPDATE = "update vt_username_map set username='%v' where user_id=%v"
	USERNAME_INDEX_DELETE = "delete from vt_username_map where username='%v' and user_id=%v"
	VIDEOID_INDEX_INSERT  = "insert into vt_video_id_map (video_id, user_id) values (%v, %v)"
	VIDEOID_INDEX_DELETE  = "delete from vt_video_id_map where video_id=%v and user_id=%v"
	SETID_INDEX_INSERT    = "insert into vt_set_id_map (set_id, user_id) values (%v, %v)"
	SETID_INDEX_DELETE    = "delete from vt_set_id_map where set_id=%v and user_id=%v"
	STREAM_COMMENT_START  = "/* _stream "
	SPACE                 = " "
	USE_VT                = "use _vt"
	USE_DB                = "use %v"
	INSERT_INTO_RECOVERY  = `insert into _vt.blp_checkpoint (uid, host, port, master_filename, master_position, relay_filename, relay_position, group_id, keyrange_start, keyrange_end, txn_timestamp, time_updated) 
	                          values (%v, '%v', %v, '%v', %v, '%v', %v, %v, '%v', '%v', unix_timestamp(), %v)`
	UPDATE_RECOVERY      = "update _vt.blp_checkpoint set master_filename='%v', master_position=%v, relay_filename='%v', relay_position=%v, group_id=%v, txn_timestamp=unix_timestamp(), time_updated=%v where uid=%v"
	UPDATE_PORT          = "update _vt.blp_checkpoint set port=%v where uid=%v"
	SELECT_FROM_RECOVERY = "select * from _vt.blp_checkpoint where uid=%v"
)

/*
{
 "Uid: : <tabet uid>",
 "Host": "<vt_binlog_server host>>",
 "Port": <vt_binlog_server port>,
 "startPosition": "MasterFilename:dbXX.000123-bin.000123, MasterPosition:1234567",
 "KeyrangeStart": "1000000000000000",
 "KeyrangeEnd": "2000000000000000",
 }
*/
type binlogRecoveryState struct {
	Uid           uint32
	Host          string
	Port          int
	Position      mysqlctl.ReplicationCoordinates
	KeyrangeStart string //hex string
	KeyrangeEnd   string //hex string
}

type VtClient interface {
	Connect() (*mysql.Connection, error)
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error)
}

type dummyVtClient struct{}

func (dc dummyVtClient) Connect() (*mysql.Connection, error) {
	return nil, nil
}

func (dc dummyVtClient) Begin() error {
	stdout.WriteString("BEGIN;\n")
	return nil
}
func (dc dummyVtClient) Commit() error {
	stdout.WriteString("COMMIT;\n")
	return nil
}
func (dc dummyVtClient) Rollback() error {
	stdout.WriteString("ROLLBACK;\n")
	return nil
}
func (dc dummyVtClient) Close() {
	return
}

func (dc dummyVtClient) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error) {
	stdout.WriteString(string(query) + ";\n")
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

type blpStats struct {
	queryCount    *stats.Counters
	txnCount      *stats.Counters
	queriesPerSec *stats.Rates
	txnsPerSec    *stats.Rates
	txnTime       *stats.Timings
	queryTime     *stats.Timings
	lookupTxn     *stats.Timings
}

func NewBlpStats() *blpStats {
	bs := &blpStats{}
	bs.txnCount = stats.NewCounters("TxnCount")
	bs.queryCount = stats.NewCounters("QueryCount")
	bs.queriesPerSec = stats.NewRates("QueriesPerSec", bs.queryCount, 15, 60e9)
	bs.txnsPerSec = stats.NewRates("TxnPerSec", bs.txnCount, 15, 60e9)
	bs.txnTime = stats.NewTimings("TxnTime")
	bs.queryTime = stats.NewTimings("QueryTime")
	bs.lookupTxn = stats.NewTimings("LookupTxn")
	return bs
}

type BinlogPlayer struct {
	keyrange       key.KeyRange
	keyrangeTag    string
	recoveryState  *binlogRecoveryState
	startPosition  *binlogRecoveryState
	rpcClient      *rpcplus.Client
	inTxn          bool
	txnBuffer      []*mysqlctl.BinlogResponse
	dbClient       VtClient
	lookupClient   VtClient
	debug          bool
	tables         []string
	txnIndex       int
	batchStart     time.Time
	txnBatch       int
	maxTxnInterval time.Duration
	execDdl        bool
	*blpStats
}

func NewBinlogPlayer(startPosition *binlogRecoveryState, port int, krStart, krEnd key.KeyspaceId) *BinlogPlayer {
	blp := new(BinlogPlayer)
	blp.startPosition = startPosition
	blp.recoveryState = &binlogRecoveryState{Uid: blp.startPosition.Uid,
		Host:          blp.startPosition.Host,
		Port:          port,
		Position:      blp.startPosition.Position,
		KeyrangeStart: blp.startPosition.KeyrangeStart,
		KeyrangeEnd:   blp.startPosition.KeyrangeEnd}
	blp.keyrange.Start = krStart
	blp.keyrange.End = krEnd
	blp.keyrangeTag = blp.startPosition.KeyrangeEnd
	if blp.keyrangeTag == "" {
		blp.keyrangeTag = "MAX"
	}

	blp.txnIndex = 0
	blp.inTxn = false
	blp.txnBuffer = make([]*mysqlctl.BinlogResponse, 0, mysqlctl.MAX_TXN_BATCH)
	blp.debug = false
	blp.blpStats = NewBlpStats()
	blp.batchStart = time.Now()
	return blp
}

func (blp *BinlogPlayer) updatePort(port int, uid uint32, useDb string) {
	updatePortSql := fmt.Sprintf(UPDATE_PORT, port, uid)
	sqlList := []string{USE_VT, "begin", updatePortSql, "commit", useDb}

	for _, sql := range sqlList {
		if _, err := blp.dbClient.ExecuteFetch(sql, 0, false); err != nil {
			panic(fmt.Errorf("Error %v in writing port %v", err, sql))
		}
	}
}

func (blp *BinlogPlayer) WriteRecoveryPosition(currentPosition *mysqlctl.ReplicationCoordinates, groupId uint64) {
	blp.recoveryState.Position = *currentPosition
	updateRecovery := fmt.Sprintf(UPDATE_RECOVERY, currentPosition.MasterFilename,
		currentPosition.MasterPosition,
		currentPosition.RelayFilename,
		currentPosition.RelayPosition,
		groupId,
		time.Now().Unix(),
		blp.recoveryState.Uid)

	queryStartTime := time.Now()
	if _, err := blp.dbClient.ExecuteFetch(updateRecovery, 0, false); err != nil {
		panic(fmt.Errorf("Error %v in writing recovery info %v", err, updateRecovery))
	}
	blp.txnTime.Record("QueryTime", queryStartTime)
	if time.Now().Sub(queryStartTime) > SLOW_TXN_THRESHOLD {
		relog.Info("SLOW QUERY '%v'", updateRecovery)
	}
}

func main() {
	flag.Parse()
	servenv.Init("vt_binlog_player")

	if *startPosFile == "" {
		relog.Fatal("start-pos-file was not supplied.")
	}

	if *dbConfigFile == "" {
		relog.Fatal("Cannot start without db-config-file")
	}

	blp, err := initBinlogPlayer(*startPosFile, *dbConfigFile, *lookupConfigFile, *dbCredFile, *useCheckpoint, *debug, *port)
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
		blp.startPosition.KeyrangeStart,
		blp.startPosition.KeyrangeEnd,
		blp.startPosition.Position)

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
	stdout = bufio.NewWriterSize(os.Stdout, 16*1024)
	err = blp.applyBinlogEvents()
	if err != nil {
		relog.Error("Error in applying binlog events, err %v", err)
	}
	relog.Info("vt_binlog_player done")
}

func startPositionValid(startPos *binlogRecoveryState) bool {
	if startPos.Uid == 0 {
		relog.Error("Missing Uid")
		return false
	}
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

func createLookupClient(lookupConfigFile, dbCredFile string) (*DBClient, error) {
	lookupConfigData, err := ioutil.ReadFile(lookupConfigFile)
	if err != nil {
		return nil, fmt.Errorf("Error %s in reading lookup-config-file %s", err, lookupConfigFile)
	}

	lookupClient := &DBClient{}
	lookupConfig := new(mysql.ConnectionParams)
	err = json.Unmarshal(lookupConfigData, lookupConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshaling lookupConfig data, err '%v'", err)
	}

	var lookupPasswd string
	if dbCredFile != "" {
		dbCredentials := make(map[string][]string)
		dbCredData, err := ioutil.ReadFile(dbCredFile)
		if err != nil {
			return nil, fmt.Errorf("Error %s in reading db-credentials-file %s", err, dbCredFile)
		}
		err = json.Unmarshal(dbCredData, &dbCredentials)
		if err != nil {
			return nil, fmt.Errorf("Error in unmarshaling db-credentials-file %s", err)
		}
		if passwd, ok := dbCredentials[lookupConfig.Uname]; ok {
			lookupPasswd = passwd[0]
		}
	}

	lookupConfig.Pass = lookupPasswd
	relog.Info("lookupConfig %v", lookupConfig)
	lookupClient.dbConfig = lookupConfig

	lookupClient.dbConn, err = lookupClient.Connect()
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mysql db, err %v", err)
	}
	return lookupClient, nil
}

func getStartPosition(qr *proto.QueryResult) (*mysqlctl.ReplicationCoordinates, error) {
	startPosition := &mysqlctl.ReplicationCoordinates{}
	row := qr.Rows[0]
	for i, field := range qr.Fields {
		switch strings.ToLower(field.Name) {
		case "master_filename":
			val := row[i]
			if !val.IsNull() {
				startPosition.MasterFilename = val.String()
			}
		case "master_position":
			val := row[i]
			if !val.IsNull() {
				strVal := val.String()
				masterPos, err := strconv.ParseUint(strVal, 0, 64)
				if err != nil {
					return nil, fmt.Errorf("Couldn't obtain correct value for '%v'", field.Name)
				}
				startPosition.MasterPosition = masterPos
			}
		case "relay_filename":
			val := row[i]
			if !val.IsNull() {
				startPosition.RelayFilename = val.String()
			}
		case "relay_position":
			val := row[i]
			if !val.IsNull() {
				strVal := val.String()
				relayPos, err := strconv.ParseUint(strVal, 0, 64)
				if err != nil {
					return nil, fmt.Errorf("Couldn't obtain correct value for '%v'", field.Name)
				}
				startPosition.RelayPosition = relayPos
			}
		default:
			continue
		}
	}
	return startPosition, nil
}

func initBinlogPlayer(startPosFile, dbConfigFile, lookupConfigFile, dbCredFile string, useCheckpoint, debug bool, port int) (*BinlogPlayer, error) {
	startData, err := ioutil.ReadFile(startPosFile)
	if err != nil {
		return nil, fmt.Errorf("Error %s in reading start position file %s", err, startPosFile)
	}
	startPosition := new(binlogRecoveryState)
	err = json.Unmarshal(startData, startPosition)
	if err != nil {
		return nil, fmt.Errorf("Error in unmarshaling recovery data: %s, startData %v", err, string(startData))
	}

	dbClient, err := createDbClient(dbConfigFile)
	if err != nil {
		return nil, err
	}
	if useCheckpoint {
		selectRecovery := fmt.Sprintf(SELECT_FROM_RECOVERY, startPosition.Uid)
		qr, err := dbClient.ExecuteFetch(selectRecovery, 1, true)
		if err != nil {
			panic(fmt.Errorf("Error %v in selecting from recovery table %v", err, selectRecovery))
		}
		if qr.RowsAffected != 1 {
			relog.Fatal("Checkpoint information not available in db")
		}
		startCoord, err := getStartPosition(qr)
		if err != nil {
			relog.Fatal("Error in obtaining checkpoint information")
		}
		startPosition.Position = *startCoord
	}

	if !startPositionValid(startPosition) {
		return nil, fmt.Errorf("Invalid Start Position")
	}

	krStart, err := key.HexKeyspaceId(startPosition.KeyrangeStart).Unhex()
	if err != nil {
		return nil, fmt.Errorf("Error in Unhex for %v, '%v'", startPosition.KeyrangeStart, err)
	}
	krEnd, err := key.HexKeyspaceId(startPosition.KeyrangeEnd).Unhex()
	if err != nil {
		return nil, fmt.Errorf("Error in Unhex for %v, '%v'", startPosition.KeyrangeEnd, err)
	}

	binlogPlayer := NewBinlogPlayer(startPosition, port, krStart, krEnd)

	if debug {
		binlogPlayer.debug = true
		binlogPlayer.dbClient = dummyVtClient{}
		binlogPlayer.lookupClient = dummyVtClient{}
	} else {
		binlogPlayer.dbClient = *dbClient

		lookupClient, err := createLookupClient(lookupConfigFile, dbCredFile)
		if err != nil {
			return nil, err
		}
		binlogPlayer.lookupClient = *lookupClient

		if !useCheckpoint {
			initialize_recovery_table(dbClient, startPosition, port)
		}

		useDb := fmt.Sprintf(USE_DB, dbClient.dbConfig.Dbname)
		binlogPlayer.updatePort(port, startPosition.Uid, useDb)
	}

	return binlogPlayer, nil
}

func initialize_recovery_table(dbClient *DBClient, startPosition *binlogRecoveryState, port int) {
	useDb := fmt.Sprintf(USE_DB, dbClient.dbConfig.Dbname)

	selectRecovery := fmt.Sprintf(SELECT_FROM_RECOVERY, startPosition.Uid)
	qr, err := dbClient.ExecuteFetch(selectRecovery, 1, true)
	if err != nil {
		panic(fmt.Errorf("Error %v in selecting from recovery table %v", err, selectRecovery))
	}
	if qr.RowsAffected == 0 {
		insertRecovery := fmt.Sprintf(INSERT_INTO_RECOVERY, startPosition.Uid, startPosition.Host,
			port,
			startPosition.Position.MasterFilename,
			startPosition.Position.MasterPosition,
			startPosition.Position.RelayFilename,
			startPosition.Position.RelayPosition,
			0,
			startPosition.KeyrangeStart,
			startPosition.KeyrangeEnd,
			time.Now().Unix())
		recoveryDmls := []string{USE_VT, "begin", insertRecovery, "commit", useDb}
		for _, sql := range recoveryDmls {
			if _, err := dbClient.ExecuteFetch(sql, 0, false); err != nil {
				panic(fmt.Errorf("Error %v in inserting into recovery table %v", err, sql))
			}
		}
	}
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

func (blp *BinlogPlayer) processBinlogEvent(binlogResponse *mysqlctl.BinlogResponse) (err error) {
	defer handleError(&err, blp)

	//Read event
	if binlogResponse.Error != "" {
		//This is to handle the terminal condition where the client is exiting but there
		//maybe pending transactions in the buffer.
		if strings.Contains(binlogResponse.Error, "EOF") {
			relog.Info("Flushing last few txns before exiting, txnIndex %v, len(txnBuffer) %v", blp.txnIndex, len(blp.txnBuffer))
			if blp.txnIndex > 0 && blp.txnBuffer[len(blp.txnBuffer)-1].SqlType == mysqlctl.COMMIT {
				blp.flushTxnBatch()
			}
		}
		if binlogResponse.BlPosition.Position.MasterFilename != "" {
			panic(fmt.Errorf("Error encountered at position %v, err: '%v'", binlogResponse.BlPosition.Position.String(), binlogResponse.Error))
		} else {
			panic(fmt.Errorf("Error encountered from server %v", binlogResponse.Error))
		}
	}

	switch binlogResponse.SqlType {
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
		return fmt.Errorf("Unknown SqlType %v", binlogResponse.SqlType, binlogResponse.Sql)
	}

	return nil
}

//DDL - apply the schema
func (blp *BinlogPlayer) handleDdl(ddlEvent *mysqlctl.BinlogResponse) {
	for _, sql := range ddlEvent.Sql {
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
	blp.WriteRecoveryPosition(&ddlEvent.Position, ddlEvent.BlPosition.GroupId)
	if err = blp.dbClient.Commit(); err != nil {
		panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
	}
}

func (blp *BinlogPlayer) handleLookupWrites(indexUpdates []string) {
	if len(indexUpdates) == 0 {
		return
	}

	var err error
	if err = blp.lookupClient.Begin(); err != nil {
		panic(fmt.Errorf("Failed query 'BEGIN', err: %s", err))
	}

	for _, indexSql := range indexUpdates {
		if _, err = blp.lookupClient.ExecuteFetch(indexSql, 0, false); err != nil {
			panic(fmt.Errorf("Failed query %s, err: %s", string(indexSql), err))
		}
	}

	if err = blp.lookupClient.Commit(); err != nil {
		panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
	}
}

func (blp *BinlogPlayer) createIndexUpdates(dmlEvent *mysqlctl.BinlogResponse) (indexSql string) {
	keyspaceIdUint, err := strconv.ParseUint(dmlEvent.KeyspaceId, 10, 64)
	if err != nil {
		panic(fmt.Errorf("Invalid keyspaceid '%v', error converting it, %v", dmlEvent.KeyspaceId, err))
	}
	keyspaceId := key.Uint64Key(keyspaceIdUint).KeyspaceId()

	if !blp.keyrange.Contains(keyspaceId) {
		panic(fmt.Errorf("Invalid keyspace id %v for range %v-%v", dmlEvent.KeyspaceId, blp.startPosition.KeyrangeStart, blp.startPosition.KeyrangeEnd))
	}

	if dmlEvent.IndexType != "" {
		indexSql, err = createIndexSql(dmlEvent.SqlType, dmlEvent.IndexType, dmlEvent.IndexId, dmlEvent.UserId)
		if err != nil {
			panic(fmt.Errorf("Error creating index update sql - IndexType %v, IndexId %v, UserId %v Sql '%v', err: '%v'", dmlEvent.IndexType, dmlEvent.IndexId, dmlEvent.UserId, dmlEvent.Sql, err))
		}
	}
	return
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
// and commit the entire batch at the last commit. Lookup txn is flushed before that.
func (blp *BinlogPlayer) handleTxn() bool {
	var err error

	indexUpdates := make([]string, 0, len(blp.txnBuffer))
	dmlMatch := 0
	txnCount := 0
	var queryCount int64
	var txnStartTime, lookupStartTime, queryStartTime time.Time

	for _, dmlEvent := range blp.txnBuffer {
		switch dmlEvent.SqlType {
		case mysqlctl.BEGIN:
			continue
		case mysqlctl.COMMIT:
			txnCount += 1
			if txnCount < blp.txnIndex {
				continue
			}
			lookupStartTime = time.Now()
			blp.handleLookupWrites(indexUpdates)
			blp.txnTime.Record("LookupTxn", lookupStartTime)
			blp.WriteRecoveryPosition(&dmlEvent.Position, dmlEvent.BlPosition.GroupId)
			if err = blp.dbClient.Commit(); err != nil {
				panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
			}
			//added 1 for recovery dml
			queryCount += 2
			blp.queryCount.Add("QueryCount", queryCount)
			blp.txnCount.Add("TxnCount", int64(blp.txnIndex))
			blp.txnTime.Record("TxnTime", txnStartTime)
		case "update", "delete", "insert":
			if blp.dmlTableMatch(dmlEvent.Sql) {
				dmlMatch += 1
				if dmlMatch == 1 {
					if err = blp.dbClient.Begin(); err != nil {
						panic(fmt.Errorf("Failed query 'BEGIN', err: %s", err))
					}
					queryCount += 1
					txnStartTime = time.Now()
				}

				indexSql := blp.createIndexUpdates(dmlEvent)
				if indexSql != "" {
					indexUpdates = append(indexUpdates, indexSql)
				}
				for _, sql := range dmlEvent.Sql {
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
					blp.txnTime.Record("QueryTime", queryStartTime)
				}
				queryCount += int64(len(dmlEvent.Sql))
			}
		default:
			panic(fmt.Errorf("Invalid SqlType %v", dmlEvent.SqlType))
		}
	}
	return true
}

func createIndexSql(dmlType, indexType string, indexId interface{}, userId uint64) (indexSql string, err error) {
	switch indexType {
	case "username":
		indexSlice, ok := indexId.(string)
		if !ok {
			return "", fmt.Errorf("Invalid IndexId value %v for 'username'", indexId)
		}
		index := string(indexSlice)
		switch dmlType {
		case "insert":
			indexSql = fmt.Sprintf(USERNAME_INDEX_INSERT, index, userId)
		case "update":
			indexSql = fmt.Sprintf(USERNAME_INDEX_UPDATE, index, userId)
		case "delete":
			indexSql = fmt.Sprintf(USERNAME_INDEX_DELETE, index, userId)
		default:
			return "", fmt.Errorf("Invalid dmlType %v - for 'username' %v", dmlType, indexId)
		}
	case "video_id":
		index, ok := indexId.(uint64)
		if !ok {
			return "", fmt.Errorf("Invalid IndexId value %v for 'video_id'", indexId)
		}
		switch dmlType {
		case "insert":
			indexSql = fmt.Sprintf(VIDEOID_INDEX_INSERT, index, userId)
		case "delete":
			indexSql = fmt.Sprintf(VIDEOID_INDEX_DELETE, index, userId)
		default:
			return "", fmt.Errorf("Invalid dmlType %v - for 'video_id' %v", dmlType, indexId)
		}
	case "set_id":
		index, ok := indexId.(uint64)
		if !ok {
			return "", fmt.Errorf("Invalid IndexId value %v for 'set_id'", indexId)
		}
		switch dmlType {
		case "insert":
			indexSql = fmt.Sprintf(SETID_INDEX_INSERT, index, userId)
		case "delete":
			indexSql = fmt.Sprintf(SETID_INDEX_DELETE, index, userId)
		default:
			return "", fmt.Errorf("Invalid dmlType %v - for 'set_id' %v", dmlType, indexId)
		}
	default:
		err = fmt.Errorf("Invalid IndexType %v", indexType)
	}
	return
}

//This makes a bson rpc request to the vt_binlog_server
//and processes the events.
func (blp *BinlogPlayer) applyBinlogEvents() error {
	var err error
	connectionStr := fmt.Sprintf("%v:%v", blp.startPosition.Host, SERVER_PORT)
	relog.Info("Dialing server @ %v", connectionStr)
	blp.rpcClient, err = rpcplus.DialHTTP("tcp", connectionStr)
	defer blp.rpcClient.Close()
	if err != nil {
		relog.Error("Error in dialing to vt_binlog_server, %v", err)
		return fmt.Errorf("Error in dialing to vt_binlog_server, %v", err)
	}

	responseChan := make(chan *mysqlctl.BinlogResponse)
	relog.Info("making rpc request @ %v for keyrange %v:%v", blp.startPosition.Position, blp.startPosition.KeyrangeStart, blp.startPosition.KeyrangeEnd)
	blServeRequest := &mysqlctl.BinlogServerRequest{StartPosition: blp.startPosition.Position,
		KeyspaceStart: blp.startPosition.KeyrangeStart,
		KeyspaceEnd:   blp.startPosition.KeyrangeEnd}
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
