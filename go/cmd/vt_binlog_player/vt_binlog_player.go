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
	"os"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/servenv"
)

var stdout *bufio.Writer

const (
	TMP_RECOVERY_PREFIX  = "/tmp/vt_blp-tmp."
	RECOVERY_FILE_PREFIX = "/tmp/vt_blp."
)

var (
	startPosFile      = flag.String("start-pos-file", "", "server address and start coordinates")
	dbConfigFile      = flag.String("db-config-file", "", "json file for db credentials")
	lookupConfigFile  = flag.String("lookup-config-file", "", "json file for lookup db credentials")
	recoveryStatePath = flag.String("recovery-path", "", "path to save the recovery position")
	debug             = flag.Bool("debug", true, "run a debug version - prints the sql statements rather than executing them")
	tables            = flag.String("tables", "", "tables to play back")
	dbCredFile        = flag.String("db-credentials-file", "", "db-creditials file to look up passwd to connect to lookup host")
)

var (
	BEGIN                 = []byte("begin")
	COMMIT                = []byte("commit")
	ROLLBACK              = []byte("rollback")
	USERNAME_INDEX_INSERT = "insert into vt_username_map (username, user_id) values ('%v', %v)"
	VIDEOID_INDEX_INSERT  = "insert into vt_video_id_map (video_id, user_id) values (%v, %v)"
	SETID_INDEX_INSERT    = "insert into vt_set_id_map (set_id, user_id) values (%v, %v)"
	CURRENT_SEQ_SELECT    = "select id from vt_sequence where name='%v' FOR UPDATE"
	SEQ_UPDATE_SQL        = "update vt_sequence set id=%v where name='%v'"
	SEQ_INSERT_SQL        = "insert into vt_sequence (name, id) values ('%v', %v)"
	STREAM_COMMENT_START  = "/* _stream "
	SPACE                 = " "
	BACKDOOR              = "backdoor" //this is the db account name for lookup writes.
)

/*
{
 "Host": "<vt_binlog_server host>>",
 "Port": <vt_binlog_server port>,
 "startPosition": "MasterFilename:dbXX.000123-bin.000123, MasterPosition:1234567",
 "KeyrangeStart": "1000000000000000",
 "KeyrangeEnd": "2000000000000000",
 }
*/
type binlogRecoveryState struct {
	Host          string
	Port          int
	Position      string //json string
	KeyrangeStart string //hex string
	KeyrangeEnd   string //hex string
}

type VtClient interface {
	Connect() (*mysql.Connection, error)
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query []byte, maxrows int, wantfields bool) (qr *proto.QueryResult, err error)
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

func (dc dummyVtClient) ExecuteFetch(query []byte, maxrows int, wantfields bool) (qr *proto.QueryResult, err error) {
	stdout.WriteString(string(query) + ";\n")
	return nil, nil
}

type DBClient struct {
	dbConfig *mysql.ConnectionParams
	dbConn   *mysql.Connection
}

func (dc *DBClient) handleError(err error) {
	relog.Error("in DBClient handleError %v", err.(error))
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

func (dc DBClient) ExecuteFetch(query []byte, maxrows int, wantfields bool) (*proto.QueryResult, error) {
	query = append(query, mysqlctl.SEMICOLON_BYTE...)
	mqr, err := dc.dbConn.ExecuteFetch(query, maxrows, wantfields)
	if err != nil {
		relog.Error("ExecuteFetch failed w/ error %v", err)
		dc.handleError(err)
		return nil, err
	}
	qr := proto.QueryResult(*mqr)
	return &qr, nil
}

type BinlogPlayer struct {
	keyrange                key.KeyRange
	keyrangeTag             string
	tmpRecoveryFile         string
	binlogRecoveryStatePath string
	recoveryState           *binlogRecoveryState
	startPosition           *binlogRecoveryState
	rpcClient               *rpcplus.Client
	inTxn                   bool
	txnBuffer               []*mysqlctl.BinlogResponse
	dbClient                VtClient
	lookupClient            VtClient
	debug                   bool
	tables                  []string
}

func NewBinlogPlayer(startPosition *binlogRecoveryState, krStart, krEnd key.KeyspaceId) *BinlogPlayer {
	blp := new(BinlogPlayer)
	blp.startPosition = startPosition
	blp.recoveryState = &binlogRecoveryState{Host: blp.startPosition.Host,
		Port:          blp.startPosition.Port,
		Position:      blp.startPosition.Position,
		KeyrangeStart: blp.startPosition.KeyrangeStart,
		KeyrangeEnd:   blp.startPosition.KeyrangeEnd}
	blp.keyrange.Start = krStart
	blp.keyrange.End = krEnd
	blp.keyrangeTag = blp.startPosition.KeyrangeEnd
	if blp.keyrangeTag == "" {
		blp.keyrangeTag = "MAX"
	}
	blp.tmpRecoveryFile = fmt.Sprintf("%v%v.%v", TMP_RECOVERY_PREFIX, blp.keyrangeTag, blp.startPosition.Host)

	blp.inTxn = false
	blp.txnBuffer = make([]*mysqlctl.BinlogResponse, 0, mysqlctl.MAX_TXN_BATCH)
	blp.debug = false
	return blp
}

func (blp *BinlogPlayer) WriteRecoveryPosition(currentPosition string) {
	blp.recoveryState.Position = currentPosition
	data, err := json.Marshal(blp.recoveryState)
	if err != nil {
		panic(fmt.Errorf("Error in marshaling recovery info, err: %v", err))
	}

	if err = ioutil.WriteFile(blp.tmpRecoveryFile, data, 0664); err != nil {
		panic(fmt.Errorf("Error in writing temp recovery file '%v' to disk, err: %v", blp.tmpRecoveryFile, err))
	}

	if err = os.Rename(blp.tmpRecoveryFile, blp.binlogRecoveryStatePath); err != nil {
		panic(fmt.Errorf("Error in renaming temp file '%v' to recovery '%v', err: %v", blp.tmpRecoveryFile, blp.binlogRecoveryStatePath, err))
	}
}

func main() {
	flag.Parse()
	servenv.Init("vt_binlog_player")

	if *startPosFile == "" {
		if *recoveryStatePath != "" {
			*startPosFile = *recoveryStatePath
		} else {
			relog.Fatal("Invalid start position and recovery path")
		}
	}

	blp, err := initBinlogPlayer(*startPosFile, *dbConfigFile, *lookupConfigFile, *dbCredFile, *debug)
	if err != nil {
		relog.Fatal("Error in initializing binlog player - '%v'", err)
	}

	if *tables != "" {
		tables := strings.Split(*tables, ",")
		blp.tables = make([]string, len(tables))
		for i, table := range tables {
			blp.tables[i] = strings.TrimSpace(table)
		}
		relog.Info("len tables %v tables %v", len(blp.tables), blp.tables)
	}

	if *recoveryStatePath == "" {
		*recoveryStatePath = fmt.Sprintf("%v%v.%v", RECOVERY_FILE_PREFIX, blp.keyrangeTag, blp.startPosition.Host)
		relog.Warning("Recovery state path empty, assigned default '%v'", *recoveryStatePath)
	}
	blp.binlogRecoveryStatePath = *recoveryStatePath

	relog.Info("BinlogPlayer client for keyrange '%v:%v' starting @ '%v', checkpoint saved @ %v",
		blp.startPosition.KeyrangeStart,
		blp.startPosition.KeyrangeEnd,
		blp.startPosition.Position,
		blp.binlogRecoveryStatePath)

	//Make a request to the server and start processing the events.
	stdout = bufio.NewWriterSize(os.Stdout, 16*1024)
	for {
		err := blp.applyBinlogEvents()
		if err != nil {
			relog.Error("Error in applying binlog events, err %v", err)
			//FIXME: should this retry 'n' times only ?
			if strings.Contains(err.Error(), "EOF") {
				blp.startPosition.Position = blp.recoveryState.Position
				blp.inTxn = false
				blp.txnBuffer = blp.txnBuffer[:0]
				relog.Warning("Encountered EOF, retrying at last position %v", blp.startPosition.Position)
			} else {
				break
			}
		}
	}
	relog.Info("vt_binlog_player done")
}

func startPositionValid(startPos *binlogRecoveryState) bool {
	if startPos.Host == "" || startPos.Port == 0 {
		relog.Error("Invalid connection params.")
		return false
	}
	if startPos.Position == "" {
		relog.Error("Empty start position.")
		return false
	}
	//One of them can be empty for min or max key.
	if startPos.KeyrangeStart == "" && startPos.KeyrangeEnd == "" {
		relog.Error("Invalid keyrange endpoints.")
		return false
	}
	return true
}

func initBinlogPlayer(startPosFile, dbConfigFile, lookupConfigFile, dbCredFile string, debug bool) (*BinlogPlayer, error) {
	startData, err := ioutil.ReadFile(startPosFile)
	if err != nil {
		return nil, fmt.Errorf("Error %s in reading start position file %s", err, startPosFile)
	}
	startPosition := new(binlogRecoveryState)
	err = json.Unmarshal(startData, startPosition)
	if err != nil {
		return nil, fmt.Errorf("Error in unmarshaling recovery data: %s, startData %v", err, string(startData))
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

	binlogPlayer := NewBinlogPlayer(startPosition, krStart, krEnd)

	if debug {
		binlogPlayer.debug = true
		binlogPlayer.dbClient = dummyVtClient{}
		binlogPlayer.lookupClient = dummyVtClient{}
	} else {
		dbConfigData, err := ioutil.ReadFile(dbConfigFile)
		if err != nil {
			return nil, fmt.Errorf("Error %s in reading db-config-file %s", err, dbConfigFile)
		}
		relog.Info("dbConfigData %v", string(dbConfigData))

		lookupConfigData, err := ioutil.ReadFile(lookupConfigFile)
		if err != nil {
			return nil, fmt.Errorf("Error %s in reading lookup-config-file %s", err, lookupConfigFile)
		}

		dbClient := DBClient{}
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
		binlogPlayer.dbClient = dbClient

		//Below is the code to lookup "backdoor" user's passwd
		//for loookup writes.
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
			if passwd, ok := dbCredentials[BACKDOOR]; ok {
				lookupPasswd = passwd[0]
			}
		}

		lookupClient := DBClient{}
		lookupConfig := new(mysql.ConnectionParams)
		err = json.Unmarshal(lookupConfigData, lookupConfig)
		if err != nil {
			return nil, fmt.Errorf("error in unmarshaling lookupConfig data, err '%v'", err)
		}
		lookupConfig.Pass = lookupPasswd
		relog.Info("lookupConfig %v", lookupConfig)
		lookupClient.dbConfig = lookupConfig

		lookupClient.dbConn, err = lookupClient.Connect()
		if err != nil {
			return nil, fmt.Errorf("error in connecting to mysql db, err %v", err)
		}
		binlogPlayer.lookupClient = lookupClient
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

func (blp *BinlogPlayer) processBinlogEvent(binlogResponse *mysqlctl.BinlogResponse) (err error) {
	defer handleError(&err, blp)

	//Read event
	if binlogResponse.Error != "" {
		//EOF error, retry.
		if strings.Contains(binlogResponse.Error, "EOF") {
			relog.Error("Retry %v", binlogResponse.Error)
			panic(fmt.Errorf(binlogResponse.Error))
		}
		if binlogResponse.BinlogPosition.Position != "" {
			panic(fmt.Errorf("Error encountered at position %v: %v", binlogResponse.BinlogPosition.Position, binlogResponse.Error))
		} else {
			panic(fmt.Errorf("Error encountered from server %v", binlogResponse.Error))
		}
	}

	switch binlogResponse.SqlType {
	case mysqlctl.DDL:
		blp.handleDdl(binlogResponse)
		//FIXME: should we write recovery info to _vt db for every successful txn ?
		blp.WriteRecoveryPosition(binlogResponse.Position)
	case mysqlctl.BEGIN:
		if blp.inTxn {
			return fmt.Errorf("Invalid txn: txn already in progress, len(blp.txnBuffer) %v", len(blp.txnBuffer))
		}
		blp.txnBuffer = blp.txnBuffer[:0]
		blp.inTxn = true
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)
	case mysqlctl.COMMIT:
		if !blp.inTxn {
			return fmt.Errorf("Invalid event: COMMIT event without a transaction.")
		}
		blp.txnBuffer = append(blp.txnBuffer, binlogResponse)
		blp.handleTxn()
		blp.inTxn = false
		blp.txnBuffer = blp.txnBuffer[:0]
		//FIXME: should we write recovery info to _vt db for every successful txn ?
		blp.WriteRecoveryPosition(binlogResponse.Position)
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
	if len(ddlEvent.Sql) == 0 || len(ddlEvent.Sql) > 1 {
		panic(fmt.Errorf("Invalid ddl event type, sqls %v", ddlEvent.Sql))
	}
	if _, err := blp.dbClient.ExecuteFetch([]byte(ddlEvent.Sql[0]), 0, false); err != nil {
		panic(fmt.Errorf("Error %v in executing sql %v", err, ddlEvent.Sql))
	}
}

func (blp *BinlogPlayer) handleLookupWrites(indexUpdates, seqUpdates [][]byte) {
	if len(indexUpdates) == 0 && len(seqUpdates) == 0 {
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

	for _, seqSql := range seqUpdates {
		if _, err = blp.lookupClient.ExecuteFetch(seqSql, 0, false); err != nil {
			panic(fmt.Errorf("Failed query %s, err: %s", string(seqSql), err))
		}
	}

	if err = blp.lookupClient.Commit(); err != nil {
		panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
	}
}

func (blp *BinlogPlayer) createIndexSeqSql(dmlEvent *mysqlctl.BinlogResponse) (indexSql, seqSql []byte) {
	keyspaceIdUint, err := strconv.ParseUint(dmlEvent.KeyspaceId, 10, 64)
	if err != nil {
		panic(fmt.Errorf("Invalid keyspaceid '%v', error converting it, %v", dmlEvent.KeyspaceId, err))
	}
	keyspaceId := key.Uint64Key(keyspaceIdUint).KeyspaceId()

	if !blp.keyrange.Contains(keyspaceId) {
		panic(fmt.Errorf("Invalid keyspace id %v for range %v-%v", dmlEvent.KeyspaceId, blp.startPosition.KeyrangeStart, blp.startPosition.KeyrangeEnd))
	}

	if dmlEvent.IndexType != "" {
		indexSql, err = createIndexSql(dmlEvent.IndexType, dmlEvent.IndexId, dmlEvent.UserId)
		if err != nil {
			panic(fmt.Errorf("Error creating index update sql %v, IndexType %v, IndexId %v, UserId %v Sql '%v'", err, dmlEvent.IndexType, dmlEvent.IndexId, dmlEvent.UserId, dmlEvent.Sql))
		}
	}
	if dmlEvent.SeqName != "" {
		seqSql, err = blp.createSeqSql(dmlEvent.SeqName, dmlEvent.SeqId)
		if err != nil {
			panic(fmt.Errorf("Error creating seq update sql %v, SeqName %v SeqId %v, Sql '%v'", err, dmlEvent.SeqName, dmlEvent.SeqId, dmlEvent.Sql))
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
			relog.Warning("sql doesn't have stream comment '%v'", sql)
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

//Txn - start a context and apply the entire txn.
//for each dml - verify that keyspace is correct for this shard - abort immediately for a wrong sql.
// if indexType, indexid is set, update r_lookup.
//if seqName and seqId is set, update r_lookup.
func (blp *BinlogPlayer) handleTxn() {
	var err error
	indexUpdates := make([][]byte, 0, len(blp.txnBuffer))
	seqUpdates := make([][]byte, 0, len(blp.txnBuffer))
	dmlMatch := 0

	for _, dmlEvent := range blp.txnBuffer {
		switch dmlEvent.SqlType {
		case mysqlctl.BEGIN:
			continue
			//if err = blp.dbClient.Begin(); err != nil {
			//	panic(fmt.Errorf("Failed query '%v', err: %s", dmlEvent.Sql, err))
			//}
		case mysqlctl.COMMIT:
			blp.handleLookupWrites(indexUpdates, seqUpdates)
			if err = blp.dbClient.Commit(); err != nil {
				panic(fmt.Errorf("Failed query 'COMMIT', err: %s", err))
			}
		case "update", "delete", "insert":
			if blp.dmlTableMatch(dmlEvent.Sql) {
				dmlMatch += 1
				if dmlMatch == 1 {
					if err = blp.dbClient.Begin(); err != nil {
						panic(fmt.Errorf("Failed query 'BEGIN', err: %s", err))
					}
				}

				indexSql, seqSql := blp.createIndexSeqSql(dmlEvent)
				if indexSql != nil {
					indexUpdates = append(indexUpdates, indexSql)
				}
				if seqSql != nil {
					seqUpdates = append(seqUpdates, seqSql)
				}
				for _, sql := range dmlEvent.Sql {
					if _, err = blp.dbClient.ExecuteFetch([]byte(sql), 0, false); err != nil {
						panic(fmt.Errorf("Error %v in executing sql '%v'", err, sql))
					}
				}
			}
		default:
			panic(fmt.Errorf("Invalid SqlType %v", dmlEvent.SqlType))
		}
	}
}

func createIndexSql(indexType string, indexId interface{}, userId uint64) (indexSql []byte, err error) {

	switch indexType {
	case "username":
		indexSlice, ok := indexId.([]byte)
		if !ok {
			return nil, fmt.Errorf("Invalid IndexId value %v for 'username'", indexId)
		}
		index := string(indexSlice)
		indexSql = []byte(fmt.Sprintf(USERNAME_INDEX_INSERT, index, userId))
	case "video_id":
		index, ok := indexId.(uint64)
		if !ok {
			return nil, fmt.Errorf("Invalid IndexId value %v for 'video_id'", indexId)
		}
		indexSql = []byte(fmt.Sprintf(VIDEOID_INDEX_INSERT, index, userId))
	case "set_id":
		index, ok := indexId.(uint64)
		if !ok {
			return nil, fmt.Errorf("Invalid IndexId value %v for 'set_id'", indexId)
		}
		indexSql = []byte(fmt.Sprintf(SETID_INDEX_INSERT, index, userId))
	default:
		err = fmt.Errorf("Invalid IndexType %v", indexType)
	}
	return
}

func (blp *BinlogPlayer) createSeqSql(seqName string, seqId uint64) (seqSql []byte, err error) {
	if blp.debug {
		switch seqName {
		case "user_id", "video_id", "set_id":
			seqSql = []byte(fmt.Sprintf(SEQ_UPDATE_SQL, seqId, seqName))
		default:
			err = fmt.Errorf("Invalid Seq Name %v", seqName)
		}
		return
	}
	//Real-case
	seqQuery := fmt.Sprintf(CURRENT_SEQ_SELECT, seqName)
	res, err := blp.lookupClient.ExecuteFetch([]byte(seqQuery), 1, true)
	if err != nil {
		return nil, fmt.Errorf("Error in obtaining vt_sequence data")
	}
	//vt_sequence isn't initialized
	if res.RowsAffected != 1 {
		switch seqName {
		case "video_id", "set_id", "user_id":
			seqSql = []byte(fmt.Sprintf(SEQ_INSERT_SQL, seqName, seqId))
		default:
			err = fmt.Errorf("Invalid Seq Name %v", seqName)
		}
		return
	}

	//vt_sequence is initialized - insert rows
	indexOfId := 0
	for i, field := range res.Fields {
		if field.Name == "id" {
			indexOfId = i
			break
		}
	}
	row := res.Rows[0]
	if !row[indexOfId].IsNumeric() {
		return nil, fmt.Errorf("Invalid Id field")
	}
	currentIndexStr := string(row[indexOfId].Raw())
	currentIndex, err := strconv.ParseUint(currentIndexStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Error in converting seqId value %v", currentIndexStr)
	}
	//If the current value of sequence Id > to be updated value, skip.
	//This is possible since the ranges from different shards are being merged.
	if currentIndex > seqId {
		return nil, nil
	}

	switch seqName {
	case "video_id", "set_id", "user_id":
		seqSql = []byte(fmt.Sprintf(SEQ_UPDATE_SQL, seqId, seqName))
	default:
		err = fmt.Errorf("Invalid Seq Name %v", seqName)
	}
	return
}

//This makes a bson rpc request to the vt_binlog_server
//and processes the events.
func (blp *BinlogPlayer) applyBinlogEvents() error {
	var err error
	connectionStr := fmt.Sprintf("%v:%v", blp.startPosition.Host, blp.startPosition.Port)
	relog.Info("Dialing server @ %v", connectionStr)
	blp.rpcClient, err = bsonrpc.DialHTTP("tcp", connectionStr, 0)
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

	for response := range responseChan {
		err = blp.processBinlogEvent(response)
		if err != nil {
			return fmt.Errorf("Error in processing binlog event %v", err)
		}
	}
	if resp.Error != nil {
		return fmt.Errorf("Error received from ServeBinlog %v", resp.Error)
	}
	return nil
}
