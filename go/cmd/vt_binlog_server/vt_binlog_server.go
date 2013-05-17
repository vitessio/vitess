// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt binlog server: Serves binlog for out of band replication.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/stats"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/servenv"
)

var (
	port      = flag.Int("port", 6614, "port for the server")
	dbname    = flag.String("dbname", "", "database name")
	mycnfFile = flag.String("mycnf-file", "", "path of mycnf file")
)

const (
	COLON   = ":"
	DOT     = "."
	MAX_KEY = "MAX_KEY"
)

var (
	KEYSPACE_ID_COMMENT = []byte("/* EMD keyspace_id:")
	USER_ID             = []byte("user_id")
	INDEX_COMMENT       = []byte("index")
	COLON_BYTE          = []byte(COLON)
	DOT_BYTE            = []byte(DOT)
	END_COMMENT         = []byte("*/")
	_VT                 = []byte("_vt.")
	HEARTBEAT           = []byte("heartbeat")
	ADMIN               = []byte("admin")
)

type blpStats struct {
	parseStats    *stats.Counters
	dmlCount      *stats.Counters
	txnCount      *stats.Counters
	queriesPerSec *stats.Rates
	txnsPerSec    *stats.Rates
}

func NewBlpStats() *blpStats {
	bs := &blpStats{}
	bs.parseStats = stats.NewCounters("ParseEvent")
	bs.txnCount = stats.NewCounters("TxnCount")
	bs.dmlCount = stats.NewCounters("DmlCount")
	bs.queriesPerSec = stats.NewRates("QueriesPerSec", bs.dmlCount, 15, 60e9)
	bs.txnsPerSec = stats.NewRates("TxnPerSec", bs.txnCount, 15, 60e9)
	return bs
}

type BinlogServer struct {
	dbname string
	mycnf  *mysqlctl.Mycnf
	*blpStats
}

//Raw event buffer used to gather data during parsing.
type eventBuffer struct {
	mysqlctl.BlPosition
	LogLine []byte
	firstKw string
}

func NewEventBuffer(pos *mysqlctl.BlPosition, line []byte) *eventBuffer {
	buf := &eventBuffer{}
	buf.LogLine = make([]byte, len(line))
	//buf.LogLine = append(buf.LogLine, line...)
	written := copy(buf.LogLine, line)
	if written < len(line) {
		relog.Warning("Problem in copying logline to new buffer, written %v, len %v", written, len(line))
	}
	buf.BlPosition = *pos
	buf.BlPosition.Timestamp = pos.Timestamp
	return buf
}

type Blp struct {
	nextStmtPosition uint64
	inTxn            bool
	txnLineBuffer    []*eventBuffer
	responseStream   []*mysqlctl.BinlogResponse
	initialSeek      bool
	startPosition    *mysqlctl.ReplicationCoordinates
	currentPosition  *mysqlctl.BlPosition
	dbmatch          bool
	keyspaceRange    key.KeyRange
	keyrangeTag      string
	globalState      *BinlogServer
	logMetadata      *mysqlctl.SlaveMetadata
	usingRelayLogs   bool
	binlogPrefix     string
	//FIXME: this is for debug, remove it.
	currentLine string
	*blpStats
}

func NewBlp(startCoordinates *mysqlctl.ReplicationCoordinates, blServer *BinlogServer, keyRange *key.KeyRange) *Blp {
	blp := &Blp{}
	blp.startPosition = startCoordinates
	blp.keyspaceRange = *keyRange
	blp.currentPosition = &mysqlctl.BlPosition{}
	blp.currentPosition.Position = *startCoordinates
	blp.currentPosition.Position.RelayPosition = 0
	blp.inTxn = false
	blp.initialSeek = true
	blp.txnLineBuffer = make([]*eventBuffer, 0, mysqlctl.MAX_TXN_BATCH)
	blp.responseStream = make([]*mysqlctl.BinlogResponse, 0, mysqlctl.MAX_TXN_BATCH)
	blp.globalState = blServer
	//by default assume that the db matches.
	blp.dbmatch = true
	blp.keyrangeTag = string(keyRange.End.Hex())
	if blp.keyrangeTag == "" {
		blp.keyrangeTag = MAX_KEY
	}
	return blp
}

type BinlogParseError struct {
	Msg string
}

func NewBinlogParseError(msg string) *BinlogParseError {
	return &BinlogParseError{Msg: msg}
}

func (err BinlogParseError) Error() string {
	return err.Msg
}

func (blp *Blp) streamBinlog(sendReply mysqlctl.SendUpdateStreamResponse) {
	var readErr error
	defer func() {
		reqIdentifier := fmt.Sprintf("%v, line: '%v'", blp.currentPosition.Position.String(), blp.currentLine)
		if x := recover(); x != nil {
			serr, ok := x.(*BinlogParseError)
			if !ok {
				relog.Error("[%v:%v] Uncaught panic for stream @ %v, err: %v ", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, x)
				panic(x)
			}
			err := *serr
			if readErr != nil {
				relog.Error("[%v:%v] StreamBinlog error @ %v, error: %v, readErr %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, err, readErr)
				err = BinlogParseError{Msg: fmt.Sprintf("%v, readErr: %v", err, readErr)}
			} else {
				relog.Error("[%v:%v] StreamBinlog error @ %v, error: %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, err)
			}
			sendError(sendReply, reqIdentifier, err, blp.currentPosition)
		}
	}()

	blr := mysqlctl.NewBinlogReader(blp.binlogPrefix)

	var binlogReader io.Reader
	var blrReader, blrWriter *os.File
	var err, pipeErr error

	blrReader, blrWriter, pipeErr = os.Pipe()
	if pipeErr != nil {
		panic(NewBinlogParseError(pipeErr.Error()))
	}
	defer blrWriter.Close()
	defer blrReader.Close()

	readErrChan := make(chan error, 1)
	//This reads the binlogs - read end of data pipeline.
	go blp.getBinlogStream(blrWriter, blr, readErrChan)

	//Decode end of the data pipeline.
	binlogDecoder := new(mysqlctl.BinlogDecoder)
	binlogReader, err = binlogDecoder.DecodeMysqlBinlog(blrReader)
	if err != nil {
		panic(NewBinlogParseError(err.Error()))
	}

	//This function monitors the exit of read data pipeline.
	go func(readErr *error, readErrChan chan error, binlogDecoder *mysqlctl.BinlogDecoder) {
		*readErr = <-readErrChan
		//relog.Info("Read data-pipeline returned readErr: '%v'", *readErr)
		if *readErr != nil {
			binlogDecoder.Kill()
		}
	}(&readErr, readErrChan, binlogDecoder)

	blp.parseBinlogEvents(sendReply, binlogReader)
}

func (blp *Blp) getBinlogStream(writer *os.File, blr *mysqlctl.BinlogReader, readErrChan chan error) {
	defer func() {
		if err := recover(); err != nil {
			relog.Error("getBinlogStream failed: %v", err)
			readErrChan <- err.(error)
		}
	}()
	if blp.usingRelayLogs {
		//we use RelayPosition for initial seek in case the caller has precise coordinates. But the code
		//is designed to primarily use RelayFilename, MasterFilename and MasterPosition to correctly start
		//streaming the logs if relay logs are being used.
		blr.ServeData(writer, path.Base(blp.startPosition.RelayFilename), int64(blp.startPosition.RelayPosition))
	} else {
		blr.ServeData(writer, blp.startPosition.MasterFilename, int64(blp.startPosition.MasterPosition))
	}
	readErrChan <- nil
}

//Main parse loop
func (blp *Blp) parseBinlogEvents(sendReply mysqlctl.SendUpdateStreamResponse, binlogReader io.Reader) {
	// read over the stream and buffer up the transactions
	var err error
	var line []byte
	bigLine := make([]byte, 0, mysqlctl.BINLOG_BLOCK_SIZE)
	lineReader := bufio.NewReaderSize(binlogReader, mysqlctl.BINLOG_BLOCK_SIZE)
	readAhead := false
	var event *eventBuffer
	var delimIndex int

	for {
		line = line[:0]
		bigLine = bigLine[:0]
		line, err = blp.readBlpLine(lineReader, bigLine)
		if err != nil {
			if err == io.EOF {
				//end of stream
				blp.globalState.parseStats.Add("EOFErrors."+blp.keyrangeTag, 1)
				panic(NewBinlogParseError(fmt.Sprintf("EOF")))
			}
			panic(NewBinlogParseError(fmt.Sprintf("ReadLine err: , %v", err)))
		}
		if len(line) == 0 {
			continue
		}

		if line[0] == '#' {
			//parse positional data
			line = bytes.TrimSpace(line)
			blp.currentLine = string(line)
			blp.parsePositionData(line)
		} else {
			//parse event data

			//This is to accont for replicas where we seek to a point before the desired startPosition
			if blp.initialSeek && blp.usingRelayLogs && blp.nextStmtPosition < blp.startPosition.MasterPosition {
				continue
			}

			if readAhead {
				event.LogLine = append(event.LogLine, line...)
			} else {
				event = NewEventBuffer(blp.currentPosition, line)
			}

			delimIndex = bytes.LastIndex(event.LogLine, mysqlctl.BINLOG_DELIMITER)
			if delimIndex != -1 {
				event.LogLine = event.LogLine[:delimIndex]
				readAhead = false
			} else {
				readAhead = true
				continue
			}

			event.LogLine = bytes.TrimSpace(event.LogLine)
			event.firstKw = string(bytes.ToLower(bytes.SplitN(event.LogLine, mysqlctl.SPACE, 2)[0]))

			blp.currentLine = string(event.LogLine)

			//processes statements only for the dbname that it is subscribed to.
			blp.parseDbChange(event)
			blp.parseEventData(sendReply, event)
		}
	}
}

//This reads a binlog log line.
func (blp *Blp) readBlpLine(lineReader *bufio.Reader, bigLine []byte) (line []byte, err error) {
	for {
		tempLine, tempErr := lineReader.ReadSlice('\n')
		if tempErr == bufio.ErrBufferFull {
			bigLine = append(bigLine, tempLine...)
			blp.globalState.parseStats.Add("BufferFullErrors."+blp.keyrangeTag, 1)
			continue
		} else if tempErr != nil {
			relog.Error("[%v:%v] Error in reading %v, data read %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), tempErr, string(tempLine))
			err = tempErr
		} else if len(bigLine) > 0 {
			if len(tempLine) > 0 {
				bigLine = append(bigLine, tempLine...)
			}
			line = bigLine[:len(bigLine)-1]
			blp.globalState.parseStats.Add("BigLineCount."+blp.keyrangeTag, 1)
		} else {
			line = tempLine[:len(tempLine)-1]
		}
		break
	}
	return line, err
}

//Function to set the dbmatch variable, this parses the "Use <dbname>" statement.
func (blp *Blp) parseDbChange(event *eventBuffer) {
	if event.firstKw != mysqlctl.USE {
		return
	}
	if blp.globalState.dbname == "" {
		relog.Warning("Dbname is not set, will match all database names")
		return
	}
	blp.globalState.parseStats.Add("DBChange."+blp.keyrangeTag, 1)

	new_db := string(bytes.TrimSpace(bytes.SplitN(event.LogLine, mysqlctl.BINLOG_DB_CHANGE, 2)[1]))
	if new_db != blp.globalState.dbname {
		blp.dbmatch = false
	} else {
		blp.dbmatch = true
	}
}

func (blp *Blp) parsePositionData(line []byte) {
	if bytes.HasPrefix(line, mysqlctl.BINLOG_POSITION_PREFIX) {
		//Master Position
		if blp.nextStmtPosition == 0 {
			return
		}
	} else if bytes.Index(line, mysqlctl.BINLOG_ROTATE_TO) != -1 {
		blp.parseRotateEvent(line)
	} else if bytes.Index(line, mysqlctl.BINLOG_END_LOG_POS) != -1 {
		//Ignore the position data that appears at the start line of binlog.
		if bytes.Index(line, mysqlctl.BINLOG_START) != -1 {
			return
		}
		blp.parseMasterPosition(line)
		if blp.nextStmtPosition != 0 {
			blp.currentPosition.Position.MasterPosition = blp.nextStmtPosition
		}
	}
	if bytes.Index(line, mysqlctl.BINLOG_XID) != -1 {
		blp.parseXid(line)
	}
	// FIXME(shrutip): group_id is most relevant for commit events
	// check how group_id is set for ddls and possibly move this block
	// in parseXid
	if bytes.Index(line, mysqlctl.BINLOG_GROUP_ID) != -1 {
		blp.parseGroupId(line)
	}
}

func (blp *Blp) parseEventData(sendReply mysqlctl.SendUpdateStreamResponse, event *eventBuffer) {
	if bytes.HasPrefix(event.LogLine, mysqlctl.BINLOG_SET_TIMESTAMP) {
		blp.extractEventTimestamp(event)
		blp.initialSeek = false
		if blp.inTxn {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		}
	} else if bytes.HasPrefix(event.LogLine, mysqlctl.BINLOG_BEGIN) {
		blp.handleBeginEvent(event)
	} else if bytes.HasPrefix(event.LogLine, mysqlctl.BINLOG_ROLLBACK) {
		blp.inTxn = false
		blp.txnLineBuffer = blp.txnLineBuffer[:0]
	} else if bytes.HasPrefix(event.LogLine, mysqlctl.BINLOG_COMMIT) {
		blp.handleCommitEvent(sendReply, event)
		blp.inTxn = false
		blp.txnLineBuffer = blp.txnLineBuffer[:0]
	} else if len(event.LogLine) > 0 {
		sqlType := mysqlctl.GetSqlType(event.firstKw)
		if blp.inTxn && mysqlctl.IsTxnStatement(event.LogLine, event.firstKw) {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		} else if sqlType == mysqlctl.DDL {
			blp.handleDdlEvent(sendReply, event)
		} else {
			if sqlType == mysqlctl.DML {
				lineBuf := make([][]byte, 0, 10)
				for _, dml := range blp.txnLineBuffer {
					lineBuf = append(lineBuf, dml.LogLine)
				}
				panic(NewBinlogParseError(fmt.Sprintf("DML outside a txn - len %v, dml '%v', txn buffer '%v'", len(blp.txnLineBuffer), string(event.LogLine), string(bytes.Join(lineBuf, mysqlctl.SEMICOLON_BYTE)))))
			}
			//Ignore these often occuring statement types.
			if !mysqlctl.IgnoredStatement(event.LogLine) {
				relog.Warning("Unknown statement '%v'", string(event.LogLine))
			}
		}
	}
}

/*
Position Parsing Functions.
*/

func (blp *Blp) parseMasterPosition(line []byte) {
	var err error
	rem := bytes.SplitN(line, mysqlctl.BINLOG_END_LOG_POS, 2)
	masterPosStr := string(bytes.SplitN(rem[1], mysqlctl.SPACE, 2)[0])
	blp.nextStmtPosition, err = strconv.ParseUint(masterPosStr, 10, 64)
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in extracting master position, %v, sql %v, pos string %v", err, string(line), masterPosStr)))
	}
}

func (blp *Blp) parseXid(line []byte) {
	rem := bytes.SplitN(line, mysqlctl.BINLOG_XID, 2)
	xid, err := strconv.ParseUint(string(rem[1]), 10, 64)
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in extracting Xid position %v, sql %v", err, string(line))))
	}
	blp.currentPosition.Xid = xid
}

func (blp *Blp) parseGroupId(line []byte) {
	rem := bytes.SplitN(line, mysqlctl.BINLOG_GROUP_ID, 2)
	rem2 := bytes.SplitN(rem[1], mysqlctl.SPACE, 2)
	groupIdStr := strings.TrimSpace(string(rem2[0]))
	groupId, err := strconv.ParseUint(groupIdStr, 10, 64)
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in extracting group_id %v, sql %v", err, string(line))))
	}
	blp.currentPosition.GroupId = groupId
}

func (blp *Blp) extractEventTimestamp(event *eventBuffer) {
	line := event.LogLine
	timestampStr := string(line[len(mysqlctl.BINLOG_SET_TIMESTAMP):])
	if timestampStr == "" {
		panic(NewBinlogParseError(fmt.Sprintf("Invalid timestamp line %v", string(line))))
	}
	currentTimestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in extracting timestamp %v, sql %v", err, string(line))))
	}
	blp.currentPosition.Timestamp = currentTimestamp
	event.BlPosition.Timestamp = currentTimestamp
}

func (blp *Blp) parseRotateEvent(line []byte) {
	rem := bytes.SplitN(line, mysqlctl.BINLOG_ROTATE_TO, 2)
	rem2 := bytes.SplitN(rem[1], mysqlctl.POS, 2)
	rotateFilename := strings.TrimSpace(string(rem2[0]))
	rotatePos, err := strconv.ParseUint(string(rem2[1]), 10, 64)
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in extracting rotate pos %v from line %s", err, string(line))))
	}

	if !blp.usingRelayLogs {
		//If the file being parsed is a binlog,
		//then the rotate events only correspond to itself.
		blp.currentPosition.Position.MasterFilename = rotateFilename
		blp.currentPosition.Position.MasterPosition = rotatePos
		blp.globalState.parseStats.Add("BinlogRotate."+blp.keyrangeTag, 1)
	} else {
		//For relay logs, the rotate events could be that of relay log or the binlog,
		//the prefix of rotateFilename is used to test which case is it.
		logsDir, relayFile := path.Split(blp.currentPosition.Position.RelayFilename)
		currentPrefix := strings.Split(relayFile, ".")[0]
		rotatePrefix := strings.Split(rotateFilename, ".")[0]
		if currentPrefix == rotatePrefix {
			//relay log rotated
			blp.currentPosition.Position.RelayFilename = path.Join(logsDir, rotateFilename)
			blp.globalState.parseStats.Add("RelayRotate."+blp.keyrangeTag, 1)
		} else {
			//master file rotated
			blp.currentPosition.Position.MasterFilename = rotateFilename
			blp.currentPosition.Position.MasterPosition = rotatePos
			blp.globalState.parseStats.Add("BinlogRotate."+blp.keyrangeTag, 1)
		}
	}
}

/*
Data event parsing and handling functions.
*/

func (blp *Blp) handleBeginEvent(event *eventBuffer) {
	if len(blp.txnLineBuffer) > 0 {
		if blp.inTxn {
			lineBuf := make([][]byte, 0, 10)
			for _, event := range blp.txnLineBuffer {
				lineBuf = append(lineBuf, event.LogLine)
			}
			panic(NewBinlogParseError(fmt.Sprintf("BEGIN encountered with non-empty trxn buffer, len: %d, buf %v", len(blp.txnLineBuffer), string(bytes.Join(lineBuf, mysqlctl.SEMICOLON_BYTE)))))
		} else {
			relog.Warning("Non-zero txn buffer, while inTxn false")
		}
	}
	blp.txnLineBuffer = blp.txnLineBuffer[:0]
	blp.inTxn = true
	blp.txnLineBuffer = append(blp.txnLineBuffer, event)
}

//This creates the response for DDL event.
func createDdlStream(lineBuffer *eventBuffer) (ddlStream *mysqlctl.BinlogResponse) {
	ddlStream = new(mysqlctl.BinlogResponse)
	ddlStream.BlPosition = lineBuffer.BlPosition
	ddlStream.SqlType = mysqlctl.DDL
	ddlStream.Sql = make([]string, 0, 1)
	ddlStream.Sql = append(ddlStream.Sql, string(lineBuffer.LogLine))
	return ddlStream
}

func (blp *Blp) handleDdlEvent(sendReply mysqlctl.SendUpdateStreamResponse, event *eventBuffer) {
	ddlStream := createDdlStream(event)
	buf := []*mysqlctl.BinlogResponse{ddlStream}
	if err := sendStream(sendReply, buf); err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in sending event to client %v", err)))
	}
	blp.globalState.parseStats.Add("DdlCount."+blp.keyrangeTag, 1)
}

func (blp *Blp) handleCommitEvent(sendReply mysqlctl.SendUpdateStreamResponse, commitEvent *eventBuffer) {
	if !blp.dbmatch {
		return
	}

	if blp.usingRelayLogs {
		for !blp.slavePosBehindReplication() {
			relog.Info("[%v:%v] parsing is not behind replication, sleeping", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex())
			time.Sleep(2 * time.Second)
		}
	}

	commitEvent.BlPosition.Xid = blp.currentPosition.Xid
	commitEvent.BlPosition.GroupId = blp.currentPosition.GroupId
	blp.txnLineBuffer = append(blp.txnLineBuffer, commitEvent)
	//txn block for DMLs, parse it and send events for a txn
	var dmlCount int64
	//This filters the dmls for keyrange supplied by the client.
	blp.responseStream, dmlCount = blp.buildTxnResponse()

	//No dmls matched the keyspace id so return
	if dmlCount == 0 {
		return
	}

	if err := sendStream(sendReply, blp.responseStream); err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Error in sending event to client %v", err)))
	}

	blp.globalState.dmlCount.Add("DmlCount."+blp.keyrangeTag, dmlCount)
	blp.globalState.txnCount.Add("TxnCount."+blp.keyrangeTag, 1)
}

//This function determines whether streaming is behind replication as it should be.
func (blp *Blp) slavePosBehindReplication() bool {
	repl, err := blp.logMetadata.GetCurrentReplicationPosition()
	if err != nil {
		relog.Error(err.Error())
		panic(NewBinlogParseError(fmt.Sprintf("Error in obtaining current replication position %v", err)))
	}
	currentCoord := blp.currentPosition.Position
	if repl.MasterFilename == currentCoord.MasterFilename {
		if currentCoord.MasterPosition <= repl.MasterPosition {
			return true
		}
	} else {
		replExt, err := strconv.ParseUint(strings.Split(repl.MasterFilename, ".")[1], 10, 64)
		if err != nil {
			relog.Error(err.Error())
			panic(NewBinlogParseError(fmt.Sprintf("Error in obtaining current replication position %v", err)))
		}
		parseExt, err := strconv.ParseUint(strings.Split(currentCoord.MasterFilename, ".")[1], 10, 64)
		if err != nil {
			relog.Error(err.Error())
			panic(NewBinlogParseError(fmt.Sprintf("Error in obtaining current replication position %v", err)))
		}
		if replExt >= parseExt {
			return true
		}
	}
	return false
}

//This builds BinlogResponse for each transaction.
func (blp *Blp) buildTxnResponse() (txnResponseList []*mysqlctl.BinlogResponse, dmlCount int64) {
	var line []byte
	var keyspaceIdStr string
	var keyspaceId key.KeyspaceId

	dmlBuffer := make([]string, 0, 10)

	for _, event := range blp.txnLineBuffer {
		line = event.LogLine
		if bytes.HasPrefix(line, mysqlctl.BINLOG_BEGIN) {
			streamBuf := new(mysqlctl.BinlogResponse)
			streamBuf.BlPosition = event.BlPosition
			streamBuf.SqlType = mysqlctl.BEGIN
			txnResponseList = append(txnResponseList, streamBuf)
			continue
		}
		if bytes.HasPrefix(line, mysqlctl.BINLOG_COMMIT) {
			commitEvent := createCommitEvent(event)
			txnResponseList = append(txnResponseList, commitEvent)
			continue
		}
		sqlType := mysqlctl.GetSqlType(event.firstKw)
		if sqlType == mysqlctl.DML {
			keyspaceIdStr, keyspaceId = parseKeyspaceId(line, mysqlctl.GetDmlType(event.firstKw))
			if keyspaceIdStr == "" {
				continue
			}
			if !blp.keyspaceRange.Contains(keyspaceId) {
				dmlBuffer = dmlBuffer[:0]
				continue
			}
			dmlCount += 1
			//extract keyspace id - match it with client's request,
			//extract index ids.
			dmlBuffer = append(dmlBuffer, string(line))
			dmlEvent := blp.createDmlEvent(event, keyspaceIdStr)
			dmlEvent.Sql = make([]string, len(dmlBuffer))
			dmlLines := copy(dmlEvent.Sql, dmlBuffer)
			if dmlLines < len(dmlBuffer) {
				relog.Warning("The entire dml buffer was not properly copied")
			}
			txnResponseList = append(txnResponseList, dmlEvent)
			dmlBuffer = dmlBuffer[:0]
		} else {
			//add as prefixes to the DML from last DML.
			//start a new dml buffer and keep adding to it.
			dmlBuffer = append(dmlBuffer, string(line))
		}
	}
	return txnResponseList, dmlCount
}

func (blp *Blp) createDmlEvent(eventBuf *eventBuffer, keyspaceId string) (dmlEvent *mysqlctl.BinlogResponse) {
	//parse keyspace id
	//for inserts check for index comments
	dmlEvent = new(mysqlctl.BinlogResponse)
	dmlEvent.BlPosition = eventBuf.BlPosition
	dmlEvent.SqlType = mysqlctl.GetDmlType(eventBuf.firstKw)
	dmlEvent.KeyspaceId = keyspaceId
	indexType, indexId, userId := parseIndex(eventBuf.LogLine)
	if userId != 0 {
		dmlEvent.UserId = userId
	}
	if indexType != "" {
		dmlEvent.IndexType = indexType
		dmlEvent.IndexId = indexId
	}
	return dmlEvent
}

func controlDbStatement(sql []byte, dmlType string) bool {
	sql = bytes.ToLower(sql)
	if bytes.Contains(sql, _VT) || (bytes.Contains(sql, ADMIN) && bytes.Contains(sql, HEARTBEAT)) {
		return true
	}
	return false
}

func parseKeyspaceId(sql []byte, dmlType string) (keyspaceIdStr string, keyspaceId key.KeyspaceId) {
	keyspaceIndex := bytes.Index(sql, KEYSPACE_ID_COMMENT)
	if keyspaceIndex == -1 {
		if controlDbStatement(sql, dmlType) {
			relog.Warning("Ignoring no keyspace id, control db stmt %v", string(sql))
			return
		}
		panic(NewBinlogParseError(fmt.Sprintf("Invalid Sql, doesn't contain keyspace id, sql: %v", string(sql))))
	}
	seekIndex := keyspaceIndex + len(KEYSPACE_ID_COMMENT)
	keyspaceIdComment := sql[seekIndex:]
	keyspaceIdStr = string(bytes.TrimSpace(bytes.SplitN(keyspaceIdComment, USER_ID, 2)[0]))
	if keyspaceIdStr == "" {
		panic(NewBinlogParseError(fmt.Sprintf("Invalid keyspace id, sql %v", string(sql))))
	}
	keyspaceIdUint, err := strconv.ParseUint(keyspaceIdStr, 10, 64)
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Invalid keyspaceid, error converting it, sql %v", string(sql))))
	}
	keyspaceId = key.Uint64Key(keyspaceIdUint).KeyspaceId()
	return keyspaceIdStr, keyspaceId
}

func parseIndex(sql []byte) (indexName string, indexId interface{}, userId uint64) {
	var err error
	keyspaceIndex := bytes.Index(sql, KEYSPACE_ID_COMMENT)
	if keyspaceIndex == -1 {
		panic(NewBinlogParseError(fmt.Sprintf("Error parsing index comment, doesn't contain keyspace id %v", string(sql))))
	}
	keyspaceIdComment := sql[keyspaceIndex+len(KEYSPACE_ID_COMMENT):]
	indexCommentStart := bytes.Index(keyspaceIdComment, INDEX_COMMENT)
	if indexCommentStart != -1 {
		indexCommentParts := bytes.SplitN(keyspaceIdComment[indexCommentStart:], COLON_BYTE, 2)
		userId, err = strconv.ParseUint(string(bytes.SplitN(indexCommentParts[1], mysqlctl.SPACE, 2)[0]), 10, 64)
		if err != nil {
			panic(NewBinlogParseError(fmt.Sprintf("Error converting user_id %v", string(sql))))
		}
		indexNameId := bytes.Split(indexCommentParts[0], DOT_BYTE)
		indexName = string(indexNameId[1])
		if indexName == "username" {
			indexId = string(bytes.TrimRight(indexNameId[2], COLON))
		} else {
			indexId, err = strconv.ParseUint(string(bytes.TrimRight(indexNameId[2], COLON)), 10, 64)
			if err != nil {
				panic(NewBinlogParseError(fmt.Sprintf("Error converting index id %v %v", string(bytes.TrimRight(indexNameId[2], COLON)), string(sql))))
			}
		}
	}
	return
}

//This creates the response for COMMIT event.
func createCommitEvent(eventBuf *eventBuffer) (streamBuf *mysqlctl.BinlogResponse) {
	streamBuf = new(mysqlctl.BinlogResponse)
	streamBuf.BlPosition = eventBuf.BlPosition
	streamBuf.SqlType = mysqlctl.COMMIT
	return
}

func isRequestValid(req *mysqlctl.BinlogServerRequest) bool {
	if req.StartPosition.MasterFilename == "" || req.StartPosition.MasterPosition == 0 {
		return false
	}
	if req.KeyspaceStart == "" && req.KeyspaceEnd == "" {
		return false
	}
	return true
}

//This sends the stream to the client.
func sendStream(sendReply mysqlctl.SendUpdateStreamResponse, responseBuf []*mysqlctl.BinlogResponse) (err error) {
	for _, event := range responseBuf {
		err = sendReply(event)
		if err != nil {
			return NewBinlogParseError(fmt.Sprintf("Error in sending reply to client, %v", err))
		}
	}
	return nil
}

//This sends the error to the client.
func sendError(sendReply mysqlctl.SendUpdateStreamResponse, reqIdentifier string, inputErr error, blpPos *mysqlctl.BlPosition) {
	var err error
	streamBuf := new(mysqlctl.BinlogResponse)
	streamBuf.Error = inputErr.Error()
	if blpPos != nil {
		streamBuf.BlPosition = *blpPos
	}
	buf := []*mysqlctl.BinlogResponse{streamBuf}
	err = sendStream(sendReply, buf)
	if err != nil {
		relog.Error("Error in communicating message %v with the client: %v", inputErr, err)
	}
}

func (blServer *BinlogServer) ServeBinlog(req *mysqlctl.BinlogServerRequest, sendReply mysqlctl.SendUpdateStreamResponse) error {
	defer func() {
		if x := recover(); x != nil {
			//Send the error to the client.
			_, ok := x.(*BinlogParseError)
			if !ok {
				relog.Error("Uncaught panic at top-most level: '%v'", x)
				//panic(x)
			}
			sendError(sendReply, req.StartPosition.String(), x.(error), nil)
		}
	}()

	relog.Info("received req: %v kr start %v end %v", req.StartPosition.String(), req.KeyspaceStart, req.KeyspaceEnd)
	if !isRequestValid(req) {
		panic(NewBinlogParseError("Invalid request, cannot serve the stream"))
	}

	usingRelayLogs := false
	var binlogPrefix, logsDir string
	if req.StartPosition.RelayFilename != "" {
		usingRelayLogs = true
		binlogPrefix = blServer.mycnf.RelayLogPath
		logsDir = path.Dir(binlogPrefix)
		if !mysqlctl.IsRelayPositionValid(&req.StartPosition, logsDir) {
			panic(NewBinlogParseError(fmt.Sprintf("Invalid start position %v, cannot serve the stream, cannot locate start position", req.StartPosition)))
		}
	} else {
		binlogPrefix = blServer.mycnf.BinLogPath
		logsDir = path.Dir(binlogPrefix)
		if !mysqlctl.IsMasterPositionValid(&req.StartPosition) {
			panic(NewBinlogParseError(fmt.Sprintf("Invalid start position %v, cannot serve the stream, cannot locate start position", req.StartPosition)))
		}
	}

	startKey, err := key.HexKeyspaceId(req.KeyspaceStart).Unhex()
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Unhex on key '%v' failed", req.KeyspaceStart)))
	}
	endKey, err := key.HexKeyspaceId(req.KeyspaceEnd).Unhex()
	if err != nil {
		panic(NewBinlogParseError(fmt.Sprintf("Unhex on key '%v' failed", req.KeyspaceEnd)))
	}
	keyRange := &key.KeyRange{Start: startKey, End: endKey}

	blp := NewBlp(&req.StartPosition, blServer, keyRange)
	blp.usingRelayLogs = usingRelayLogs
	blp.binlogPrefix = binlogPrefix
	blp.logMetadata = mysqlctl.NewSlaveMetadata(logsDir, blServer.mycnf.RelayLogInfoPath)

	relog.Info("usingRelayLogs %v blp.binlogPrefix %v logsDir %v", blp.usingRelayLogs, blp.binlogPrefix, logsDir)
	blp.streamBinlog(sendReply)
	return nil
}

func main() {
	flag.Parse()
	servenv.Init("vt_binlog_server")

	binlogServer := new(BinlogServer)
	if *mycnfFile == "" {
		relog.Fatal("Please specify the path for mycnf file.")
	}
	mycnf, err := mysqlctl.ReadMycnf(*mycnfFile)
	if err != nil {
		relog.Fatal("Error reading mycnf file %v", *mycnfFile)
	}
	binlogServer.mycnf = mycnf

	binlogServer.dbname = strings.ToLower(strings.TrimSpace(*dbname))
	binlogServer.blpStats = NewBlpStats()

	rpc.Register(binlogServer)
	rpcwrap.RegisterAuthenticated(binlogServer)
	//bsonrpc.ServeAuthRPC()

	rpc.HandleHTTP()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	umgmt.SetLameDuckPeriod(30.0)
	umgmt.SetRebindDelay(0.01)
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})
	umgmt.AddStartupCallback(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		go func() {
			for sig := range c {
				umgmt.SigTermHandler(sig)
			}
		}()
	})

	relog.Info("vt_binlog_server registered at port %v", *port)
	umgmtSocket := fmt.Sprintf("/tmp/vt_binlog_server-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}
