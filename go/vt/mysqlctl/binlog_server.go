// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt binlog server: Serves binlog for out of band replication.
package mysqlctl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/relog"
	gstats "github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/key"
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

type blsStats struct {
	parseStats    *gstats.Counters
	dmlCount      *gstats.Counters
	txnCount      *gstats.Counters
	queriesPerSec *gstats.Rates
	txnsPerSec    *gstats.Rates
}

func NewBlsStats() *blsStats {
	bs := &blsStats{}
	bs.parseStats = gstats.NewCounters("ParseEvent")
	bs.txnCount = gstats.NewCounters("TxnCount")
	bs.dmlCount = gstats.NewCounters("DmlCount")
	bs.queriesPerSec = gstats.NewRates("QueriesPerSec", bs.dmlCount, 15, 60e9)
	bs.txnsPerSec = gstats.NewRates("TxnPerSec", bs.txnCount, 15, 60e9)
	return bs
}

type BinlogServer struct {
	dbname   string
	mycnf    *Mycnf
	blsStats *blsStats
}

//Raw event buffer used to gather data during parsing.
type blsEventBuffer struct {
	BlPosition
	LogLine []byte
	firstKw string
}

func NewBlsEventBuffer(pos *BlPosition, line []byte) *blsEventBuffer {
	buf := &blsEventBuffer{}
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

type Bls struct {
	nextStmtPosition uint64
	inTxn            bool
	txnLineBuffer    []*blsEventBuffer
	responseStream   []*BinlogResponse
	initialSeek      bool
	startPosition    *ReplicationCoordinates
	currentPosition  *BlPosition
	dbmatch          bool
	keyspaceRange    key.KeyRange
	keyrangeTag      string
	globalState      *BinlogServer
	binlogPrefix     string
	//FIXME: this is for debug, remove it.
	currentLine string
	blsStats    *blsStats
}

func NewBls(startCoordinates *ReplicationCoordinates, blServer *BinlogServer, keyRange *key.KeyRange) *Bls {
	blp := &Bls{}
	blp.startPosition = startCoordinates
	blp.keyspaceRange = *keyRange
	blp.currentPosition = &BlPosition{}
	blp.currentPosition.Position = *startCoordinates
	blp.inTxn = false
	blp.initialSeek = true
	blp.txnLineBuffer = make([]*blsEventBuffer, 0, MAX_TXN_BATCH)
	blp.responseStream = make([]*BinlogResponse, 0, MAX_TXN_BATCH)
	blp.globalState = blServer
	//by default assume that the db matches.
	blp.dbmatch = true
	blp.keyrangeTag = string(keyRange.End.Hex())
	if blp.keyrangeTag == "" {
		blp.keyrangeTag = MAX_KEY
	}
	return blp
}

type BinlogServerError struct {
	Msg string
}

func NewBinlogServerError(msg string) *BinlogServerError {
	return &BinlogServerError{Msg: msg}
}

func (err BinlogServerError) Error() string {
	return err.Msg
}

func (blp *Bls) streamBinlog(sendReply SendUpdateStreamResponse) {
	var readErr error
	defer func() {
		reqIdentifier := fmt.Sprintf("%v, line: '%v'", blp.currentPosition.Position.String(), blp.currentLine)
		if x := recover(); x != nil {
			serr, ok := x.(*BinlogServerError)
			if !ok {
				relog.Error("[%v:%v] Uncaught panic for stream @ %v, err: %v ", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, x)
				panic(x)
			}
			err := *serr
			if readErr != nil {
				relog.Error("[%v:%v] StreamBinlog error @ %v, error: %v, readErr %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, err, readErr)
				err = BinlogServerError{Msg: fmt.Sprintf("%v, readErr: %v", err, readErr)}
			} else {
				relog.Error("[%v:%v] StreamBinlog error @ %v, error: %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, err)
			}
			sendError(sendReply, reqIdentifier, err, blp.currentPosition)
		}
	}()

	blr := NewBinlogReader(blp.binlogPrefix)

	var binlogReader io.Reader
	var blrReader, blrWriter *os.File
	var err, pipeErr error

	blrReader, blrWriter, pipeErr = os.Pipe()
	if pipeErr != nil {
		panic(NewBinlogServerError(pipeErr.Error()))
	}
	defer blrWriter.Close()
	defer blrReader.Close()

	readErrChan := make(chan error, 1)
	//This reads the binlogs - read end of data pipeline.
	go blp.getBinlogStream(blrWriter, blr, readErrChan)

	//Decode end of the data pipeline.
	binlogDecoder := new(BinlogDecoder)
	binlogReader, err = binlogDecoder.DecodeMysqlBinlog(blrReader)
	if err != nil {
		panic(NewBinlogServerError(err.Error()))
	}

	//This function monitors the exit of read data pipeline.
	go func(readErr *error, readErrChan chan error, binlogDecoder *BinlogDecoder) {
		*readErr = <-readErrChan
		//relog.Info("Read data-pipeline returned readErr: '%v'", *readErr)
		if *readErr != nil {
			binlogDecoder.Kill()
		}
	}(&readErr, readErrChan, binlogDecoder)

	blp.parseBinlogEvents(sendReply, binlogReader)
}

func (blp *Bls) getBinlogStream(writer *os.File, blr *BinlogReader, readErrChan chan error) {
	defer func() {
		if err := recover(); err != nil {
			relog.Error("getBinlogStream failed: %v", err)
			readErrChan <- err.(error)
		}
	}()
	blr.ServeData(writer, blp.startPosition.MasterFilename, int64(blp.startPosition.MasterPosition))
	readErrChan <- nil
}

//Main parse loop
func (blp *Bls) parseBinlogEvents(sendReply SendUpdateStreamResponse, binlogReader io.Reader) {
	// read over the stream and buffer up the transactions
	var err error
	var line []byte
	bigLine := make([]byte, 0, BINLOG_BLOCK_SIZE)
	lineReader := bufio.NewReaderSize(binlogReader, BINLOG_BLOCK_SIZE)
	readAhead := false
	var event *blsEventBuffer
	var delimIndex int

	for {
		line = line[:0]
		bigLine = bigLine[:0]
		line, err = blp.readBlsLine(lineReader, bigLine)
		if err != nil {
			if err == io.EOF {
				//end of stream
				blp.globalState.blsStats.parseStats.Add("EOFErrors."+blp.keyrangeTag, 1)
				panic(NewBinlogServerError(fmt.Sprintf("EOF")))
			}
			panic(NewBinlogServerError(fmt.Sprintf("ReadLine err: , %v", err)))
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

			if readAhead {
				event.LogLine = append(event.LogLine, line...)
			} else {
				event = NewBlsEventBuffer(blp.currentPosition, line)
			}

			delimIndex = bytes.LastIndex(event.LogLine, BINLOG_DELIMITER)
			if delimIndex != -1 {
				event.LogLine = event.LogLine[:delimIndex]
				readAhead = false
			} else {
				readAhead = true
				continue
			}

			event.LogLine = bytes.TrimSpace(event.LogLine)
			event.firstKw = string(bytes.ToLower(bytes.SplitN(event.LogLine, SPACE, 2)[0]))

			blp.currentLine = string(event.LogLine)

			//processes statements only for the dbname that it is subscribed to.
			blp.parseDbChange(event)
			blp.parseEventData(sendReply, event)
		}
	}
}

//This reads a binlog log line.
func (blp *Bls) readBlsLine(lineReader *bufio.Reader, bigLine []byte) (line []byte, err error) {
	for {
		tempLine, tempErr := lineReader.ReadSlice('\n')
		if tempErr == bufio.ErrBufferFull {
			bigLine = append(bigLine, tempLine...)
			blp.globalState.blsStats.parseStats.Add("BufferFullErrors."+blp.keyrangeTag, 1)
			continue
		} else if tempErr != nil {
			relog.Error("[%v:%v] Error in reading %v, data read %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), tempErr, string(tempLine))
			err = tempErr
		} else if len(bigLine) > 0 {
			if len(tempLine) > 0 {
				bigLine = append(bigLine, tempLine...)
			}
			line = bigLine[:len(bigLine)-1]
			blp.globalState.blsStats.parseStats.Add("BigLineCount."+blp.keyrangeTag, 1)
		} else {
			line = tempLine[:len(tempLine)-1]
		}
		break
	}
	return line, err
}

//Function to set the dbmatch variable, this parses the "Use <dbname>" statement.
func (blp *Bls) parseDbChange(event *blsEventBuffer) {
	if event.firstKw != USE {
		return
	}
	if blp.globalState.dbname == "" {
		relog.Warning("Dbname is not set, will match all database names")
		return
	}
	blp.globalState.blsStats.parseStats.Add("DBChange."+blp.keyrangeTag, 1)

	new_db := string(bytes.TrimSpace(bytes.SplitN(event.LogLine, BINLOG_DB_CHANGE, 2)[1]))
	if new_db != blp.globalState.dbname {
		blp.dbmatch = false
	} else {
		blp.dbmatch = true
	}
}

func (blp *Bls) parsePositionData(line []byte) {
	if bytes.HasPrefix(line, BINLOG_POSITION_PREFIX) {
		//Master Position
		if blp.nextStmtPosition == 0 {
			return
		}
	} else if bytes.Index(line, BINLOG_ROTATE_TO) != -1 {
		blp.parseRotateEvent(line)
	} else if bytes.Index(line, BINLOG_END_LOG_POS) != -1 {
		//Ignore the position data that appears at the start line of binlog.
		if bytes.Index(line, BINLOG_START) != -1 {
			return
		}
		blp.parseMasterPosition(line)
		if blp.nextStmtPosition != 0 {
			blp.currentPosition.Position.MasterPosition = blp.nextStmtPosition
		}
	}
	if bytes.Index(line, BINLOG_XID) != -1 {
		blp.parseXid(line)
	}
	// FIXME(shrutip): group_id is most relevant for commit events
	// check how group_id is set for ddls and possibly move this block
	// in parseXid
	if bytes.Index(line, BINLOG_GROUP_ID) != -1 {
		blp.parseGroupId(line)
	}
}

func (blp *Bls) parseEventData(sendReply SendUpdateStreamResponse, event *blsEventBuffer) {
	if bytes.HasPrefix(event.LogLine, BINLOG_SET_TIMESTAMP) {
		blp.extractEventTimestamp(event)
		blp.initialSeek = false
		if blp.inTxn {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		}
	} else if bytes.HasPrefix(event.LogLine, BINLOG_BEGIN) {
		blp.handleBeginEvent(event)
	} else if bytes.HasPrefix(event.LogLine, BINLOG_ROLLBACK) {
		blp.inTxn = false
		blp.txnLineBuffer = blp.txnLineBuffer[:0]
	} else if bytes.HasPrefix(event.LogLine, BINLOG_COMMIT) {
		blp.handleCommitEvent(sendReply, event)
		blp.inTxn = false
		blp.txnLineBuffer = blp.txnLineBuffer[:0]
	} else if len(event.LogLine) > 0 {
		sqlType := GetSqlType(event.firstKw)
		if blp.inTxn && IsTxnStatement(event.LogLine, event.firstKw) {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		} else if sqlType == DDL {
			blp.handleDdlEvent(sendReply, event)
		} else {
			if sqlType == DML {
				lineBuf := make([][]byte, 0, 10)
				for _, dml := range blp.txnLineBuffer {
					lineBuf = append(lineBuf, dml.LogLine)
				}
				panic(NewBinlogServerError(fmt.Sprintf("DML outside a txn - len %v, dml '%v', txn buffer '%v'", len(blp.txnLineBuffer), string(event.LogLine), string(bytes.Join(lineBuf, SEMICOLON_BYTE)))))
			}
			//Ignore these often occuring statement types.
			if !IgnoredStatement(event.LogLine) {
				relog.Warning("Unknown statement '%v'", string(event.LogLine))
			}
		}
	}
}

/*
Position Parsing Functions.
*/

func (blp *Bls) parseMasterPosition(line []byte) {
	var err error
	rem := bytes.SplitN(line, BINLOG_END_LOG_POS, 2)
	masterPosStr := string(bytes.SplitN(rem[1], SPACE, 2)[0])
	blp.nextStmtPosition, err = strconv.ParseUint(masterPosStr, 10, 64)
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Error in extracting master position, %v, sql %v, pos string %v", err, string(line), masterPosStr)))
	}
}

func (blp *Bls) parseXid(line []byte) {
	rem := bytes.SplitN(line, BINLOG_XID, 2)
	xid, err := strconv.ParseUint(string(rem[1]), 10, 64)
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Error in extracting Xid position %v, sql %v", err, string(line))))
	}
	blp.currentPosition.Xid = xid
}

func (blp *Bls) parseGroupId(line []byte) {
	rem := bytes.SplitN(line, BINLOG_GROUP_ID, 2)
	rem2 := bytes.SplitN(rem[1], SPACE, 2)
	groupId := strings.TrimSpace(string(rem2[0]))
	blp.currentPosition.Position.GroupId = groupId
}

func (blp *Bls) extractEventTimestamp(event *blsEventBuffer) {
	line := event.LogLine
	timestampStr := string(line[len(BINLOG_SET_TIMESTAMP):])
	if timestampStr == "" {
		panic(NewBinlogServerError(fmt.Sprintf("Invalid timestamp line %v", string(line))))
	}
	currentTimestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Error in extracting timestamp %v, sql %v", err, string(line))))
	}
	blp.currentPosition.Timestamp = currentTimestamp
	event.BlPosition.Timestamp = currentTimestamp
}

func (blp *Bls) parseRotateEvent(line []byte) {
	rem := bytes.SplitN(line, BINLOG_ROTATE_TO, 2)
	rem2 := bytes.SplitN(rem[1], POS, 2)
	rotateFilename := strings.TrimSpace(string(rem2[0]))
	rotatePos, err := strconv.ParseUint(string(rem2[1]), 10, 64)
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Error in extracting rotate pos %v from line %s", err, string(line))))
	}

	//If the file being parsed is a binlog,
	//then the rotate events only correspond to itself.
	blp.currentPosition.Position.MasterFilename = rotateFilename
	blp.currentPosition.Position.MasterPosition = rotatePos
	blp.globalState.blsStats.parseStats.Add("BinlogRotate."+blp.keyrangeTag, 1)
}

/*
Data event parsing and handling functions.
*/

func (blp *Bls) handleBeginEvent(event *blsEventBuffer) {
	if len(blp.txnLineBuffer) > 0 {
		if blp.inTxn {
			lineBuf := make([][]byte, 0, 10)
			for _, event := range blp.txnLineBuffer {
				lineBuf = append(lineBuf, event.LogLine)
			}
			panic(NewBinlogServerError(fmt.Sprintf("BEGIN encountered with non-empty trxn buffer, len: %d, buf %v", len(blp.txnLineBuffer), string(bytes.Join(lineBuf, SEMICOLON_BYTE)))))
		} else {
			relog.Warning("Non-zero txn buffer, while inTxn false")
		}
	}
	blp.txnLineBuffer = blp.txnLineBuffer[:0]
	blp.inTxn = true
	blp.txnLineBuffer = append(blp.txnLineBuffer, event)
}

//This creates the response for DDL event.
func blsCreateDdlStream(lineBuffer *blsEventBuffer) (ddlStream *BinlogResponse) {
	ddlStream = new(BinlogResponse)
	ddlStream.BlPosition = lineBuffer.BlPosition
	ddlStream.SqlType = DDL
	ddlStream.Sql = make([]string, 0, 1)
	ddlStream.Sql = append(ddlStream.Sql, string(lineBuffer.LogLine))
	return ddlStream
}

func (blp *Bls) handleDdlEvent(sendReply SendUpdateStreamResponse, event *blsEventBuffer) {
	ddlStream := blsCreateDdlStream(event)
	buf := []*BinlogResponse{ddlStream}
	if err := blsSendStream(sendReply, buf); err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Error in sending event to client %v", err)))
	}
	blp.globalState.blsStats.parseStats.Add("DdlCount."+blp.keyrangeTag, 1)
}

func (blp *Bls) handleCommitEvent(sendReply SendUpdateStreamResponse, commitEvent *blsEventBuffer) {
	if !blp.dbmatch {
		return
	}

	commitEvent.BlPosition.Xid = blp.currentPosition.Xid
	commitEvent.BlPosition.Position.GroupId = blp.currentPosition.Position.GroupId
	blp.txnLineBuffer = append(blp.txnLineBuffer, commitEvent)
	//txn block for DMLs, parse it and send events for a txn
	var dmlCount int64
	//This filters the dmls for keyrange supplied by the client.
	blp.responseStream, dmlCount = blp.buildTxnResponse()

	//No dmls matched the keyspace id so return
	if dmlCount == 0 {
		return
	}

	if err := blsSendStream(sendReply, blp.responseStream); err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Error in sending event to client %v", err)))
	}

	blp.globalState.blsStats.dmlCount.Add("DmlCount."+blp.keyrangeTag, dmlCount)
	blp.globalState.blsStats.txnCount.Add("TxnCount."+blp.keyrangeTag, 1)
}

//This builds BinlogResponse for each transaction.
func (blp *Bls) buildTxnResponse() (txnResponseList []*BinlogResponse, dmlCount int64) {
	var line []byte
	var keyspaceIdStr string
	var keyspaceId key.KeyspaceId

	dmlBuffer := make([]string, 0, 10)

	for _, event := range blp.txnLineBuffer {
		line = event.LogLine
		if bytes.HasPrefix(line, BINLOG_BEGIN) {
			streamBuf := new(BinlogResponse)
			streamBuf.BlPosition = event.BlPosition
			streamBuf.SqlType = BEGIN
			txnResponseList = append(txnResponseList, streamBuf)
			continue
		}
		if bytes.HasPrefix(line, BINLOG_COMMIT) {
			commitEvent := blsCreateCommitEvent(event)
			txnResponseList = append(txnResponseList, commitEvent)
			continue
		}
		sqlType := GetSqlType(event.firstKw)
		if sqlType == DML {
			keyspaceIdStr, keyspaceId = parseKeyspaceId(line, GetDmlType(event.firstKw))
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

func (blp *Bls) createDmlEvent(eventBuf *blsEventBuffer, keyspaceId string) (dmlEvent *BinlogResponse) {
	//parse keyspace id
	//for inserts check for index comments
	dmlEvent = new(BinlogResponse)
	dmlEvent.BlPosition = eventBuf.BlPosition
	dmlEvent.SqlType = GetDmlType(eventBuf.firstKw)
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
		panic(NewBinlogServerError(fmt.Sprintf("Invalid Sql, doesn't contain keyspace id, sql: %v", string(sql))))
	}
	seekIndex := keyspaceIndex + len(KEYSPACE_ID_COMMENT)
	keyspaceIdComment := sql[seekIndex:]
	keyspaceIdStr = string(bytes.TrimSpace(bytes.SplitN(keyspaceIdComment, USER_ID, 2)[0]))
	if keyspaceIdStr == "" {
		panic(NewBinlogServerError(fmt.Sprintf("Invalid keyspace id, sql %v", string(sql))))
	}
	keyspaceIdUint, err := strconv.ParseUint(keyspaceIdStr, 10, 64)
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Invalid keyspaceid, error converting it, sql %v", string(sql))))
	}
	keyspaceId = key.Uint64Key(keyspaceIdUint).KeyspaceId()
	return keyspaceIdStr, keyspaceId
}

func parseIndex(sql []byte) (indexName string, indexId interface{}, userId uint64) {
	var err error
	keyspaceIndex := bytes.Index(sql, KEYSPACE_ID_COMMENT)
	if keyspaceIndex == -1 {
		panic(NewBinlogServerError(fmt.Sprintf("Error parsing index comment, doesn't contain keyspace id %v", string(sql))))
	}
	keyspaceIdComment := sql[keyspaceIndex+len(KEYSPACE_ID_COMMENT):]
	indexCommentStart := bytes.Index(keyspaceIdComment, INDEX_COMMENT)
	if indexCommentStart != -1 {
		indexCommentParts := bytes.SplitN(keyspaceIdComment[indexCommentStart:], COLON_BYTE, 2)
		userId, err = strconv.ParseUint(string(bytes.SplitN(indexCommentParts[1], SPACE, 2)[0]), 10, 64)
		if err != nil {
			panic(NewBinlogServerError(fmt.Sprintf("Error converting user_id %v", string(sql))))
		}
		indexNameId := bytes.Split(indexCommentParts[0], DOT_BYTE)
		indexName = string(indexNameId[1])
		if indexName == "username" {
			indexId = string(bytes.TrimRight(indexNameId[2], COLON))
		} else {
			indexId, err = strconv.ParseUint(string(bytes.TrimRight(indexNameId[2], COLON)), 10, 64)
			if err != nil {
				panic(NewBinlogServerError(fmt.Sprintf("Error converting index id %v %v", string(bytes.TrimRight(indexNameId[2], COLON)), string(sql))))
			}
		}
	}
	return
}

//This creates the response for COMMIT event.
func blsCreateCommitEvent(eventBuf *blsEventBuffer) (streamBuf *BinlogResponse) {
	streamBuf = new(BinlogResponse)
	streamBuf.BlPosition = eventBuf.BlPosition
	streamBuf.SqlType = COMMIT
	return
}

func isRequestValid(req *BinlogServerRequest) bool {
	if req.StartPosition.MasterFilename == "" || req.StartPosition.MasterPosition == 0 {
		return false
	}
	if req.KeyspaceStart == "" && req.KeyspaceEnd == "" {
		return false
	}
	return true
}

//This sends the stream to the client.
func blsSendStream(sendReply SendUpdateStreamResponse, responseBuf []*BinlogResponse) (err error) {
	for _, event := range responseBuf {
		err = sendReply(event)
		if err != nil {
			return NewBinlogServerError(fmt.Sprintf("Error in sending reply to client, %v", err))
		}
	}
	return nil
}

//This sends the error to the client.
func sendError(sendReply SendUpdateStreamResponse, reqIdentifier string, inputErr error, blpPos *BlPosition) {
	var err error
	streamBuf := new(BinlogResponse)
	streamBuf.Error = inputErr.Error()
	if blpPos != nil {
		streamBuf.BlPosition = *blpPos
	}
	buf := []*BinlogResponse{streamBuf}
	err = blsSendStream(sendReply, buf)
	if err != nil {
		relog.Error("Error in communicating message %v with the client: %v", inputErr, err)
	}
}

func (blServer *BinlogServer) ServeBinlog(req *BinlogServerRequest, sendReply SendUpdateStreamResponse) error {
	defer func() {
		if x := recover(); x != nil {
			//Send the error to the client.
			_, ok := x.(*BinlogServerError)
			if !ok {
				relog.Error("Uncaught panic at top-most level: '%v'", x)
				//panic(x)
			}
			sendError(sendReply, req.StartPosition.String(), x.(error), nil)
		}
	}()

	relog.Info("received req: %v kr start %v end %v", req.StartPosition.String(), req.KeyspaceStart, req.KeyspaceEnd)
	if !isRequestValid(req) {
		panic(NewBinlogServerError("Invalid request, cannot serve the stream"))
	}

	binlogPrefix := blServer.mycnf.BinLogPath
	logsDir := path.Dir(binlogPrefix)
	if !IsMasterPositionValid(&req.StartPosition) {
		panic(NewBinlogServerError(fmt.Sprintf("Invalid start position %v, cannot serve the stream, cannot locate start position", req.StartPosition)))
	}

	startKey, err := key.HexKeyspaceId(req.KeyspaceStart).Unhex()
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Unhex on key '%v' failed", req.KeyspaceStart)))
	}
	endKey, err := key.HexKeyspaceId(req.KeyspaceEnd).Unhex()
	if err != nil {
		panic(NewBinlogServerError(fmt.Sprintf("Unhex on key '%v' failed", req.KeyspaceEnd)))
	}
	keyRange := &key.KeyRange{Start: startKey, End: endKey}

	blp := NewBls(&req.StartPosition, blServer, keyRange)
	blp.binlogPrefix = binlogPrefix

	relog.Info("blp.binlogPrefix %v logsDir %v", blp.binlogPrefix, logsDir)
	blp.streamBinlog(sendReply)
	return nil
}

func NewBinlogServer(mycnf *Mycnf, dbname string) *BinlogServer {
	binlogServer := new(BinlogServer)
	binlogServer.mycnf = mycnf
	binlogServer.dbname = strings.ToLower(strings.TrimSpace(dbname))
	binlogServer.blsStats = NewBlsStats()
	return binlogServer
}
