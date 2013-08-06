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
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/rpcwrap"
	estats "github.com/youtube/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

const (
	BINLOG_SERVER_DISABLED = iota
	BINLOG_SERVER_ENABLED
)

var (
	KEYSPACE_ID_COMMENT = []byte("/* EMD keyspace_id:")
	USER_ID             = []byte("user_id")
	END_COMMENT         = []byte("*/")
	_VT                 = []byte("_vt.")
	HEARTBEAT           = []byte("heartbeat")
	ADMIN               = []byte("admin")
)

type blsStats struct {
	parseStats    *estats.Counters
	dmlCount      *estats.Counters
	txnCount      *estats.Counters
	queriesPerSec *estats.Rates
	txnsPerSec    *estats.Rates
}

func newBlsStats() *blsStats {
	bs := &blsStats{}
	bs.parseStats = estats.NewCounters("")
	bs.txnCount = estats.NewCounters("")
	bs.dmlCount = estats.NewCounters("")
	bs.queriesPerSec = estats.NewRates("", bs.dmlCount, 15, 60e9)
	bs.txnsPerSec = estats.NewRates("", bs.txnCount, 15, 60e9)
	return bs
}

// String returns a json encoded version of stats
func (bs *blsStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\n \"DmlCount\": %v,", bs.dmlCount)
	fmt.Fprintf(buf, "\n \"ParseEvent\": %v,", bs.parseStats)
	fmt.Fprintf(buf, "\n \"QueriesPerSec\": %v,", bs.queriesPerSec)
	fmt.Fprintf(buf, "\n \"TxnCount\": %v,", bs.txnCount)
	fmt.Fprintf(buf, "\n \"TxnPerSec\": %v", bs.txnsPerSec)
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

type BinlogServer struct {
	dbname   string
	mycnf    *Mycnf
	blsStats *blsStats
	state    sync2.AtomicInt32
	states   *estats.States

	// interrupted is used to stop the serving clients when the service
	// gets interrupted. It is created when the service is enabled.
	// It is closed when the service is disabled.
	interrupted chan struct{}
}

//Raw event buffer used to gather data during parsing.
type blsEventBuffer struct {
	proto.BinlogPosition
	LogLine []byte
	firstKw string
}

func newBlsEventBuffer(pos *proto.BinlogPosition, line []byte) *blsEventBuffer {
	buf := &blsEventBuffer{}
	buf.LogLine = make([]byte, len(line))
	//buf.LogLine = append(buf.LogLine, line...)
	written := copy(buf.LogLine, line)
	if written < len(line) {
		log.Warningf("Problem in copying logline to new buffer, written %v, len %v", written, len(line))
	}
	buf.BinlogPosition = *pos
	buf.BinlogPosition.Timestamp = pos.Timestamp
	return buf
}

type Bls struct {
	nextStmtPosition uint64
	inTxn            bool
	txnLineBuffer    []*blsEventBuffer
	responseStream   []*proto.BinlogResponse
	initialSeek      bool
	startPosition    *proto.ReplicationCoordinates
	currentPosition  *proto.BinlogPosition
	dbmatch          bool
	keyspaceRange    key.KeyRange
	keyrangeTag      string
	globalState      *BinlogServer
	binlogPrefix     string
	//FIXME: this is for debug, remove it.
	currentLine string
	blsStats    *blsStats
}

func newBls(startCoordinates *proto.ReplicationCoordinates, blServer *BinlogServer, keyRange *key.KeyRange) *Bls {
	blp := &Bls{}
	blp.startPosition = startCoordinates
	blp.keyspaceRange = *keyRange
	blp.currentPosition = &proto.BinlogPosition{}
	blp.currentPosition.Position = *startCoordinates
	blp.inTxn = false
	blp.initialSeek = true
	blp.txnLineBuffer = make([]*blsEventBuffer, 0, MAX_TXN_BATCH)
	blp.responseStream = make([]*proto.BinlogResponse, 0, MAX_TXN_BATCH)
	blp.globalState = blServer
	//by default assume that the db matches.
	blp.dbmatch = true
	blp.keyrangeTag = string(keyRange.Start.Hex()) + "-" + string(keyRange.End.Hex())
	return blp
}

type BinlogServerError struct {
	Msg string
}

func newBinlogServerError(msg string) *BinlogServerError {
	return &BinlogServerError{Msg: msg}
}

func (err BinlogServerError) Error() string {
	return err.Msg
}

func (blp *Bls) streamBinlog(sendReply proto.SendBinlogResponse, interrupted chan struct{}) {
	var readErr error
	defer func() {
		reqIdentifier := fmt.Sprintf("%v, line: '%v'", blp.currentPosition.Position.String(), blp.currentLine)
		if x := recover(); x != nil {
			serr, ok := x.(*BinlogServerError)
			if !ok {
				log.Errorf("[%v:%v] Uncaught panic for stream @ %v, err: %v ", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, x)
				panic(x)
			}
			err := *serr
			if readErr != nil {
				log.Errorf("[%v:%v] StreamBinlog error @ %v, error: %v, readErr %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, err, readErr)
				err = BinlogServerError{Msg: fmt.Sprintf("%v, readErr: %v", err, readErr)}
			} else {
				log.Errorf("[%v:%v] StreamBinlog error @ %v, error: %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), reqIdentifier, err)
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
		panic(newBinlogServerError(pipeErr.Error()))
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
		panic(newBinlogServerError(err.Error()))
	}

	//This function monitors the exit of read data pipeline.
	go func(readErr *error, readErrChan chan error, binlogDecoder *BinlogDecoder) {
		select {
		case *readErr = <-readErrChan:
			//log.Infof("Read data-pipeline returned readErr: '%v'", *readErr)
			if *readErr != nil {
				binlogDecoder.Kill()
			}
		case <-interrupted:
			*readErr = fmt.Errorf("BinlogServer service disabled")
			binlogDecoder.Kill()
		}
	}(&readErr, readErrChan, binlogDecoder)

	blp.parseBinlogEvents(sendReply, binlogReader)
}

func (blp *Bls) getBinlogStream(writer *os.File, blr *BinlogReader, readErrChan chan error) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("getBinlogStream failed: %v", err)
			readErrChan <- err.(error)
		}
	}()
	blr.ServeData(writer, blp.startPosition.MasterFilename, int64(blp.startPosition.MasterPosition))
	readErrChan <- nil
}

//Main parse loop
func (blp *Bls) parseBinlogEvents(sendReply proto.SendBinlogResponse, binlogReader io.Reader) {
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
				panic(newBinlogServerError(fmt.Sprintf("EOF")))
			}
			panic(newBinlogServerError(fmt.Sprintf("ReadLine err: , %v", err)))
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
				event = newBlsEventBuffer(blp.currentPosition, line)
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
			log.Errorf("[%v:%v] Error in reading %v, data read %v", blp.keyspaceRange.Start.Hex(), blp.keyspaceRange.End.Hex(), tempErr, string(tempLine))
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
	if strings.ToLower(event.firstKw) != proto.USE {
		return
	}
	if blp.globalState.dbname == "" {
		log.Warningf("Dbname is not set, will match all database names")
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

func (blp *Bls) parseEventData(sendReply proto.SendBinlogResponse, event *blsEventBuffer) {
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
		if blp.inTxn && IsTxnStatement(event.LogLine, event.firstKw) {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		} else {
			sqlType := proto.GetSqlType(event.firstKw)
			switch sqlType {
			case proto.DDL:
				blp.handleDdlEvent(sendReply, event)
			case proto.DML:
				lineBuf := make([][]byte, 0, 10)
				for _, dml := range blp.txnLineBuffer {
					lineBuf = append(lineBuf, dml.LogLine)
				}
				// FIXME(alainjobart) in these cases, we
				// probably want to skip that event and keep
				// going. Or at least offer the option to do
				// so somewhere.
				panic(newBinlogServerError(fmt.Sprintf("DML outside a txn - len %v, dml '%v', txn buffer '%v'", len(blp.txnLineBuffer), string(event.LogLine), string(bytes.Join(lineBuf, SEMICOLON_BYTE)))))
			default:
				//Ignore these often occuring statement types.
				if !IgnoredStatement(event.LogLine) {
					log.Warningf("Unknown statement '%v'", string(event.LogLine))
				}
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
		panic(newBinlogServerError(fmt.Sprintf("Error in extracting master position, %v, sql %v, pos string %v", err, string(line), masterPosStr)))
	}
}

func (blp *Bls) parseXid(line []byte) {
	rem := bytes.SplitN(line, BINLOG_XID, 2)
	xid, err := strconv.ParseUint(string(rem[1]), 10, 64)
	if err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Error in extracting Xid position %v, sql %v", err, string(line))))
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
		panic(newBinlogServerError(fmt.Sprintf("Invalid timestamp line %v", string(line))))
	}
	currentTimestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Error in extracting timestamp %v, sql %v", err, string(line))))
	}
	blp.currentPosition.Timestamp = currentTimestamp
	event.BinlogPosition.Timestamp = currentTimestamp
}

func (blp *Bls) parseRotateEvent(line []byte) {
	rem := bytes.SplitN(line, BINLOG_ROTATE_TO, 2)
	rem2 := bytes.SplitN(rem[1], POS, 2)
	rotateFilename := strings.TrimSpace(string(rem2[0]))
	rotatePos, err := strconv.ParseUint(string(rem2[1]), 10, 64)
	if err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Error in extracting rotate pos %v from line %s", err, string(line))))
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
			panic(newBinlogServerError(fmt.Sprintf("BEGIN encountered with non-empty trxn buffer, len: %d, buf %v", len(blp.txnLineBuffer), string(bytes.Join(lineBuf, SEMICOLON_BYTE)))))
		} else {
			log.Warningf("Non-zero txn buffer, while inTxn false")
		}
	}
	blp.txnLineBuffer = blp.txnLineBuffer[:0]
	blp.inTxn = true
	blp.txnLineBuffer = append(blp.txnLineBuffer, event)
}

//This creates the response for DDL event.
func blsCreateDdlStream(lineBuffer *blsEventBuffer) (ddlStream *proto.BinlogResponse) {
	ddlStream = new(proto.BinlogResponse)
	ddlStream.Position = lineBuffer.BinlogPosition
	ddlStream.Data.SqlType = proto.DDL
	ddlStream.Data.Sql = make([]string, 0, 1)
	ddlStream.Data.Sql = append(ddlStream.Data.Sql, string(lineBuffer.LogLine))
	return ddlStream
}

func (blp *Bls) handleDdlEvent(sendReply proto.SendBinlogResponse, event *blsEventBuffer) {
	ddlStream := blsCreateDdlStream(event)
	buf := []*proto.BinlogResponse{ddlStream}
	if err := blsSendStream(sendReply, buf); err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Error in sending event to client %v", err)))
	}
	blp.globalState.blsStats.parseStats.Add("DdlCount."+blp.keyrangeTag, 1)
}

func (blp *Bls) handleCommitEvent(sendReply proto.SendBinlogResponse, commitEvent *blsEventBuffer) {
	if !blp.dbmatch {
		return
	}

	commitEvent.BinlogPosition.Xid = blp.currentPosition.Xid
	commitEvent.BinlogPosition.Position.GroupId = blp.currentPosition.Position.GroupId
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
		panic(newBinlogServerError(fmt.Sprintf("Error in sending event to client %v", err)))
	}

	blp.globalState.blsStats.dmlCount.Add("DmlCount."+blp.keyrangeTag, dmlCount)
	blp.globalState.blsStats.txnCount.Add("TxnCount."+blp.keyrangeTag, 1)
}

//This builds BinlogResponse for each transaction.
func (blp *Bls) buildTxnResponse() (txnResponseList []*proto.BinlogResponse, dmlCount int64) {
	var line []byte
	var keyspaceIdStr string
	var keyspaceId key.KeyspaceId

	dmlBuffer := make([]string, 0, 10)

	for _, event := range blp.txnLineBuffer {
		line = event.LogLine
		if bytes.HasPrefix(line, BINLOG_BEGIN) {
			streamBuf := new(proto.BinlogResponse)
			streamBuf.Position = event.BinlogPosition
			streamBuf.Data.SqlType = proto.BEGIN
			txnResponseList = append(txnResponseList, streamBuf)
			continue
		}
		if bytes.HasPrefix(line, BINLOG_COMMIT) {
			commitEvent := blsCreateCommitEvent(event)
			txnResponseList = append(txnResponseList, commitEvent)
			continue
		}
		sqlType := proto.GetSqlType(event.firstKw)
		if sqlType == proto.DML {
			keyspaceIdStr, keyspaceId = parseKeyspaceId(line)
			if keyspaceIdStr == "" {
				continue
			}
			if !blp.keyspaceRange.Contains(keyspaceId) {
				dmlBuffer = dmlBuffer[:0]
				continue
			}
			dmlCount += 1
			dmlBuffer = append(dmlBuffer, string(line))
			dmlEvent := blp.createDmlEvent(event, keyspaceIdStr)
			dmlEvent.Data.Sql = make([]string, len(dmlBuffer))
			dmlLines := copy(dmlEvent.Data.Sql, dmlBuffer)
			if dmlLines < len(dmlBuffer) {
				log.Warningf("The entire dml buffer was not properly copied")
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

func (blp *Bls) createDmlEvent(eventBuf *blsEventBuffer, keyspaceId string) (dmlEvent *proto.BinlogResponse) {
	//parse keyspace id
	//for inserts check for index comments
	dmlEvent = new(proto.BinlogResponse)
	dmlEvent.Position = eventBuf.BinlogPosition
	dmlEvent.Data.SqlType = proto.DML
	dmlEvent.Data.KeyspaceId = keyspaceId
	return dmlEvent
}

func controlDbStatement(sql []byte) bool {
	sql = bytes.ToLower(sql)
	if bytes.Contains(sql, _VT) || (bytes.Contains(sql, ADMIN) && bytes.Contains(sql, HEARTBEAT)) {
		return true
	}
	return false
}

func parseKeyspaceId(sql []byte) (keyspaceIdStr string, keyspaceId key.KeyspaceId) {
	keyspaceIndex := bytes.Index(sql, KEYSPACE_ID_COMMENT)
	if keyspaceIndex == -1 {
		if controlDbStatement(sql) {
			log.Warningf("Ignoring no keyspace id, control db stmt %v", string(sql))
			return
		}
		panic(newBinlogServerError(fmt.Sprintf("Invalid Sql, doesn't contain keyspace id, sql: %v", string(sql))))
	}
	seekIndex := keyspaceIndex + len(KEYSPACE_ID_COMMENT)
	keyspaceIdComment := sql[seekIndex:]
	keyspaceIdStr = string(bytes.TrimSpace(bytes.SplitN(keyspaceIdComment, USER_ID, 2)[0]))
	if keyspaceIdStr == "" {
		panic(newBinlogServerError(fmt.Sprintf("Invalid keyspace id, sql %v", string(sql))))
	}
	keyspaceIdUint, err := strconv.ParseUint(keyspaceIdStr, 10, 64)
	if err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Invalid keyspaceid, error converting it, sql %v", string(sql))))
	}
	keyspaceId = key.Uint64Key(keyspaceIdUint).KeyspaceId()
	return keyspaceIdStr, keyspaceId
}

//This creates the response for COMMIT event.
func blsCreateCommitEvent(eventBuf *blsEventBuffer) (streamBuf *proto.BinlogResponse) {
	streamBuf = new(proto.BinlogResponse)
	streamBuf.Position = eventBuf.BinlogPosition
	streamBuf.Data.SqlType = proto.COMMIT
	return
}

func isRequestValid(req *proto.BinlogServerRequest) bool {
	if req.StartPosition.MasterFilename == "" || req.StartPosition.MasterPosition == 0 {
		return false
	}
	if req.KeyspaceStart == "" && req.KeyspaceEnd == "" {
		return false
	}
	return true
}

//This sends the stream to the client.
func blsSendStream(sendReply proto.SendBinlogResponse, responseBuf []*proto.BinlogResponse) (err error) {
	for _, event := range responseBuf {
		err = sendReply(event)
		if err != nil {
			return newBinlogServerError(fmt.Sprintf("Error in sending reply to client, %v", err))
		}
	}
	return nil
}

//This sends the error to the client.
func sendError(sendReply proto.SendBinlogResponse, reqIdentifier string, inputErr error, blpPos *proto.BinlogPosition) {
	var err error
	streamBuf := new(proto.BinlogResponse)
	streamBuf.Error = inputErr.Error()
	if blpPos != nil {
		streamBuf.Position = *blpPos
	}
	buf := []*proto.BinlogResponse{streamBuf}
	err = blsSendStream(sendReply, buf)
	if err != nil {
		log.Errorf("Error in communicating message %v with the client: %v", inputErr, err)
	}
}

func (blServer *BinlogServer) ServeBinlog(req *proto.BinlogServerRequest, sendReply proto.SendBinlogResponse) error {
	defer func() {
		if x := recover(); x != nil {
			//Send the error to the client.
			_, ok := x.(*BinlogServerError)
			if !ok {
				log.Errorf("Uncaught panic at top-most level: '%v'", x)
				//panic(x)
			}
			sendError(sendReply, req.StartPosition.String(), x.(error), nil)
		}
	}()

	log.Infof("received req: %v kr start %v end %v", req.StartPosition.String(), req.KeyspaceStart, req.KeyspaceEnd)
	if !blServer.isServiceEnabled() {
		panic(newBinlogServerError("Binlog Server is disabled"))
	}
	if !isRequestValid(req) {
		panic(newBinlogServerError("Invalid request, cannot serve the stream"))
	}

	binlogPrefix := blServer.mycnf.BinLogPath
	logsDir := path.Dir(binlogPrefix)
	if !IsMasterPositionValid(&req.StartPosition) {
		panic(newBinlogServerError(fmt.Sprintf("Invalid start position %v, cannot serve the stream, cannot locate start position", req.StartPosition)))
	}

	startKey, err := key.HexKeyspaceId(req.KeyspaceStart).Unhex()
	if err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Unhex on key '%v' failed", req.KeyspaceStart)))
	}
	endKey, err := key.HexKeyspaceId(req.KeyspaceEnd).Unhex()
	if err != nil {
		panic(newBinlogServerError(fmt.Sprintf("Unhex on key '%v' failed", req.KeyspaceEnd)))
	}
	keyRange := &key.KeyRange{Start: startKey, End: endKey}

	blp := newBls(&req.StartPosition, blServer, keyRange)
	blp.binlogPrefix = binlogPrefix

	log.Infof("blp.binlogPrefix %v logsDir %v", blp.binlogPrefix, logsDir)
	blp.streamBinlog(sendReply, blServer.interrupted)
	return nil
}

func (blServer *BinlogServer) isServiceEnabled() bool {
	return blServer.state.Get() == BINLOG_SERVER_ENABLED
}

func (blServer *BinlogServer) setState(state int32) {
	blServer.state.Set(state)
	blServer.states.SetState(int(state))
}

func (blServer *BinlogServer) statsJSON() string {
	return fmt.Sprintf("{"+
		"\"Stats\": %v,"+
		"\"States\": %v"+
		"}", blServer.blsStats.String(), blServer.states.String())
}

func NewBinlogServer(mycnf *Mycnf) *BinlogServer {
	binlogServer := new(BinlogServer)
	binlogServer.mycnf = mycnf
	binlogServer.blsStats = newBlsStats()
	binlogServer.states = estats.NewStates("", []string{
		"Disabled",
		"Enabled",
	}, time.Now(), BINLOG_SERVER_DISABLED)
	return binlogServer
}

// RegisterBinlogServerService registers the service for serving and stats.
func RegisterBinlogServerService(blServer *BinlogServer) {
	rpcwrap.RegisterAuthenticated(blServer)
	estats.PublishFunc("BinlogServerRpcService", func() string { return blServer.statsJSON() })
}

// EnableBinlogServerService enabled the service for serving.
func EnableBinlogServerService(blServer *BinlogServer, dbname string) {
	if blServer.isServiceEnabled() {
		log.Warningf("Binlog Server service is already enabled")
		return
	}

	blServer.dbname = dbname
	blServer.interrupted = make(chan struct{}, 1)
	blServer.setState(BINLOG_SERVER_ENABLED)
	log.Infof("Binlog Server enabled")
}

// DisableBinlogServerService disables the service for serving.
func DisableBinlogServerService(blServer *BinlogServer) {
	//If the service is already disabled, just return
	if !blServer.isServiceEnabled() {
		return
	}
	blServer.setState(BINLOG_SERVER_DISABLED)
	close(blServer.interrupted)
	log.Infof("Binlog Server Disabled")
}

func IsBinlogServerEnabled(blServer *BinlogServer) bool {
	return blServer.isServiceEnabled()
}
