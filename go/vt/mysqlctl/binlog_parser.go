// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	"code.google.com/p/vitess/go/relog"
	parser "code.google.com/p/vitess/go/vt/sqlparser"
)

/*
Functionality to parse a binlog/relay log and send event streams to clients.
*/

const (
	MAX_TXN_BATCH = 1024
	DML           = "DML"
	DDL           = "DDL"
	BEGIN         = "BEGIN"
	COMMIT        = "COMMIT"
	USE           = "use"
	EOF           = "EOF"
)

var SqlKwMap = map[string]string{
	"create":   DDL,
	"alter":    DDL,
	"drop":     DDL,
	"truncate": DDL,
	"rename":   DDL,
	"insert":   DML,
	"update":   DML,
	"delete":   DML,
	"begin":    BEGIN,
	"commit":   COMMIT,
}

var (
	STREAM_COMMENT_START   = []byte("/* _stream ")
	BINLOG_DELIMITER       = []byte("/*!*/;")
	BINLOG_POSITION_PREFIX = []byte("# at ")
	BINLOG_ROTATE_TO       = []byte("Rotate to ")
	BINLOG_BEGIN           = []byte("BEGIN")
	BINLOG_COMMIT          = []byte("COMMIT")
	BINLOG_ROLLBACK        = []byte("ROLLBACK")
	BINLOG_SET_TIMESTAMP   = []byte("SET TIMESTAMP=")
	BINLOG_SET_INSERT      = []byte("SET INSERT_ID=")
	BINLOG_END_LOG_POS     = []byte("end_log_pos ")
	BINLOG_XID             = []byte("Xid = ")
	BINLOG_GROUP_ID        = []byte("group_id ")
	BINLOG_START           = []byte("Start: binlog")
	BINLOG_DB_CHANGE       = []byte(USE)
	POS                    = []byte(" pos: ")
	SPACE                  = []byte(" ")
	COMMENT                = []byte("/*!")
	SET_SESSION_VAR        = []byte("SET @@session")
	DELIMITER              = []byte("DELIMITER ")
	BINLOG                 = []byte("BINLOG ")
	SEMICOLON_BYTE         = []byte(";")
)

type BinlogPosition struct {
	Position  ReplicationCoordinates
	Timestamp int64
	Xid       uint64
	GroupId   uint64
}

func (pos *BinlogPosition) String() string {
	return fmt.Sprintf("%v:%v", pos.Position.MasterFilename, pos.Position.MasterPosition)
}

func (pos *BinlogPosition) Valid() bool {
	if pos.Position.MasterFilename == "" || pos.Position.MasterPosition == 0 {
		return false
	}
	return true
}

func (pos *BinlogPosition) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Object, "Position")
	pos.encodeReplCoordinates(buf)

	bson.EncodePrefix(buf, bson.Long, "Timestamp")
	bson.EncodeUint64(buf, uint64(pos.Timestamp))

	bson.EncodePrefix(buf, bson.Ulong, "Xid")
	bson.EncodeUint64(buf, pos.Xid)

	bson.EncodePrefix(buf, bson.Ulong, "GroupId")
	bson.EncodeUint64(buf, pos.GroupId)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (pos *BinlogPosition) encodeReplCoordinates(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "RelayFilename")
	bson.EncodeString(buf, pos.Position.RelayFilename)

	bson.EncodePrefix(buf, bson.Ulong, "RelayPosition")
	bson.EncodeUint64(buf, pos.Position.RelayPosition)

	bson.EncodePrefix(buf, bson.Binary, "MasterFilename")
	bson.EncodeString(buf, pos.Position.MasterFilename)

	bson.EncodePrefix(buf, bson.Ulong, "MasterPosition")
	bson.EncodeUint64(buf, pos.Position.MasterPosition)
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (pos *BinlogPosition) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Position":
			pos.decodeReplCoordBson(buf, kind)
		case "Timestamp":
			pos.Timestamp = bson.DecodeInt64(buf, kind)
		case "Xid":
			pos.Xid = bson.DecodeUint64(buf, kind)
		case "GroupId":
			pos.GroupId = bson.DecodeUint64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (pos *BinlogPosition) decodeReplCoordBson(buf *bytes.Buffer, kind byte) {
	pos.Position = ReplicationCoordinates{}
	bson.Next(buf, 4)
	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "RelayFilename":
			pos.Position.RelayFilename = bson.DecodeString(buf, kind)
		case "RelayPosition":
			pos.Position.RelayPosition = bson.DecodeUint64(buf, kind)
		case "MasterFilename":
			pos.Position.MasterFilename = bson.DecodeString(buf, kind)
		case "MasterPosition":
			pos.Position.MasterPosition = bson.DecodeUint64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

//Api Interface
type UpdateResponse struct {
	Error string
	BinlogPosition
	EventData
}

type EventData struct {
	SqlType    string
	TableName  string
	Sql        string
	PkColNames []string
	PkValues   [][]interface{}
}

//Raw event buffer used to gather data during parsing.
type eventBuffer struct {
	BinlogPosition
	LogLine []byte
	firstKw string
}

func NewEventBuffer(pos *BinlogPosition, line []byte) *eventBuffer {
	buf := &eventBuffer{}
	buf.LogLine = make([]byte, len(line))
	written := copy(buf.LogLine, line)
	if written < len(line) {
		relog.Warning("Logline not properly copied while creating new event written: %v len: %v", written, len(line))
	}
	//The RelayPosition is never used, so using the default value
	buf.BinlogPosition.Position = ReplicationCoordinates{RelayFilename: pos.Position.RelayFilename,
		MasterFilename: pos.Position.MasterFilename,
		MasterPosition: pos.Position.MasterPosition,
	}
	buf.BinlogPosition.Timestamp = pos.Timestamp
	return buf
}

type blpStats struct {
	TimeStarted      time.Time
	RotateEventCount uint16
	TxnCount         uint64
	DmlCount         uint64
	DdlCount         uint64
	LineCount        uint64
	BigLineCount     uint64
	BufferFullErrors uint64
	LinesPerSecond   float64
	TxnsPerSecond    float64
}

func (stats blpStats) DebugJsonString() string {
	elapsed := float64(time.Now().Sub(stats.TimeStarted)) / 1e9
	stats.LinesPerSecond = float64(stats.LineCount) / elapsed
	stats.TxnsPerSecond = float64(stats.TxnCount) / elapsed
	data, err := json.MarshalIndent(stats, "  ", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

type Blp struct {
	nextStmtPosition uint64
	inTxn            bool
	txnLineBuffer    []*eventBuffer
	responseStream   []*UpdateResponse
	initialSeek      bool
	startPosition    *ReplicationCoordinates
	currentPosition  *BinlogPosition
	globalState      *UpdateStream
	dbmatch          bool
	blpStats
}

func NewBlp(startCoordinates *ReplicationCoordinates, updateStream *UpdateStream) *Blp {
	blp := &Blp{}
	blp.startPosition = startCoordinates
	//The RelayPosition is never used, so using the default value
	currentCoord := NewReplicationCoordinates(startCoordinates.RelayFilename,
		0,
		startCoordinates.MasterFilename,
		startCoordinates.MasterPosition)
	blp.currentPosition = &BinlogPosition{Position: *currentCoord}
	blp.currentPosition.Position.RelayPosition = 0
	blp.inTxn = false
	blp.initialSeek = true
	blp.txnLineBuffer = make([]*eventBuffer, 0, MAX_TXN_BATCH)
	blp.responseStream = make([]*UpdateResponse, 0, MAX_TXN_BATCH)
	blp.globalState = updateStream
	//by default assume that the db matches.
	blp.dbmatch = true
	return blp
}

// Error types for update stream
const (
	FATAL             = "Fatal"
	SERVICE_ERROR     = "Service Error"
	EVENT_ERROR       = "Event Error"
	CODE_ERROR        = "Code Error"
	REPLICATION_ERROR = "Replication Error"
	CONNECTION_ERROR  = "Connection Error"
)

type BinlogParseError struct {
	errType string
	msg     string
}

func NewBinlogParseError(errType, msg string) *BinlogParseError {
	return &BinlogParseError{errType: errType, msg: msg}
}

func (err BinlogParseError) Error() string {
	errStr := fmt.Sprintf("%v: %v", err.errType, err.msg)
	if err.IsFatal() {
		return FATAL + " " + errStr
	}
	return errStr
}

func (err *BinlogParseError) IsFatal() bool {
	return (err.errType != EVENT_ERROR)
}

func (err *BinlogParseError) IsEOF() bool {
	return err.msg == EOF
}

type SendUpdateStreamResponse func(response interface{}) error

func (blp *Blp) isBehindReplication() (bool, error) {
	rp, replErr := blp.globalState.getReplicationPosition()
	if replErr != nil {
		return false, replErr
	}
	if rp.MasterFilename != blp.currentPosition.Position.MasterFilename ||
		rp.MasterPosition != blp.currentPosition.Position.MasterPosition {
		return false, nil
	}
	return true, nil
}

//Main entry function for reading and parsing the binlog.
func (blp *Blp) StreamBinlog(sendReply SendUpdateStreamResponse, binlogPrefix string) {
	var err error
	for {
		err = blp.streamBinlog(sendReply, binlogPrefix)
		sErr, ok := err.(*BinlogParseError)
		if ok && sErr.IsEOF() {
			// Double check the current parse position
			// with replication position, if not so, it is an error
			// otherwise it is a true EOF so retry.
			relog.Info("EOF, retrying")
			ok, replErr := blp.isBehindReplication()
			if replErr != nil {
				err = replErr
			} else if !ok {
				err = NewBinlogParseError(REPLICATION_ERROR, "EOF, but parse position behind replication")
			} else {
				time.Sleep(5.0 * time.Second)
				blp.startPosition = &ReplicationCoordinates{MasterFilename: blp.currentPosition.Position.MasterFilename,
					MasterPosition: blp.currentPosition.Position.MasterPosition,
					RelayFilename:  blp.currentPosition.Position.RelayFilename,
					RelayPosition:  blp.currentPosition.Position.RelayPosition}
				continue
			}
		}
		relog.Error("StreamBinlog error @ %v, error: %v", blp.currentPosition.String(), err.Error())
		SendError(sendReply, err, blp.currentPosition)
		break
	}
}

func (blp *Blp) handleError(err *error, readErr error) {
	reqIdentifier := blp.currentPosition.String()
	if x := recover(); x != nil {
		serr, ok := x.(*BinlogParseError)
		if !ok {
			relog.Error("Uncaught panic for stream @ %v", reqIdentifier)
			panic(x)
		}
		*err = NewBinlogParseError(serr.errType, serr.msg)
		if readErr != nil {
			*err = NewBinlogParseError(REPLICATION_ERROR, fmt.Sprintf("%v, readErr: %v", serr.Error(), readErr))
		}
	}
}

func (blp *Blp) streamBinlog(sendReply SendUpdateStreamResponse, binlogPrefix string) (err error) {
	var readErr error
	defer blp.handleError(&err, readErr)

	var binlogReader io.Reader
	blr := NewBinlogReader(binlogPrefix)

	var blrReader, blrWriter *os.File
	var pipeErr error

	blrReader, blrWriter, pipeErr = os.Pipe()
	if pipeErr != nil {
		panic(NewBinlogParseError(CODE_ERROR, pipeErr.Error()))
	}
	defer blrWriter.Close()
	defer blrReader.Close()

	readErrChan := make(chan error, 1)
	//This reads the binlogs - read end of data pipeline.
	go blp.getBinlogStream(blrWriter, blr, readErrChan)
	binlogDecoder := new(BinlogDecoder)
	binlogReader, err = binlogDecoder.DecodeMysqlBinlog(blrReader)
	if err != nil {
		panic(NewBinlogParseError(REPLICATION_ERROR, err.Error()))
	}
	//This function monitors the exit of read data pipeline.
	go func(readErr *error, readErrChan chan error, binlogDecoder *BinlogDecoder) {
		*readErr = <-readErrChan
		if *readErr != nil {
			binlogDecoder.Kill()
		}
	}(&readErr, readErrChan, binlogDecoder)

	blp.parseBinlogEvents(sendReply, binlogReader)
	return nil
}

//Main parse loop
func (blp *Blp) parseBinlogEvents(sendReply SendUpdateStreamResponse, binlogReader io.Reader) {
	// read over the stream and buffer up the transactions
	var err error
	var line []byte
	bigLine := make([]byte, 0, BINLOG_BLOCK_SIZE)
	lineReader := bufio.NewReaderSize(binlogReader, BINLOG_BLOCK_SIZE)
	readAhead := false
	var event *eventBuffer
	var delimIndex int

	for {
		if !blp.globalState.isServiceEnabled() {
			panic(NewBinlogParseError(SERVICE_ERROR, "Disconnecting because the Update Stream service has been disabled"))
		}
		line = line[:0]
		bigLine = bigLine[:0]
		line, err = blp.readBlpLine(lineReader, bigLine)
		if err != nil {
			if err == io.EOF {
				//end of stream
				panic(NewBinlogParseError(REPLICATION_ERROR, EOF))
			}
			panic(NewBinlogParseError(REPLICATION_ERROR, fmt.Sprintf("ReadLine err: , %v", err)))
		}
		if len(line) == 0 {
			continue
		}
		blp.LineCount++

		if line[0] == '#' {
			//parse positional data
			line = bytes.TrimSpace(line)
			blp.parsePositionData(line)
		} else {
			//parse event data

			//This is to accont for replicas where we seek to a point before the desired startPosition
			if blp.initialSeek && blp.globalState.usingRelayLogs && blp.nextStmtPosition < blp.startPosition.MasterPosition {
				continue
			}

			//readAhead is used for reading a log line that is spread across multiple lines and has newlines within it.
			if readAhead {
				event.LogLine = append(event.LogLine, line...)
			} else {
				event = NewEventBuffer(blp.currentPosition, line)
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

			//processes statements only for the dbname that it is subscribed to.
			blp.parseDbChange(event)
			blp.parseEventData(sendReply, event)
		}
	}
}

//Function to set the dbmatch variable, this parses the "Use <dbname>" statement.
func (blp *Blp) parseDbChange(event *eventBuffer) {
	if event.firstKw != USE {
		return
	}
	if blp.globalState.dbname == "" {
		relog.Warning("Dbname is not set, will match all database names")
		return
	}

	new_db := string(bytes.TrimSpace(bytes.SplitN(event.LogLine, BINLOG_DB_CHANGE, 2)[1]))
	if new_db != blp.globalState.dbname {
		blp.dbmatch = false
	} else {
		blp.dbmatch = true
	}
}

func (blp *Blp) parsePositionData(line []byte) {
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
}

func (blp *Blp) parseEventData(sendReply SendUpdateStreamResponse, event *eventBuffer) {
	if bytes.HasPrefix(event.LogLine, BINLOG_SET_TIMESTAMP) {
		blp.extractEventTimestamp(event)
		blp.initialSeek = false
		if blp.inTxn {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		}
	} else if bytes.HasPrefix(event.LogLine, BINLOG_SET_INSERT) {
		if blp.inTxn {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
		} else {
			panic(NewBinlogParseError(EVENT_ERROR, fmt.Sprintf("SET INSERT_ID outside a txn - len %v, dml '%v'", len(blp.txnLineBuffer), string(event.LogLine))))
		}
	} else if bytes.HasPrefix(event.LogLine, BINLOG_BEGIN) {
		blp.handleBeginEvent(event)
	} else if bytes.HasPrefix(event.LogLine, BINLOG_COMMIT) {
		blp.handleCommitEvent(sendReply, event)
		blp.inTxn = false
		blp.txnLineBuffer = blp.txnLineBuffer[0:0]
	} else if bytes.HasPrefix(event.LogLine, BINLOG_ROLLBACK) {
		blp.inTxn = false
		blp.txnLineBuffer = blp.txnLineBuffer[0:0]
	} else if len(event.LogLine) > 0 {
		sqlType := GetSqlType(event.firstKw)
		if blp.inTxn && IsTxnStatement(event.LogLine, event.firstKw) {
			blp.txnLineBuffer = append(blp.txnLineBuffer, event)
			blp.DmlCount++
		} else if sqlType == DDL {
			blp.handleDdlEvent(sendReply, event)
		} else {
			if sqlType == DML {
				panic(NewBinlogParseError(EVENT_ERROR, fmt.Sprintf("DML outside a txn - len %v, dml '%v'", len(blp.txnLineBuffer), string(event.LogLine))))
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

func (blp *Blp) parseMasterPosition(line []byte) {
	var err error
	rem := bytes.Split(line, BINLOG_END_LOG_POS)
	masterPosStr := string(bytes.Split(rem[1], SPACE)[0])
	blp.nextStmtPosition, err = strconv.ParseUint(masterPosStr, 10, 64)
	if err != nil {
		panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting master position %v", err)))
	}
}

func (blp *Blp) parseXid(line []byte) {
	rem := bytes.Split(line, BINLOG_XID)
	xid, err := strconv.ParseUint(string(rem[1]), 10, 64)
	if err != nil {
		panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting Xid position %v", err)))
	}
	blp.currentPosition.Xid = xid
}

func (blp *Blp) extractEventTimestamp(event *eventBuffer) {
	line := event.LogLine
	currentTimestamp, err := strconv.ParseInt(string(line[len(BINLOG_SET_TIMESTAMP):]), 10, 64)
	if err != nil {
		panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting timestamp %v", err)))
	}
	blp.currentPosition.Timestamp = currentTimestamp
	event.BinlogPosition.Timestamp = currentTimestamp
}

func (blp *Blp) parseRotateEvent(line []byte) {
	blp.RotateEventCount++
	rem := bytes.Split(line, BINLOG_ROTATE_TO)
	rem2 := bytes.Split(rem[1], POS)
	rotateFilename := strings.TrimSpace(string(rem2[0]))
	rotatePos, err := strconv.ParseUint(string(rem2[1]), 10, 64)
	if err != nil {
		panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting rotate pos %v from line %s", err, string(line))))
	}
	if !blp.globalState.usingRelayLogs {
		//If the file being parsed is a binlog,
		//then the rotate events only correspond to itself.
		blp.currentPosition.Position.MasterFilename = rotateFilename
		blp.currentPosition.Position.MasterPosition = rotatePos
	} else {
		//For relay logs, the rotate events could be that of relay log or the binlog,
		//the prefix of rotateFilename is used to test which case is it.
		logsDir, relayFile := path.Split(blp.currentPosition.Position.RelayFilename)
		currentPrefix := strings.Split(relayFile, ".")[0]
		rotatePrefix := strings.Split(rotateFilename, ".")[0]
		if currentPrefix == rotatePrefix {
			//relay log rotated
			blp.currentPosition.Position.RelayFilename = path.Join(logsDir, rotateFilename)
		} else {
			//master file rotated
			blp.currentPosition.Position.MasterFilename = rotateFilename
			blp.currentPosition.Position.MasterPosition = rotatePos
		}
	}
}

/*
Data event parsing and handling functions.
*/

func (blp *Blp) handleBeginEvent(event *eventBuffer) {
	if len(blp.txnLineBuffer) != 0 || blp.inTxn {
		panic(NewBinlogParseError(EVENT_ERROR, fmt.Sprintf("BEGIN encountered with non-empty trxn buffer, len: %d", len(blp.txnLineBuffer))))
	}
	blp.txnLineBuffer = blp.txnLineBuffer[:0]
	blp.inTxn = true
	blp.txnLineBuffer = append(blp.txnLineBuffer, event)
}

func (blp *Blp) handleDdlEvent(sendReply SendUpdateStreamResponse, event *eventBuffer) {
	ddlStream := createDdlStream(event)
	buf := []*UpdateResponse{ddlStream}
	if err := sendStream(sendReply, buf); err != nil {
		panic(NewBinlogParseError(CONNECTION_ERROR, fmt.Sprintf("Error in sending event to client %v", err)))
	}
	blp.DdlCount++
}

func (blp *Blp) handleCommitEvent(sendReply SendUpdateStreamResponse, commitEvent *eventBuffer) {
	if !blp.dbmatch {
		return
	}

	if blp.globalState.usingRelayLogs {
		for !blp.slavePosBehindReplication() {
			rp := &ReplicationPosition{MasterLogFile: blp.currentPosition.Position.MasterFilename,
				MasterLogPosition: uint(blp.currentPosition.Position.MasterPosition)}
			if err := blp.globalState.mysqld.WaitMasterPos(rp, 30); err != nil {
				panic(NewBinlogParseError(REPLICATION_ERROR, fmt.Sprintf("Error in waiting for replication to catch up, %v", err)))
			}
		}
	}
	commitEvent.BinlogPosition.Xid = blp.currentPosition.Xid
	blp.txnLineBuffer = append(blp.txnLineBuffer, commitEvent)
	//txn block for DMLs, parse it and send events for a txn
	var dmlCount uint
	blp.responseStream, dmlCount = buildTxnResponse(blp.txnLineBuffer)

	//No dml events - no point in sending this.
	if dmlCount == 0 {
		return
	}

	if err := sendStream(sendReply, blp.responseStream); err != nil {
		panic(NewBinlogParseError(CONNECTION_ERROR, fmt.Sprintf("Error in sending event to client %v", err)))
	}

	blp.TxnCount += 1
}

/*
Other utility functions.
*/

//This reads a binlog log line.
func (blp *Blp) readBlpLine(lineReader *bufio.Reader, bigLine []byte) (line []byte, err error) {
	for {
		tempLine, tempErr := lineReader.ReadSlice('\n')
		if tempErr == bufio.ErrBufferFull {
			blp.BufferFullErrors++
			bigLine = append(bigLine, tempLine...)
			continue
		} else if tempErr != nil {
			err = tempErr
		} else if len(bigLine) > 0 {
			if len(tempLine) > 0 {
				bigLine = append(bigLine, tempLine...)
			}
			line = bigLine[:len(bigLine)-1]
			blp.BigLineCount++
		} else {
			line = tempLine[:len(tempLine)-1]
		}
		break
	}
	return line, err
}

//This function streams the raw binlog.
func (blp *Blp) getBinlogStream(writer *os.File, blr *BinlogReader, readErrChan chan error) {
	defer func() {
		if err := recover(); err != nil {
			relog.Error("getBinlogStream failed: %v", err)
			readErrChan <- err.(error)
		}
	}()
	if blp.globalState.usingRelayLogs {
		//we use RelayPosition for initial seek in case the caller has precise coordinates. But the code
		//is designed to primarily use RelayFilename, MasterFilename and MasterPosition to correctly start
		//streaming the logs if relay logs are being used.
		blr.ServeData(writer, path.Base(blp.startPosition.RelayFilename), int64(blp.startPosition.RelayPosition))
	} else {
		blr.ServeData(writer, blp.startPosition.MasterFilename, int64(blp.startPosition.MasterPosition))
	}
	readErrChan <- nil
}

//This function determines whether streaming is behind replication as it should be.
func (blp *Blp) slavePosBehindReplication() bool {
	repl, err := blp.globalState.getReplicationPosition()
	if err != nil {
		relog.Error(err.Error())
		panic(NewBinlogParseError(REPLICATION_ERROR, fmt.Sprintf("Error in obtaining current replication position %v", err)))
	}
	if repl.MasterFilename == blp.currentPosition.Position.MasterFilename {
		if blp.currentPosition.Position.MasterPosition <= repl.MasterPosition {
			return true
		}
	} else {
		replExt, err := strconv.ParseUint(strings.Split(repl.MasterFilename, ".")[1], 10, 64)
		if err != nil {
			relog.Error(err.Error())
			panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting replication position %v", err)))
		}
		parseExt, err := strconv.ParseUint(strings.Split(blp.currentPosition.Position.MasterFilename, ".")[1], 10, 64)
		if err != nil {
			relog.Error(err.Error())
			panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting replication position %v", err)))
		}
		if replExt >= parseExt {
			return true
		}
	}
	return false
}

//This builds UpdateResponse for each transaction.
func buildTxnResponse(trxnLineBuffer []*eventBuffer) (txnResponseList []*UpdateResponse, dmlCount uint) {
	var err error
	var line []byte
	var dmlType string
	var eventNodeTree *parser.Node
	var autoincId uint64
	dmlBuffer := make([][]byte, 0, 10)

	for _, event := range trxnLineBuffer {
		line = event.LogLine
		if bytes.HasPrefix(line, BINLOG_BEGIN) {
			streamBuf := new(UpdateResponse)
			streamBuf.BinlogPosition = event.BinlogPosition
			streamBuf.SqlType = BEGIN
			txnResponseList = append(txnResponseList, streamBuf)
			continue
		}
		if bytes.HasPrefix(line, BINLOG_COMMIT) {
			commitEvent := createCommitEvent(event)
			txnResponseList = append(txnResponseList, commitEvent)
			continue
		}
		if bytes.HasPrefix(line, BINLOG_SET_INSERT) {
			autoincId, err = strconv.ParseUint(string(line[len(BINLOG_SET_INSERT):]), 10, 64)
			if err != nil {
				panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in extracting AutoInc Id %v", err)))
			}
			continue
		}

		sqlType := GetSqlType(event.firstKw)
		if sqlType == DML {
			//valid dml
			commentIndex := bytes.Index(line, STREAM_COMMENT_START)
			//stream comment not found.
			if commentIndex == -1 {
				if event.firstKw != "insert" {
					relog.Warning("Invalid DML - doesn't have a valid stream comment : %v", string(line))
				}
				dmlBuffer = dmlBuffer[:0]
				continue
				//FIXME: track such cases and potentially change them to errors at a later point.
				//panic(NewBinlogParseError(fmt.Sprintf("Invalid DML, doesn't have a valid stream comment. Sql: %v", string(line))))
			}
			dmlBuffer = append(dmlBuffer, line)
			streamComment := string(line[commentIndex+len(STREAM_COMMENT_START):])
			eventNodeTree = parseStreamComment(streamComment, autoincId)
			dmlType = GetDmlType(event.firstKw)
			response := createUpdateResponse(eventNodeTree, dmlType, event.BinlogPosition)
			txnResponseList = append(txnResponseList, response)
			autoincId = 0
			dmlBuffer = dmlBuffer[:0]
			dmlCount += 1
		} else {
			//add as prefixes to the DML from last DML.
			//start a new dml buffer and keep adding to it.
			dmlBuffer = append(dmlBuffer, line)
		}
	}

	return
}

/*
Functions that parse the stream comment.
The _stream comment is extracted into a tree node with the following structure.
EventNode.Sub[0] table name
EventNode.Sub[1] Pk column names
EventNode.Sub[2:] Pk Value lists
*/
// Example query: insert into vtocc_e(foo) values ('foo') /* _stream vtocc_e (eid id name ) (null 1 'bmFtZQ==' ); */
// the "null" value is used for auto-increment columns.

//This parese a paricular pk tuple.
func parsePkTuple(tokenizer *parser.Tokenizer, autoincIdPtr *uint64) (pkTuple *parser.Node) {
	//pkTuple is a list of pk value Nodes
	pkTuple = parser.NewSimpleParseNode(parser.NODE_LIST, "")
	autoincId := *autoincIdPtr
	//start scanning the list
	for tempNode := tokenizer.Scan(); tempNode.Type != ')'; tempNode = tokenizer.Scan() {
		switch tempNode.Type {
		case parser.NULL:
			pkTuple.Push(parser.NewParseNode(parser.NUMBER, []byte(strconv.FormatUint(autoincId, 10))))
			autoincId++
		case '-':
			//handle negative numbers
			t2 := tokenizer.Scan()
			if t2.Type != parser.NUMBER {
				panic(NewBinlogParseError(CODE_ERROR, "Illegal stream comment construct, - followed by a non-number"))
			}
			t2.Value = append(tempNode.Value, t2.Value...)
			pkTuple.Push(t2)
		case parser.ID, parser.NUMBER:
			pkTuple.Push(tempNode)
		case parser.STRING:
			b := tempNode.Value
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(b)))
			numDecoded, err := base64.StdEncoding.Decode(decoded, b)
			if err != nil {
				panic(NewBinlogParseError(CODE_ERROR, "Error in base64 decoding pkValue"))
			}
			tempNode.Value = decoded[:numDecoded]
			pkTuple.Push(tempNode)
		default:
			panic(NewBinlogParseError(EVENT_ERROR, fmt.Sprintf("Error in parsing stream comment: illegal node type %v %v", tempNode.Type, string(tempNode.Value))))
		}
	}
	return pkTuple
}

//This parses the stream comment.
func parseStreamComment(dmlComment string, autoincId uint64) (EventNode *parser.Node) {
	EventNode = parser.NewSimpleParseNode(parser.NODE_LIST, "")

	tokenizer := parser.NewStringTokenizer(dmlComment)
	var node *parser.Node
	var pkTuple *parser.Node

	node = tokenizer.Scan()
	if node.Type == parser.ID {
		//Table Name
		EventNode.Push(node)
	}

	for node = tokenizer.Scan(); node.Type != ';'; node = tokenizer.Scan() {
		switch node.Type {
		case '(':
			//pkTuple is a list of pk value Nodes
			pkTuple = parsePkTuple(tokenizer, &autoincId)
			EventNode.Push(pkTuple)
		default:
			panic(NewBinlogParseError(EVENT_ERROR, fmt.Sprintf("Error in parsing stream comment: illegal node type %v %v", node.Type, string(node.Value))))
		}
	}

	return EventNode
}

//This builds UpdateResponse from the parsed tree, also handles a multi-row update.
func createUpdateResponse(eventTree *parser.Node, dmlType string, blpPos BinlogPosition) (response *UpdateResponse) {
	if eventTree.Len() < 3 {
		panic(NewBinlogParseError(EVENT_ERROR, fmt.Sprintf("Invalid comment structure, len of tree %v", eventTree.Len())))
	}

	tableName := string(eventTree.At(0).Value)
	pkColNamesNode := eventTree.At(1)
	pkColNames := make([]string, 0, pkColNamesNode.Len())
	for _, pkCol := range pkColNamesNode.Sub {
		if pkCol.Type != parser.ID {
			panic(NewBinlogParseError(EVENT_ERROR, "Error in the stream comment, illegal type for column name."))
		}
		pkColNames = append(pkColNames, string(pkCol.Value))
	}
	pkColLen := pkColNamesNode.Len()

	response = new(UpdateResponse)
	response.BinlogPosition = blpPos
	response.SqlType = dmlType
	response.TableName = tableName
	response.PkColNames = pkColNames
	response.PkValues = make([][]interface{}, 0, len(eventTree.Sub[2:]))

	rowPk := make([]interface{}, pkColLen)
	for _, node := range eventTree.Sub[2:] {
		rowPk = rowPk[:0]
		if node.Len() != pkColLen {
			panic(NewBinlogParseError(EVENT_ERROR, "Error in the stream comment, length of pk values doesn't match column names."))
		}
		rowPk = encodePkValues(node.Sub)
		response.PkValues = append(response.PkValues, rowPk)
	}
	return response
}

//Interprets the parsed node and correctly encodes the primary key values.
func encodePkValues(pkValues []*parser.Node) (rowPk []interface{}) {
	for _, pkVal := range pkValues {
		if pkVal.Type == parser.STRING {
			rowPk = append(rowPk, string(pkVal.Value))
		} else if pkVal.Type == parser.NUMBER {
			//pkVal.Value is a byte array, convert this to string and use strconv to find
			//the right numberic type.
			valstr := string(pkVal.Value)
			if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
				rowPk = append(rowPk, ival)
			} else if uval, err := strconv.ParseUint(valstr, 0, 64); err == nil {
				rowPk = append(rowPk, uval)
			} else if fval, err := strconv.ParseFloat(valstr, 64); err == nil {
				rowPk = append(rowPk, fval)
			} else {
				panic(NewBinlogParseError(CODE_ERROR, fmt.Sprintf("Error in encoding pkValues %v", err)))
			}
		} else {
			panic(NewBinlogParseError(CODE_ERROR, "Error in encoding pkValues - Unsupported type"))
		}
	}
	return rowPk
}

//This sends the stream to the client.
func sendStream(sendReply SendUpdateStreamResponse, responseBuf []*UpdateResponse) (err error) {
	for _, event := range responseBuf {
		err = sendReply(event)
		if err != nil {
			return NewBinlogParseError(CONNECTION_ERROR, fmt.Sprintf("Error in sending reply to client, %v", err))
		}
	}
	return nil
}

//This sends the error to the client.
func SendError(sendReply SendUpdateStreamResponse, inputErr error, blpPos *BinlogPosition) {
	streamBuf := new(UpdateResponse)
	streamBuf.Error = inputErr.Error()
	if blpPos != nil {
		streamBuf.BinlogPosition = *blpPos
	}
	buf := []*UpdateResponse{streamBuf}
	_ = sendStream(sendReply, buf)
}

//This creates the response for COMMIT event.
func createCommitEvent(eventBuf *eventBuffer) (streamBuf *UpdateResponse) {
	streamBuf = new(UpdateResponse)
	streamBuf.BinlogPosition = eventBuf.BinlogPosition
	streamBuf.SqlType = COMMIT
	return
}

//This creates the response for DDL event.
func createDdlStream(lineBuffer *eventBuffer) (ddlStream *UpdateResponse) {
	ddlStream = new(UpdateResponse)
	ddlStream.BinlogPosition = lineBuffer.BinlogPosition
	ddlStream.SqlType = DDL
	ddlStream.Sql = string(lineBuffer.LogLine)
	return ddlStream
}

func GetSqlType(firstKw string) string {
	sqlType, _ := SqlKwMap[firstKw]
	return sqlType
}

func GetDmlType(firstKw string) string {
	sqlType, ok := SqlKwMap[firstKw]
	if ok && sqlType == DML {
		return firstKw
	}
	return ""
}

func IgnoredStatement(line []byte) bool {
	if bytes.HasPrefix(line, COMMENT) || bytes.HasPrefix(line, SET_SESSION_VAR) ||
		bytes.HasPrefix(line, DELIMITER) || bytes.HasPrefix(line, BINLOG) || bytes.HasPrefix(line, BINLOG_DB_CHANGE) {
		return true
	}
	return false
}

func IsTxnStatement(line []byte, firstKw string) bool {
	if GetSqlType(firstKw) == DML ||
		bytes.HasPrefix(line, BINLOG_SET_TIMESTAMP) ||
		bytes.HasPrefix(line, BINLOG_SET_INSERT) {
		return true
	}
	return false
}
