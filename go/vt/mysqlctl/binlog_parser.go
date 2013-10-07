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
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	vtenv "github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	parser "github.com/youtube/vitess/go/vt/sqlparser"
)

/*
Functionality to parse a binlog log and send event streams to clients.
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

//Api Interface
type UpdateResponse struct {
	Error string
	Coord proto.BinlogPosition
	Data  EventData
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
	Coord   proto.BinlogPosition
	LogLine []byte
	firstKw string
}

func NewEventBuffer(pos *proto.BinlogPosition, line []byte) *eventBuffer {
	buf := &eventBuffer{}
	buf.LogLine = make([]byte, len(line))
	written := copy(buf.LogLine, line)
	if written < len(line) {
		log.Warningf("Logline not properly copied while creating new event written: %v len: %v", written, len(line))
	}
	buf.Coord.Position = pos.Position
	buf.Coord.Timestamp = pos.Timestamp
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
	startPosition    *proto.ReplicationCoordinates
	currentPosition  *proto.BinlogPosition
	globalState      *UpdateStream
	dbmatch          bool
	blpStats
}

func NewBlp(startCoordinates *proto.ReplicationCoordinates, updateStream *UpdateStream) *Blp {
	blp := &Blp{}
	blp.startPosition = startCoordinates
	blp.currentPosition = &proto.BinlogPosition{Position: *startCoordinates}
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

func (blp *Blp) ComputeBacklog() int64 {
	rp, replErr := blp.globalState.getReplicationPosition()
	if replErr != nil {
		return -1
	}
	pos := int64(blp.currentPosition.Position.MasterPosition)
	if rp.MasterFilename != blp.currentPosition.Position.MasterFilename {
		pos = 0
	}
	return int64(rp.MasterPosition) - pos
}

//Main entry function for reading and parsing the binlog.
func (blp *Blp) StreamBinlog(sendReply SendUpdateStreamResponse, binlogPrefix string) {
	var err error
	var lastRun time.Time
	count := 0
	for {
		count++
		lastRun = time.Now()
		err = blp.streamBinlog(sendReply, binlogPrefix)
		if err != nil {
			log.Errorf("StreamBinlog error @ %v, error: %v", blp.currentPosition.String(), err.Error())
			SendError(sendReply, err, blp.currentPosition)
			return
		}
		diff := time.Now().Sub(lastRun)
		if diff < (100 * time.Millisecond) {
			time.Sleep(100*time.Millisecond - diff)
		}
		*blp.startPosition = blp.currentPosition.Position
	}
}

func (blp *Blp) handleError(err *error) {
	reqIdentifier := blp.currentPosition.String()
	if x := recover(); x != nil {
		serr, ok := x.(*BinlogParseError)
		if !ok {
			log.Errorf("Uncaught panic for stream @ %v", reqIdentifier)
			panic(x)
		}
		*err = NewBinlogParseError(serr.errType, serr.msg)
	}
}

func (blp *Blp) streamBinlog(sendReply SendUpdateStreamResponse, binlogPrefix string) (err error) {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return err
	}
	cmd := exec.Command(
		path.Join(dir, "bin/mysqlbinlog"),
		fmt.Sprintf("--start-position=%d", blp.startPosition.MasterPosition),
		path.Join(path.Dir(binlogPrefix), blp.startPosition.MasterFilename),
	)
	binlogReader, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	err = cmd.Start()
	if err != nil {
		return err
	}
	err = blp.parseBinlogEvents(sendReply, binlogReader)
	if err != nil {
		cmd.Process.Kill()
	}
	return err
}

//Main parse loop
func (blp *Blp) parseBinlogEvents(sendReply SendUpdateStreamResponse, binlogReader io.Reader) (err error) {
	defer blp.handleError(&err)
	// read over the stream and buffer up the transactions
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
				return nil
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
	return nil
}

//Function to set the dbmatch variable, this parses the "Use <dbname>" statement.
func (blp *Blp) parseDbChange(event *eventBuffer) {
	if event.firstKw != USE {
		return
	}
	if blp.globalState.dbname == "" {
		log.Warningf("dbname is not set, will match all database names")
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
				log.Warningf("Unknown statement '%v'", string(event.LogLine))
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
	event.Coord.Timestamp = currentTimestamp
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

	//If the file being parsed is a binlog,
	//then the rotate events only correspond to itself.
	blp.currentPosition.Position.MasterFilename = rotateFilename
	blp.currentPosition.Position.MasterPosition = rotatePos
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

	commitEvent.Coord.Xid = blp.currentPosition.Xid
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
			log.Errorf("getBinlogStream failed: %v", err)
			readErrChan <- err.(error)
		}
	}()
	blr.ServeData(writer, blp.startPosition.MasterFilename, int64(blp.startPosition.MasterPosition))
	readErrChan <- nil
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
			streamBuf.Coord = event.Coord
			streamBuf.Data.SqlType = BEGIN
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
					log.Warningf("Invalid DML - doesn't have a valid stream comment : %v", string(line))
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
			response := createUpdateResponse(eventNodeTree, dmlType, event.Coord)
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
func createUpdateResponse(eventTree *parser.Node, dmlType string, blpPos proto.BinlogPosition) (response *UpdateResponse) {
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
	response.Coord = blpPos
	response.Data.SqlType = dmlType
	response.Data.TableName = tableName
	response.Data.PkColNames = pkColNames
	response.Data.PkValues = make([][]interface{}, 0, len(eventTree.Sub[2:]))

	rowPk := make([]interface{}, pkColLen)
	for _, node := range eventTree.Sub[2:] {
		rowPk = rowPk[:0]
		if node.Len() != pkColLen {
			panic(NewBinlogParseError(EVENT_ERROR, "Error in the stream comment, length of pk values doesn't match column names."))
		}
		rowPk = encodePkValues(node.Sub)
		response.Data.PkValues = append(response.Data.PkValues, rowPk)
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
func SendError(sendReply SendUpdateStreamResponse, inputErr error, blpPos *proto.BinlogPosition) {
	streamBuf := new(UpdateResponse)
	streamBuf.Error = inputErr.Error()
	if blpPos != nil {
		streamBuf.Coord = *blpPos
	}
	buf := []*UpdateResponse{streamBuf}
	_ = sendStream(sendReply, buf)
}

//This creates the response for COMMIT event.
func createCommitEvent(eventBuf *eventBuffer) (streamBuf *UpdateResponse) {
	streamBuf = new(UpdateResponse)
	streamBuf.Coord = eventBuf.Coord
	streamBuf.Data.SqlType = COMMIT
	return
}

//This creates the response for DDL event.
func createDdlStream(lineBuffer *eventBuffer) (ddlStream *UpdateResponse) {
	ddlStream = new(UpdateResponse)
	ddlStream.Coord = lineBuffer.Coord
	ddlStream.Data.SqlType = DDL
	ddlStream.Data.Sql = string(lineBuffer.LogLine)
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
	if bytes.HasPrefix(line, COMMENT) ||
		bytes.HasPrefix(line, SET_SESSION_VAR) ||
		bytes.HasPrefix(line, DELIMITER) ||
		bytes.HasPrefix(line, BINLOG) ||
		bytes.HasPrefix(line, BINLOG_DB_CHANGE) {
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
