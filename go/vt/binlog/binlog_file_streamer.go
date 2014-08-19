// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var (
	// posRE is for extracting server id, end_log_pos and group_id.
	posRE = regexp.MustCompile(`^#.*server id ([0-9]+).*end_log_pos ([0-9]+).*group_id ([0-9]+)`)

	// rotateRE is for extracting log rotation info.
	rotateRE = regexp.MustCompile(`^#.*Rotate to ([^ \t]+).*pos: ([0-9]+)`)

	// delimRE is for extracting the delimeter.
	delimRE = regexp.MustCompile(`^DELIMITER[ \t]+(.*;)`)

	// eolfPrefix is for detecting the end of log file marker.
	eolfPrefix = []byte(`# End of log file`)

	// ignorePrefixes defines the prefixes that can be ignored.
	ignorePrefixes = [][]byte{
		[]byte("#"),
		[]byte("use"),
		[]byte("USE"),
		[]byte("/*"),
		[]byte("SET @@session"),
		[]byte("BINLOG"),
	}

	// Misc vars.
	HASH_COMMENT  = []byte("#")
	SLASH_COMMENT = []byte("/*")
	DELIM_STMT    = []byte("DELIMITER")
	DEFAULT_DELIM = []byte(";")
)

// binlogFileStreamer only supports Google MySQL. Support for other flavors will
// come with the switch to a connection-based streamer.
const blsMysqlFlavor = "GoogleMysql"

type binlogPosition struct {
	GTID     myproto.GTID
	ServerId int64
}

// binlogFileStreamer streams binlog events from a set of local files.
type binlogFileStreamer struct {
	// dbname, dir, and mysqld are set at creation.
	dbname string
	dir    string
	mysqld *mysqlctl.Mysqld

	svm sync2.ServiceManager

	// file, blPos & delim are updated during streaming.
	file  fileInfo
	blPos binlogPosition
	delim []byte
}

// newBinlogFileStreamer creates a BinlogStreamer.
//
// dbname specifes the db to stream events for.
func newBinlogFileStreamer(dbname string, mysqld *mysqlctl.Mysqld) BinlogStreamer {
	return &binlogFileStreamer{
		dbname: dbname,
		dir:    path.Dir(mysqld.Cnf().BinLogPath),
		mysqld: mysqld,
	}
}

// streamFilePos starts streaming events from a given file and position.
func (bls *binlogFileStreamer) streamFilePos(file string, pos int64, sendTransaction sendTransactionFunc) error {
	if err := bls.file.Init(path.Join(bls.dir, file), pos); err != nil {
		return err
	}
	defer bls.file.Close()

	// Launch using service manager so we can stop this as needed.
	bls.svm.Go(func(svc *sync2.ServiceContext) error {
		for {
			if err := bls.run(svc, sendTransaction); err != nil {
				return err
			}
			if err := bls.file.WaitForChange(svc); err != nil {
				return err
			}
		}
	})

	// Wait for service to exit, and handle errors if any.
	err := bls.svm.Join()
	if err == io.EOF {
		log.Infof("Stream ended @ %#v", bls.file)
		return nil
	}
	log.Errorf("Stream error @ %#v, error: %v", bls.file, err)
	return fmt.Errorf("stream error @ %#v, error: %v", bls.file, err)
}

// Stream implements BinlogStreamer.Stream().
func (bls *binlogFileStreamer) Stream(gtid myproto.GTID, sendTransaction sendTransactionFunc) error {
	// Query mysqld to convert GTID to file & pos.
	rp, err := bls.mysqld.BinlogInfo(gtid)
	if err != nil {
		log.Errorf("Unable to serve client request: error computing start position: %v", err)
		return fmt.Errorf("error computing start position: %v", err)
	}
	return bls.streamFilePos(rp.MasterLogFile, int64(rp.MasterLogPosition), sendTransaction)
}

// Stop implements BinlogStreamer.Stop().
func (bls *binlogFileStreamer) Stop() {
	bls.svm.Stop()
}

// run launches mysqlbinlog and starts the stream. It takes care of
// cleaning up the process when streaming returns.
func (bls *binlogFileStreamer) run(svc *sync2.ServiceContext, sendTransaction sendTransactionFunc) (err error) {
	mbl := &MysqlBinlog{}
	reader, err := mbl.Launch(bls.dbname, bls.file.name, bls.file.pos)
	if err != nil {
		return fmt.Errorf("launch error: %v", err)
	}
	defer reader.Close()
	err = bls.parseEvents(svc, sendTransaction, reader)
	// Always kill because we don't read from reader all the way to EOF.
	// If we wait, we may deadlock.
	mbl.Kill()
	return err
}

// parseEvents parses events and transmits them as transactions for the current mysqlbinlog stream.
func (bls *binlogFileStreamer) parseEvents(svc *sync2.ServiceContext, sendTransaction sendTransactionFunc, reader io.Reader) (err error) {
	bls.delim = DEFAULT_DELIM
	bufReader := bufio.NewReader(reader)
	var statements []proto.Statement
	var timestamp int64
	for {
		sql, err := bls.nextStatement(svc, bufReader)
		if sql == nil {
			return err
		}
		switch category := getStatementCategory(sql); category {
		// We trust that mysqlbinlog doesn't send proto.BL_DMLs without a proto.BL_BEGIN.
		case proto.BL_BEGIN:
			statements = nil
			timestamp = 0
		case proto.BL_ROLLBACK:
			bls.file.Save()
			statements = nil
			timestamp = 0
		case proto.BL_DDL:
			statements = append(statements, proto.Statement{Category: category, Sql: sql})
			fallthrough
		case proto.BL_COMMIT:
			trans := &proto.BinlogTransaction{
				Statements: statements,
				Timestamp:  timestamp,
				GTIDField:  myproto.GTIDField{Value: bls.blPos.GTID},
			}
			if err = sendTransaction(trans); err != nil {
				if err == io.EOF {
					return err
				}
				return fmt.Errorf("send reply error: %v", err)
			}
			bls.file.Save()
			statements = nil
			timestamp = 0
		case proto.BL_SET:
			if statements == nil && bytes.HasPrefix(sql, BINLOG_SET_TIMESTAMP) {
				// get the timestamp
				timestamp, _ = strconv.ParseInt(string(sql[BINLOG_SET_TIMESTAMP_LEN:]), 10, 64)
			}
			statements = append(statements, proto.Statement{Category: category, Sql: sql})
		// proto.BL_DML or proto.BL_UNRECOGNIZED
		default:
			statements = append(statements, proto.Statement{Category: category, Sql: sql})
		}
	}
}

// nextStatement returns the next statement encountered in the binlog stream. If there are
// positional comments, it updates the binlogFileStreamer state. It also ignores events that
// are not material. If it returns nil, it's the end of stream. If err is also nil, then
// it was due to a normal termination.
func (bls *binlogFileStreamer) nextStatement(svc *sync2.ServiceContext, bufReader *bufio.Reader) (stmt []byte, err error) {
eventLoop:
	for {
		// Stop processing if we're shutting down
		if !svc.IsRunning() {
			return nil, io.EOF
		}
		event, err := bls.readEvent(bufReader)
		if err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
		values := posRE.FindSubmatch(event)
		if values != nil {
			bls.blPos.ServerId = mustParseInt64(values[1])
			bls.file.Set(mustParseInt64(values[2]))
			bls.blPos.GTID = myproto.MustParseGTID(blsMysqlFlavor, string(values[3]))
			continue
		}
		values = rotateRE.FindSubmatch(event)
		if values != nil {
			err = bls.file.Rotate(path.Join(bls.dir, string(values[1])), mustParseInt64(values[2]))
			if err != nil {
				return nil, err
			}
			return nil, nil
		}
		if bytes.HasPrefix(event, eolfPrefix) {
			return nil, nil
		}
		values = delimRE.FindSubmatch(event)
		if values != nil {
			bls.delim = values[1]
			continue
		}
		for _, ignorePrefix := range ignorePrefixes {
			if bytes.HasPrefix(event, ignorePrefix) {
				continue eventLoop
			}
		}
		return event, nil
	}
}

// readEvent reads a single binlog event. It can be a single comment line,
// or multiple lines terminated by the delimiter.
func (bls *binlogFileStreamer) readEvent(bufReader *bufio.Reader) (event []byte, err error) {
	for {
		fragment, err := bufReader.ReadSlice('\n')
		event = append(event, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if err == io.EOF {
				return event, err
			}
			return event, fmt.Errorf("read error: %v", err)
		}
		// Use a different var than event, because you have to keep
		// the trailing \n if we continue
		trimmed := bytes.TrimSpace(event)
		if bytes.HasPrefix(trimmed, HASH_COMMENT) ||
			bytes.HasPrefix(trimmed, SLASH_COMMENT) ||
			bytes.HasPrefix(trimmed, DELIM_STMT) {
			return trimmed, nil
		}
		if bytes.HasSuffix(trimmed, bls.delim) {
			return bytes.TrimSpace(trimmed[:len(trimmed)-len(bls.delim)]), nil
		}
	}
}

// fileInfo is used to track the current binlog file and position.
type fileInfo struct {
	name string
	// pos is the position to be used if mysqlbinlog should be restarted.
	pos int64
	// lastPos is the last position seen by mysqlbinlog. This is different
	// from pos because the corresponding event may not get successfully read.
	// If we restart mysqlbinlog without successfully reading a full transaction,
	// we have to start from the beginning, which will be pos. lastPos is saved
	// into pos when we see a commit or rollback. In case of a rotate, pos is
	// set to the new value and lastPos is reset.
	lastPos int64
	handle  *os.File
}

func (f *fileInfo) Init(name string, pos int64) error {
	err := f.Rotate(name, pos)
	if err != nil {
		return err
	}
	// Make sure the current file hasn't rotated.
	next := nextFileName(name)
	fi, _ := os.Stat(next)
	if fi == nil {
		// Assume next file doesn't exist
		return nil
	}

	// Next file exists. Check if current file size matches position
	fi, err = f.handle.Stat()
	if err != nil {
		return err
	}
	if fi.Size() <= pos {
		// The file has rotated
		return f.Rotate(next, 4)
	}
	return nil
}

func (f *fileInfo) Rotate(name string, pos int64) (err error) {
	if f.handle != nil {
		f.handle.Close()
	}
	f.name = name
	f.pos, f.lastPos = pos, 0
	f.handle, err = os.Open(name)
	// If file doesn't exist, wait indefinitely for it to be created.
	for os.IsNotExist(err) {
		// Log the event so we know it's happening.
		log.Infof("Waiting for file %s to be created, retrying in 1 second.", name)
		time.Sleep(1 * time.Second)
		f.handle, err = os.Open(name)
	}
	return err
}

func (f *fileInfo) Set(pos int64) {
	f.lastPos = pos
}

func (f *fileInfo) Save() {
	if f.lastPos != 0 {
		f.pos, f.lastPos = f.lastPos, 0
	}
}

func (f *fileInfo) WaitForChange(svc *sync2.ServiceContext) error {
	for {
		// Stop waiting if we're shutting down
		if !svc.IsRunning() {
			return io.EOF
		}
		time.Sleep(100 * time.Millisecond)
		fi, err := f.handle.Stat()
		if err != nil {
			return fmt.Errorf("stat error: %v", err)
		}
		if fi.Size() != f.lastPos {
			return nil
		}
	}
}

func (f *fileInfo) Close() (err error) {
	if f.handle == nil {
		return nil
	}
	err = f.handle.Close()
	f.handle = nil
	if err != nil {
		return fmt.Errorf("close error: %v", err)
	}
	return nil
}

func nextFileName(name string) string {
	newname := []byte(name)
	index := len(newname) - 1
	for newname[index] == '9' && index > 0 {
		newname[index] = '0'
		index--
	}
	newname[index] += 1
	return string(newname)
}

// mustParseInt64 can be used if you don't expect to fail.
func mustParseInt64(b []byte) int64 {
	val, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		panic(err)
	}
	return val
}
