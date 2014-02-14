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
)

var (
	// posRE is for extracting server id, end_log_pos and group_id.
	posRE = regexp.MustCompile(`^#.*server id ([0-9]+).*end_log_pos ([0-9]+).*group_id ([0-9]+)`)

	// rotateRE is for extracting log rotation info.
	rotateRE = regexp.MustCompile(`^#.*Rotate to ([^ \t]+).*pos: ([0-9]+)`)

	// delimRE is for extracting the delimeter.
	delimRE = regexp.MustCompile(`^DELIMITER[ \t]+(.*;)`)

	// ignorePrefixes defines the prefixes that can be ignored.
	ignorePrefixes = [][]byte{
		[]byte("#"),
		[]byte("use"),
		[]byte("USE"),
		[]byte("/*"),
		[]byte("SET @@session"),
		[]byte("BINLOG"),
	}

	// statementPrefixes are normal sql statement prefixes.
	statementPrefixes = map[string]int{
		"begin":    proto.BL_BEGIN,
		"commit":   proto.BL_COMMIT,
		"rollback": proto.BL_ROLLBACK,
		"insert":   proto.BL_DML,
		"update":   proto.BL_DML,
		"delete":   proto.BL_DML,
		"create":   proto.BL_DDL,
		"alter":    proto.BL_DDL,
		"drop":     proto.BL_DDL,
		"truncate": proto.BL_DDL,
		"rename":   proto.BL_DDL,
		"set":      proto.BL_SET,
	}

	// Misc vars.
	HASH_COMMENT  = []byte("#")
	SLASH_COMMENT = []byte("/*")
	DELIM_STMT    = []byte("DELIMITER")
	DEFAULT_DELIM = []byte(";")
)

type binlogPosition struct {
	GroupId, ServerId int64
}

// BinlogStreamer streamer streams binlog events grouped
// by transactions.
type BinlogStreamer struct {
	// dbname & dir are set at creation.
	dbname string
	dir    string

	svm sync2.ServiceManager

	// file, blPos & delim are updated during streaming.
	file  fileInfo
	blPos binlogPosition
	delim []byte
}

// sendTransactionFunc is used to send binlog events.
// reply is of type proto.BinlogTransaction.
type sendTransactionFunc func(trans *proto.BinlogTransaction) error

// NewBinlogStreamer creates a BinlogStreamer. dbname specifes
// the db to stream events for, and binlogPrefix is as defined
// by the mycnf variable.
func NewBinlogStreamer(dbname, binlogPrefix string) *BinlogStreamer {
	return &BinlogStreamer{
		dbname: dbname,
		dir:    path.Dir(binlogPrefix),
	}
}

// Stream starts streaming binlog events from file & pos by repeatedly calling sendTransaction.
func (bls *BinlogStreamer) Stream(file string, pos int64, sendTransaction sendTransactionFunc) (err error) {
	if err = bls.file.Init(path.Join(bls.dir, file), pos); err != nil {
		return err
	}
	defer bls.file.Close()

	// Launch using service manager so we can stop this
	// as needed.
	bls.svm.Go(func(_ *sync2.ServiceManager) {
		for {
			if err = bls.run(sendTransaction); err != nil {
				return
			}
			if err = bls.file.WaitForChange(&bls.svm); err != nil {
				return
			}
		}
	})

	// Wait for service to exit, and handle errors if any.
	bls.svm.Wait()
	if err == io.EOF {
		log.Infof("Stream ended @ %#v", bls.file)
		return nil
	}
	log.Errorf("Stream error @ %#v, error: %v", bls.file, err)
	return fmt.Errorf("stream error @ %#v, error: %v", bls.file, err)
}

// Stop stops the currently executing Stream if there is one.
func (bls *BinlogStreamer) Stop() {
	bls.svm.Stop()
}

// run launches mysqlbinlog and starts the stream. It takes care of
// cleaning up the process when streaming returns.
func (bls *BinlogStreamer) run(sendTransaction sendTransactionFunc) (err error) {
	mbl := &MysqlBinlog{}
	reader, err := mbl.Launch(bls.dbname, bls.file.name, bls.file.pos)
	if err != nil {
		return fmt.Errorf("launch error: %v", err)
	}
	defer reader.Close()
	err = bls.parseEvents(sendTransaction, reader)
	if err != nil {
		mbl.Kill()
	} else {
		err = mbl.Wait()
		if err != nil {
			err = fmt.Errorf("wait error: %v", err)
		}
	}
	return err
}

// parseEvents parses events and transmits them as transactions for the current mysqlbinlog stream.
func (bls *BinlogStreamer) parseEvents(sendTransaction sendTransactionFunc, reader io.Reader) (err error) {
	bls.delim = DEFAULT_DELIM
	bufReader := bufio.NewReader(reader)
	var statements []proto.Statement
	for {
		sql, err := bls.nextStatement(bufReader)
		if sql == nil {
			return err
		}
		prefix := string(bytes.ToLower(bytes.SplitN(sql, SPACE, 2)[0]))
		switch category := statementPrefixes[prefix]; category {
		// We trust that mysqlbinlog doesn't send proto.BL_DMLs withot a proto.BL_BEGIN
		case proto.BL_BEGIN, proto.BL_ROLLBACK:
			statements = nil
		case proto.BL_DDL:
			statements = append(statements, proto.Statement{Category: category, Sql: sql})
			fallthrough
		case proto.BL_COMMIT:
			trans := &proto.BinlogTransaction{
				Statements: statements,
				GroupId:    bls.blPos.GroupId,
			}
			if err = sendTransaction(trans); err != nil {
				if err == io.EOF {
					return err
				}
				return fmt.Errorf("send reply error: %v", err)
			}
			statements = nil
		// proto.BL_DML, proto.BL_SET or proto.BL_UNRECOGNIZED
		default:
			statements = append(statements, proto.Statement{Category: category, Sql: sql})
		}
	}
}

// nextStatement returns the next statement encountered in the binlog stream. If there are
// positional comments, it updates the BinlogStreamer state. It also ignores events that
// are not material. If it returns nil, it's the end of stream. If err is also nil, then
// it was due to a normal termination.
func (bls *BinlogStreamer) nextStatement(bufReader *bufio.Reader) (stmt []byte, err error) {
eventLoop:
	for {
		// Stop processing if we're shutting down
		if bls.svm.State() != sync2.SERVICE_RUNNING {
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
			bls.blPos.GroupId = mustParseInt64(values[3])
			continue
		}
		values = rotateRE.FindSubmatch(event)
		if values != nil {
			err = bls.file.Rotate(path.Join(bls.dir, string(values[1])), mustParseInt64(values[2]))
			if err != nil {
				return nil, err
			}
			continue
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
func (bls *BinlogStreamer) readEvent(bufReader *bufio.Reader) (event []byte, err error) {
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
	name   string
	pos    int64
	handle *os.File
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
	f.pos = pos
	f.handle, err = os.Open(name)
	if err != nil {
		return fmt.Errorf("open error: %v", err)
	}
	return nil
}

func (f *fileInfo) Set(pos int64) {
	f.pos = pos
}

func (f *fileInfo) WaitForChange(svm *sync2.ServiceManager) error {
	for {
		// Stop waiting if we're shutting down
		if svm.State() != sync2.SERVICE_RUNNING {
			return io.EOF
		}
		time.Sleep(100 * time.Millisecond)
		fi, err := f.handle.Stat()
		if err != nil {
			return fmt.Errorf("stat error: %v", err)
		}
		if fi.Size() != f.pos {
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
	return fmt.Errorf("close error: %v", err)
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
