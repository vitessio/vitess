// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

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
)

// Valid statement types in the binlogs.
const (
	BL_UNRECOGNIZED = iota
	BL_BEGIN
	BL_COMMIT
	BL_ROLLBACK
	BL_DML
	BL_DDL
	BL_SET
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
		"begin":    BL_BEGIN,
		"commit":   BL_COMMIT,
		"rollback": BL_ROLLBACK,
		"insert":   BL_DML,
		"update":   BL_DML,
		"delete":   BL_DML,
		"create":   BL_DDL,
		"alter":    BL_DDL,
		"drop":     BL_DDL,
		"truncate": BL_DDL,
		"rename":   BL_DDL,
		"set":      BL_SET,
	}

	// Misc vars.
	HASH_COMMENT  = []byte("#")
	SLASH_COMMENT = []byte("/*")
	DELIM_STMT    = []byte("DELIMITER")
	DEFAULT_DELIM = []byte(";")
)

// TODO: Move to proto once finalized
type BinlogTransaction struct {
	Statements []Statement
	GroupId    string
}

type Statement struct {
	Category int
	Sql      []byte
}

type binlogPosition struct {
	GroupId, ServerId int64
}

func (blp *binlogPosition) String() string {
	return fmt.Sprintf("%d:%d", blp.GroupId, blp.ServerId)
}

// BinlogStreamer streamer streams binlog events grouped
// by transactions.
type BinlogStreamer struct {
	// dbname & dir are set at creation.
	dbname string
	dir    string

	// running is set when streaming begins.
	running sync2.AtomicInt32

	// file, blPos & delim are updated during streaming.
	file  fileInfo
	blPos binlogPosition
	delim []byte
}

// sendTransactionFunc is used to send binlog events.
// reply is of type BinlogTransaction.
type sendTransactionFunc func(trans *BinlogTransaction) error

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
	if !bls.running.CompareAndSwap(0, 1) {
		return fmt.Errorf("already streaming or stopped.")
	}
	if err = bls.file.Init(path.Join(bls.dir, file), pos); err != nil {
		return err
	}
	defer func() {
		bls.file.Close()
		bls.Stop()
	}()

	for {
		if err = bls.run(sendTransaction); err != nil {
			goto end
		}
		if err = bls.file.WaitForChange(&bls.running); err != nil {
			goto end
		}
	}

end:
	if err == io.EOF {
		return nil
	}
	log.Errorf("Stream error @ %#v, error: %v", bls.file, err)
	return err
}

// Stop stops the currently executing Stream if there is one.
// You cannot resume with the current BinlogStreamer after you've stopped.
func (bls *BinlogStreamer) Stop() {
	bls.running.Set(-1)
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
	var statements []Statement
	for {
		sql, err := bls.nextStatement(bufReader)
		if sql == nil {
			return err
		}
		prefix := string(bytes.ToLower(bytes.SplitN(sql, SPACE, 2)[0]))
		switch category := statementPrefixes[prefix]; category {
		case BL_UNRECOGNIZED:
			return fmt.Errorf("unrecognized: %s", sql)
		// We trust that mysqlbinlog doesn't send BL_DMLs withot a BL_BEGIN
		case BL_BEGIN, BL_ROLLBACK:
			statements = nil
		case BL_DDL:
			statements = append(statements, Statement{Category: category, Sql: sql})
			fallthrough
		case BL_COMMIT:
			trans := &BinlogTransaction{
				Statements: statements,
				GroupId:    strconv.Itoa(int(bls.blPos.GroupId)),
			}
			if err = sendTransaction(trans); err != nil {
				if err == io.EOF {
					return err
				}
				return fmt.Errorf("send reply error: %v", err)
			}
			statements = nil
		// BL_DML & BL_SET
		default:
			statements = append(statements, Statement{Category: category, Sql: sql})
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
		if bls.running.Get() != 1 {
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

func (f *fileInfo) WaitForChange(running *sync2.AtomicInt32) error {
	for {
		if running.Get() != 1 {
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
