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
// TODO(sougou): Drop BL prefix once binlog_parser is deleted.
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
	Statements [][]byte
	Position   BinlogPosition
}

type BinlogPosition struct {
	GroupId, ServerId int64
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
	blPos BinlogPosition
	delim []byte
}

// SendReplyFunc is used to send binlog events.
// reply is of type BinlogTransaction.
type SendReplyFunc func(reply interface{}) error

// NewBinlogStreamer creates a BinlogStreamer. dbname specifes
// the db to stream events for, and binlogPrefix is as defined
// by the mycnf variable.
func NewBinlogStreamer(dbname, binlogPrefix string) *BinlogStreamer {
	return &BinlogStreamer{
		dbname: dbname,
		dir:    path.Dir(binlogPrefix),
	}
}

// Stream starts streaming binlog events from file & pos by repeatedly calling sendReply.
func (bls *BinlogStreamer) Stream(file string, pos int64, sendReply SendReplyFunc) (err error) {
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
		err = bls.run(sendReply)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Errorf("Stream error @ %#v, error: %v", bls, err)
			return err
		}
		if err = bls.file.WaitForChange(); err != nil {
			return err
		}
	}
}

// Stop stops the currently executing Stream if there is one.
// You cannot resume with the current BinlogStreamer after you've stopped.
func (bls *BinlogStreamer) Stop() {
	bls.running.Set(-1)
}

func (bls *BinlogStreamer) run(sendReply SendReplyFunc) (err error) {
	mbl := &MysqlBinlog{}
	reader, err := mbl.Launch(bls.dbname, bls.file.name, bls.file.pos)
	if err != nil {
		return err
	}
	defer reader.Close()
	err = bls.parseEvents(sendReply, reader)
	if err != nil {
		mbl.Kill()
	} else {
		err = mbl.Wait()
	}
	return err
}

func (bls *BinlogStreamer) parseEvents(sendReply SendReplyFunc, reader io.Reader) (err error) {
	bls.delim = DEFAULT_DELIM
	bufReader := bufio.NewReader(reader)
	var statements [][]byte
	for {
		stmt, err := bls.nextStatement(bufReader)
		// FIXME(sougou): This looks awkward
		if stmt == nil {
			return err
		}
		prefix := string(bytes.ToLower(bytes.SplitN(stmt, SPACE, 2)[0]))
		switch statementPrefixes[prefix] {
		case BL_UNRECOGNIZED:
			return fmt.Errorf("unrecognized: %s", stmt)
		case BL_BEGIN:
			// no action
		case BL_DDL:
			statements = append(statements, stmt)
			fallthrough
		case BL_COMMIT:
			trans := &BinlogTransaction{
				Statements: statements,
				Position:   bls.blPos,
			}
			if err = sendReply(trans); err != nil {
				return err
			}
			statements = nil
		case BL_ROLLBACK:
			statements = nil
		// DML & SET
		default:
			statements = append(statements, stmt)
		}
	}
}

func (bls *BinlogStreamer) nextStatement(bufReader *bufio.Reader) (stmt []byte, err error) {
eventLoop:
	for {
		if bls.running.Get() == -1 {
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
			if err = bls.extractPosition(values[1:]); err != nil {
				return nil, err
			}
			continue
		}
		values = rotateRE.FindSubmatch(event)
		if values != nil {
			if err = bls.handleRotate(values[1:]); err != nil {
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

func (bls *BinlogStreamer) extractPosition(values [][]byte) (err error) {
	if bls.blPos.ServerId, err = strconv.ParseInt(string(values[0]), 10, 64); err != nil {
		return err
	}
	pos, err := strconv.ParseInt(string(values[1]), 10, 64)
	if err != nil {
		return err
	}
	bls.file.Set(pos)
	if bls.blPos.GroupId, err = strconv.ParseInt(string(values[2]), 10, 64); err != nil {
		return err
	}
	return nil
}

func (bls *BinlogStreamer) handleRotate(values [][]byte) (err error) {
	pos, err := strconv.ParseInt(string(values[1]), 10, 64)
	if err != nil {
		return err
	}
	log.Infof("Log rotating: %s, %v", values[0], pos)
	bls.file.Rotate(string(values[0]), pos)
	return nil
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
		if err != nil ||
			bytes.HasPrefix(event, HASH_COMMENT) ||
			bytes.HasPrefix(event, SLASH_COMMENT) ||
			bytes.HasPrefix(event, DELIM_STMT) {
			return bytes.TrimSpace(event), err
		}
		if bytes.HasSuffix(event, bls.delim) {
			return bytes.TrimSpace(event[:len(event)-len(bls.delim)]), nil
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
	return f.Rotate(name, pos)
}

func (f *fileInfo) Rotate(name string, pos int64) (err error) {
	if f.handle != nil {
		f.handle.Close()
	}
	f.handle, err = os.Open(name)
	if err != nil {
		return err
	}
	f.name = name
	f.pos = pos
	return nil
}

func (f *fileInfo) Set(pos int64) {
	f.pos = pos
}

func (f *fileInfo) WaitForChange() error {
	for {
		time.Sleep(100 * time.Millisecond)
		fi, err := f.handle.Stat()
		if err != nil {
			return err
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
	return err
}
