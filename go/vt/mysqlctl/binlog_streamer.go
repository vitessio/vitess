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

var (
	// posRE is for extracting server id, end_log_pos and group_id.
	posRE = regexp.MustCompile(`#.*server id ([0-9]+).*end_log_pos ([0-9]+).*group_id ([0-9]+)`)
	// rotateRE is for extracting log rotation info.
	rotateRE = regexp.MustCompile(`#.*Rotate to ([^ \t]+).*pos: ([0-9]+)`)
	// delimRE is for reading the new delimiter.
	delimRE = regexp.MustCompile(`DELIMITER (.*;)`)
)

var (
	HASH_COMMENT  = []byte("#")
	SLASH_COMMENT = []byte("/*")
	DEFAULT_DELIM = []byte(";\n")
)

// TODO: Move to proto once finalized
type BinlogTransaction struct {
	Statements [][]byte
	Position   BinlogPosition
}

type BinlogPosition struct {
	GroupId, ServerId int64
}

type BinlogStreamer struct {
	dbname  string
	dir     string
	blPos   BinlogPosition
	file    fileInfo
	delim   []byte
	running sync2.AtomicInt32
}

type SendReplyFunc func(reply interface{}) error

func NewBinlogStreamer(dbname, binlogPrefix string) *BinlogStreamer {
	return &BinlogStreamer{
		dbname: dbname,
		dir:    path.Dir(binlogPrefix),
	}
}

func (bls *BinlogStreamer) Stream(file string, pos int64, sendReply SendReplyFunc) (err error) {
	if !bls.running.CompareAndSwap(0, 1) {
		return fmt.Errorf("already streaming")
	}
	if err = bls.file.Init(path.Join(bls.dir, file), pos); err != nil {
		return err
	}
	defer bls.file.Close()

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

/*
func (bls *BinlogStreamer) coordinates(groupId, serverId int64) (file string, pos int64, err error) {
	// if group id is not known(0), we start from the latest position.
	if groupId == 0 {
		file, pos, _, err = bls.masterStatus()
		return file, pos, err
	}

	qr, err := bls.conn.ExecuteFetch(fmt.Sprintf("show binlog info for %d", groupId), 1, false)
	if err != nil {
		return "", 0, err
	}
	file = qr.Rows[0][0].String()
	pos, err = qr.Rows[0][1].ParseInt64()
	if err != nil {
		return "", 0, err
	}
	dbserverid, err := qr.Rows[0][2].ParseInt64()
	if err != nil {
		return "", 0, err
	}
	// If server id is not known (0), we don't check.
	if serverId != 0 && serverId != dbserverid {
		return "", 0, fmt.Errorf("server id %v does not match %v", serverId, dbserverid)
	}
	return file, pos, err
}

func (bls *BinlogStreamer) masterStatus() (file string, pos, groupId int64, err error) {
	qr, err := bls.conn.ExecuteFetch(fmt.Sprintf("show binlog info for %d", groupId), 1, false)
	if err != nil {
		return "", 0, 0, err
	}
	file = qr.Rows[0][0].String()
	pos, err = qr.Rows[0][1].ParseInt64()
	if err != nil {
		return "", 0, 0, err
	}
	groupId, err = qr.Rows[0][4].ParseInt64()
	if err != nil {
		return "", 0, 0, err
	}
	return file, pos, groupId, nil
}
*/

func (bls *BinlogStreamer) readEvent(reader *bufio.Reader) (event []byte, err error) {
	for {
		fragment, err := reader.ReadSlice('\n')
		event = append(event, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil || bytes.HasPrefix(event, HASH_COMMENT) || bytes.HasPrefix(event, SLASH_COMMENT) {
			return bytes.TrimSpace(event), err
		}
		if bytes.HasSuffix(event, bls.delim) {
			return bytes.TrimSpace(event[:len(event)-len(bls.delim)]), nil
		}
	}
}

func (bls *BinlogStreamer) parseEvents(sendReply SendReplyFunc, reader io.Reader) (err error) {
	bls.delim = DEFAULT_DELIM
	lineReader := bufio.NewReader(reader)
	for {
		if bls.running.Get() == 0 {
			return io.EOF
		}
		event, err := lineReader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if event[0] == '#' {
			values := posRE.FindSubmatch(event)
			if len(values) != 3 {
				continue
			}
			bls.blPos.ServerId, err = strconv.ParseInt(string(values[1]), 10, 64)
			if err != nil {
				return err
			}
			bls.blPos.GroupId, err = strconv.ParseInt(string(values[2]), 10, 64)
			if err != nil {
				return err
			}
			continue
		}
		// Parse regular line
	}
	return nil
}

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
