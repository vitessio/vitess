// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sync2"
)

var (
	// posExtract is a regular expression to extract the position
	posExtract = regexp.MustCompile(".*server id ([0-9]+).*group_id ([0-9]+).*")
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
	conn    *mysql.Connection
	current BinlogPosition
	running sync2.AtomicInt32
}

type SendReplyFunc func(reply interface{}) error

func NewBinlogStreamer(dbname, binlogPrefix string) *BinlogStreamer {
	return &BinlogStreamer{
		dbname: dbname,
		dir:    path.Dir(binlogPrefix),
	}
}

// TODO: Find better solution than passing in a connection.
func (bls *BinlogStreamer) Stream(groupId, serverId int64, conn *mysql.Connection, sendReply SendReplyFunc) (err error) {
	bls.conn = conn
	if !bls.running.CompareAndSwap(0, 1) {
		return fmt.Errorf("Already streaming")
	}
	bls.current.GroupId = groupId
	bls.current.ServerId = serverId
	var lastRun time.Time
	for {
		lastRun = time.Now()
		err = bls.run(sendReply)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Errorf("Stream error @ %#v, error: %v", bls, err)
			return err
		}
		diff := time.Now().Sub(lastRun)
		if diff < (100 * time.Millisecond) {
			time.Sleep(100*time.Millisecond - diff)
		}
	}
}

func (bls *BinlogStreamer) run(sendReply SendReplyFunc) (err error) {
	mbl := &MysqlBinlog{}
	file, pos, err := bls.coordinates(bls.current.GroupId, bls.current.ServerId)
	if err != nil {
		return err
	}
	reader, err := mbl.Launch(bls.dbname, path.Join(bls.dir, file), pos)
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

func (bls *BinlogStreamer) parseEvents(sendReply SendReplyFunc, reader io.Reader) (err error) {
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
			values := posExtract.FindSubmatch(event)
			if len(values) != 3 {
				continue
			}
			bls.current.ServerId, err = strconv.ParseInt(string(values[1]), 10, 64)
			if err != nil {
				return err
			}
			bls.current.GroupId, err = strconv.ParseInt(string(values[2]), 10, 64)
			if err != nil {
				return err
			}
			continue
		}
		// Parse regular line
	}
	return nil
}
