// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"encoding/binary"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sync2"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// SlaveConnection represents a connection to mysqld that pretends to be a slave
// connecting for replication. Each such connection must identify itself to
// mysqld with a server ID that is unique both among other SlaveConnections and
// among actual slaves in the topology.
type SlaveConnection struct {
	*mysql.Connection
	mysqld  *Mysqld
	slaveID uint32
	svm     sync2.ServiceManager
}

// NewSlaveConnection creates a new slave connection to the mysqld instance.
// It uses a pools.IDPool to ensure that the server IDs used to connect are
// unique within this process. This is done with the assumptions that:
//
// 1) No other processes are making fake slave connections to our mysqld.
// 2) No real slave servers will have IDs in the range 1-N where N is the peak
//    number of concurrent fake slave connections we will ever make.
func NewSlaveConnection(mysqld *Mysqld) (*SlaveConnection, error) {
	params, err := dbconfigs.MysqlParams(mysqld.dba)
	if err != nil {
		return nil, err
	}

	conn, err := mysql.Connect(params)
	if err != nil {
		return nil, err
	}

	sc := &SlaveConnection{
		Connection: conn,
		mysqld:     mysqld,
		slaveID:    slaveIDPool.Get(),
	}
	log.Infof("new slave connection: slaveID=%d", sc.slaveID)
	return sc, nil
}

// slaveIDPool is the IDPool for server IDs used to connect as a slave.
var slaveIDPool = pools.NewIDPool()

// StartBinlogDump requests a replication binlog dump from the master mysqld
// and then immediately returns a channel on which received binlog events will
// be sent. The stream will continue, waiting for new events if necessary,
// until the connection is closed, either by the master or by calling
// SlaveConnection.Close(). At that point, the channel will also be closed.
func (sc *SlaveConnection) StartBinlogDump(startPos proto.ReplicationPosition) (<-chan blproto.BinlogEvent, error) {
	log.Infof("sending binlog dump command: startPos=%v, slaveID=%v", startPos, sc.slaveID)
	err := sc.mysqld.flavor().SendBinlogDumpCommand(sc.mysqld, sc, startPos)
	if err != nil {
		log.Errorf("binlog dump command failed: %v", err)
		return nil, err
	}

	eventChan := make(chan blproto.BinlogEvent)

	// Start reading events.
	sc.svm.Go(func(svc *sync2.ServiceContext) error {
		defer close(eventChan)

		for svc.IsRunning() {
			buf, err := sc.Connection.ReadPacket()
			if err != nil || len(buf) == 0 {
				// This is not necessarily an error. It could just be that we closed
				// the connection from outside.
				log.Infof("read error while streaming binlog events")
				return nil
			}

			if buf[0] == 254 {
				// The master is telling us to stop.
				log.Infof("received EOF packet in binlog dump: %#v", buf)
				return nil
			}

			select {
			// Skip the first byte because it's only used for signaling EOF.
			case eventChan <- sc.mysqld.flavor().MakeBinlogEvent(buf[1:]):
			case <-svc.ShuttingDown:
				return nil
			}
		}
		return nil
	})

	return eventChan, nil
}

// Close closes the slave connection, which also signals an ongoing dump
// started with StartBinlogDump() to stop and close its BinlogEvent channel.
// The ID for the slave connection is recycled back into the pool.
func (sc *SlaveConnection) Close() {
	if sc.Connection != nil {
		log.Infof("force-closing slave socket to unblock reads")
		sc.Connection.ForceClose()

		log.Infof("waiting for slave dump thread to end")
		sc.svm.Stop()

		log.Infof("closing slave MySQL client, recycling slaveID %v", sc.slaveID)
		sc.Connection.Close()
		sc.Connection = nil
		slaveIDPool.Put(sc.slaveID)
	}
}

// makeBinlogDumpCommand builds a buffer containing the data for a MySQL
// COM_BINLOG_DUMP command.
func makeBinlogDumpCommand(pos uint32, flags uint16, server_id uint32, filename string) []byte {
	var buf bytes.Buffer
	buf.Grow(4 + 2 + 4 + len(filename))

	// binlog_pos (4 bytes)
	binary.Write(&buf, binary.LittleEndian, pos)
	// binlog_flags (2 bytes)
	binary.Write(&buf, binary.LittleEndian, flags)
	// server_id of slave (4 bytes)
	binary.Write(&buf, binary.LittleEndian, server_id)
	// binlog_filename (string with no terminator and no length)
	buf.WriteString(filename)

	return buf.Bytes()
}
