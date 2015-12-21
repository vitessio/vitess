// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"encoding/binary"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
)

// SlaveConnection represents a connection to mysqld that pretends to be a slave
// connecting for replication. Each such connection must identify itself to
// mysqld with a server ID that is unique both among other SlaveConnections and
// among actual slaves in the topology.
type SlaveConnection struct {
	sqldb.Conn
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
func (mysqld *Mysqld) NewSlaveConnection() (*SlaveConnection, error) {
	params, err := dbconfigs.MysqlParams(mysqld.dba)
	if err != nil {
		return nil, err
	}

	conn, err := sqldb.Connect(params)
	if err != nil {
		return nil, err
	}

	sc := &SlaveConnection{
		Conn:    conn,
		mysqld:  mysqld,
		slaveID: slaveIDPool.Get(),
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
func (sc *SlaveConnection) StartBinlogDump(startPos replication.Position) (<-chan replication.BinlogEvent, error) {
	flavor, err := sc.mysqld.flavor()
	if err != nil {
		return nil, fmt.Errorf("StartBinlogDump needs flavor: %v", err)
	}

	log.Infof("sending binlog dump command: startPos=%v, slaveID=%v", startPos, sc.slaveID)
	if err = flavor.SendBinlogDumpCommand(sc, startPos); err != nil {
		log.Errorf("couldn't send binlog dump command: %v", err)
		return nil, err
	}

	// Read the first packet to see if it's an error response to our dump command.
	buf, err := sc.Conn.ReadPacket()
	if err != nil {
		log.Errorf("couldn't start binlog dump: %v", err)
		return nil, err
	}

	eventChan := make(chan replication.BinlogEvent)

	// Start reading events.
	sc.svm.Go(func(svc *sync2.ServiceContext) error {
		defer close(eventChan)

		for svc.IsRunning() {
			if buf[0] == 254 {
				// The master is telling us to stop.
				log.Infof("received EOF packet in binlog dump: %#v", buf)
				return nil
			}

			select {
			// Skip the first byte because it's only used for signaling EOF.
			case eventChan <- flavor.MakeBinlogEvent(buf[1:]):
			case <-svc.ShuttingDown:
				return nil
			}

			buf, err = sc.Conn.ReadPacket()
			if err != nil {
				if sqlErr, ok := err.(*sqldb.SQLError); ok && sqlErr.Number() == mysql.ErrServerLost {
					// ErrServerLost = Lost connection to MySQL server during query
					// This is not necessarily an error. It could just be that we closed
					// the connection from outside.
					log.Infof("connection closed during binlog stream (possibly intentional): %v", err)
					return err
				}
				log.Errorf("read error while streaming binlog events: %v", err)
				return err
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
	if sc.Conn != nil {
		log.Infof("shutting down slave socket to unblock reads")
		sc.Conn.Shutdown()

		log.Infof("waiting for slave dump thread to end")
		sc.svm.Stop()

		log.Infof("closing slave MySQL client, recycling slaveID %v", sc.slaveID)
		sc.Conn.Close()
		sc.Conn = nil
		slaveIDPool.Put(sc.slaveID)
	}
}

// makeBinlogDumpCommand builds a buffer containing the data for a MySQL
// COM_BINLOG_DUMP command.
func makeBinlogDumpCommand(pos uint32, flags uint16, serverID uint32, filename string) []byte {
	var buf bytes.Buffer
	buf.Grow(4 + 2 + 4 + len(filename))

	// binlog_pos (4 bytes)
	binary.Write(&buf, binary.LittleEndian, pos)
	// binlog_flags (2 bytes)
	binary.Write(&buf, binary.LittleEndian, flags)
	// server_id of slave (4 bytes)
	binary.Write(&buf, binary.LittleEndian, serverID)
	// binlog_filename (string with no terminator and no length)
	buf.WriteString(filename)

	return buf.Bytes()
}
