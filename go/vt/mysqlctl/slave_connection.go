// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
)

var (
	// ErrBinlogUnavailable is returned by this library when we
	// cannot find a suitable binlog to satisfy the request.
	ErrBinlogUnavailable = fmt.Errorf("cannot find relevant binlogs on this server")
)

// SlaveConnection represents a connection to mysqld that pretends to be a slave
// connecting for replication. Each such connection must identify itself to
// mysqld with a server ID that is unique both among other SlaveConnections and
// among actual slaves in the topology.
type SlaveConnection struct {
	sqldb.Conn
	mysqld  *Mysqld
	slaveID uint32
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewSlaveConnection creates a new slave connection to the mysqld instance.
// It uses a pools.IDPool to ensure that the server IDs used to connect are
// unique within this process. This is done with the assumptions that:
//
// 1) No other processes are making fake slave connections to our mysqld.
// 2) No real slave servers will have IDs in the range 1-N where N is the peak
//    number of concurrent fake slave connections we will ever make.
func (mysqld *Mysqld) NewSlaveConnection() (*SlaveConnection, error) {
	conn, err := mysqld.connectForReplication()
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

// connectForReplication create a MySQL connection ready to use for replication.
func (mysqld *Mysqld) connectForReplication() (sqldb.Conn, error) {
	params, err := dbconfigs.MysqlParams(mysqld.dba)
	if err != nil {
		return nil, err
	}

	conn, err := sqldb.Connect(params)
	if err != nil {
		return nil, err
	}

	// Tell the server that we understand the format of events
	// that will be used if binlog_checksum is enabled on the server.
	if _, err := conn.ExecuteFetch("SET @master_binlog_checksum=@@global.binlog_checksum", 0, false); err != nil {
		return nil, fmt.Errorf("failed to set @master_binlog_checksum=@@global.binlog_checksum: %v", err)
	}

	return conn, nil
}

// slaveIDPool is the IDPool for server IDs used to connect as a slave.
var slaveIDPool = pools.NewIDPool()

// StartBinlogDumpFromPosition requests a replication binlog dump from
// the master mysqld at the given Position and then sends binlog
// events to the provided channel.
// The stream will continue in the background, waiting for new events if
// necessary, until the connection is closed, either by the master or
// by canceling the context.
//
// Note the context is valid and used until eventChan is closed.
func (sc *SlaveConnection) StartBinlogDumpFromPosition(ctx context.Context, startPos replication.Position) (<-chan replication.BinlogEvent, error) {
	ctx, sc.cancel = context.WithCancel(ctx)

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

	// FIXME(alainjobart) I think we can use a buffered channel for better performance.
	eventChan := make(chan replication.BinlogEvent)

	// Start reading events.
	sc.wg.Add(1)
	go func() {
		defer func() {
			close(eventChan)
			sc.wg.Done()
		}()
		for {
			if buf[0] == 254 {
				// The master is telling us to stop.
				log.Infof("received EOF packet in binlog dump: %#v", buf)
				return
			}

			select {
			// Skip the first byte because it's only used for signaling EOF.
			case eventChan <- flavor.MakeBinlogEvent(buf[1:]):
			case <-ctx.Done():
				return
			}

			buf, err = sc.Conn.ReadPacket()
			if err != nil {
				if sqlErr, ok := err.(*sqldb.SQLError); ok && sqlErr.Number() == mysql.ErrServerLost {
					// ErrServerLost = Lost connection to MySQL server during query
					// This is not necessarily an error. It could just be that we closed
					// the connection from outside.
					log.Infof("connection closed during binlog stream (possibly intentional): %v", err)
					return
				}
				log.Errorf("read error while streaming binlog events: %v", err)
				return
			}
		}
	}()

	return eventChan, nil
}

// StartBinlogDumpFromTimestamp requests a replication binlog dump from
// the master mysqld at the given timestamp and then sends binlog
// events to the provided channel.
//
// The startup phase will list all the binary logs, and find the one
// that has events starting strictly before the provided timestamp. It
// will then start from there, and skip all events that are before the
// provided timestamp.
//
// The stream will continue in the background, waiting for new events if
// necessary, until the connection is closed, either by the master or
// by canceling the context.
//
// Note the context is valid and used until eventChan is closed.
func (sc *SlaveConnection) StartBinlogDumpFromTimestamp(ctx context.Context, timestamp int64) (<-chan replication.BinlogEvent, error) {
	ctx, sc.cancel = context.WithCancel(ctx)

	flavor, err := sc.mysqld.flavor()
	if err != nil {
		return nil, fmt.Errorf("StartBinlogDump needs flavor: %v", err)
	}

	// List the binlogs.
	binlogs, err := sc.Conn.ExecuteFetch("SHOW BINARY LOGS", 1000, false)
	if err != nil {
		return nil, fmt.Errorf("failed to SHOW BINARY LOGS: %v", err)
	}

	// Start with the most recent binlog file until we find the right event.
	var binlogIndex int
	var firstEvent replication.BinlogEvent
	for binlogIndex = len(binlogs.Rows) - 1; binlogIndex >= 0; binlogIndex-- {
		// Exit the loop early if context is canceled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Start dumping the logs. The position is '4' to skip the
		// Binlog File Header. See this page for more info:
		// https://dev.mysql.com/doc/internals/en/binlog-file.html
		binlog := binlogs.Rows[binlogIndex][0].String()
		cmd := makeBinlogDumpCommand(4, 0, sc.slaveID, binlog)
		if err := sc.Conn.SendCommand(ComBinlogDump, cmd); err != nil {
			return nil, fmt.Errorf("failed to send the ComBinlogDump command: %v", err)
		}

		// Get the first event to get its timestamp. We skip ROTATE
		// events, as they don't have timestamps.
		for {
			buf, err := sc.Conn.ReadPacket()
			if err != nil {
				return nil, fmt.Errorf("couldn't start binlog dump of binlog %v: %v", binlog, err)
			}

			// Why would the master tell us to stop here?
			if buf[0] == 254 {
				return nil, fmt.Errorf("received EOF packet for first packet of binlog %v", binlog)
			}

			// Parse the full event.
			firstEvent = flavor.MakeBinlogEvent(buf[1:])
			if !firstEvent.IsValid() {
				return nil, fmt.Errorf("first event from binlog %v is not valid", binlog)
			}
			if !firstEvent.IsRotate() {
				break
			}
		}
		if int64(firstEvent.Timestamp()) < timestamp {
			// The first event in this binlog has a smaller
			// timestamp than what we need, we found a good
			// starting point.
			break
		}

		// The timestamp is higher, we need to try the older files.
		// Close and re-open our connection.
		sc.Conn.Close()
		conn, err := sc.mysqld.connectForReplication()
		if err != nil {
			return nil, err
		}
		sc.Conn = conn
	}
	if binlogIndex == -1 {
		// We haven't found a suitable binlog
		log.Errorf("couldn't find an old enough binlog to match timestamp >= %v (looked at %v files)", timestamp, len(binlogs.Rows))
		return nil, ErrBinlogUnavailable
	}

	// Now skip all events that have a smaller timestamp
	var event replication.BinlogEvent
	for {
		buf, err := sc.Conn.ReadPacket()
		if err != nil {
			return nil, fmt.Errorf("error reading packet while skipping binlog events: %v", err)
		}
		if buf[0] == 254 {
			// The master is telling us to stop.
			return nil, fmt.Errorf("received EOF packet in binlog dump while skipping packets: %#v", buf)
		}

		event = flavor.MakeBinlogEvent(buf[1:])
		if !event.IsValid() {
			return nil, fmt.Errorf("event from binlog is not valid (while skipping)")
		}
		if int64(event.Timestamp()) >= timestamp {
			// we found the first event to send
			break
		}

		// Exit the loop early if context is canceled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Now just loop sending and reading events.
	// FIXME(alainjobart) I think we can use a buffered channel for better performance.
	eventChan := make(chan replication.BinlogEvent)

	// Start reading events.
	sc.wg.Add(1)
	go func() {
		defer func() {
			close(eventChan)
			sc.wg.Done()
		}()

		// Send the first binlog event, it has the format description.
		select {
		case eventChan <- firstEvent:
		case <-ctx.Done():
			return
		}

		// Then send the rest.
		for {
			select {
			case eventChan <- event:
			case <-ctx.Done():
				return
			}

			buf, err := sc.Conn.ReadPacket()
			if err != nil {
				if sqlErr, ok := err.(*sqldb.SQLError); ok && sqlErr.Number() == mysql.ErrServerLost {
					// ErrServerLost = Lost connection to MySQL server during query
					// This is not necessarily an error. It could just be that we closed
					// the connection from outside.
					log.Infof("connection closed during binlog stream (possibly intentional): %v", err)
					return
				}
				log.Errorf("read error while streaming binlog events: %v", err)
				return
			}

			if buf[0] == 254 {
				// The master is telling us to stop.
				log.Infof("received EOF packet in binlog dump: %#v", buf)
				return
			}

			// Skip the first byte because it's only used
			// for signaling EOF.
			event = flavor.MakeBinlogEvent(buf[1:])
		}
	}()

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
		sc.cancel()
		sc.wg.Wait()

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

// ComBinlogDump is the command id for COM_BINLOG_DUMP.
const ComBinlogDump = 0x12
