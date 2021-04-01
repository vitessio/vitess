/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sync"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
)

var (
	// ErrBinlogUnavailable is returned by this library when we
	// cannot find a suitable binlog to satisfy the request.
	ErrBinlogUnavailable = fmt.Errorf("cannot find relevant binlogs on this server")
)

// BinlogConnection represents a connection to mysqld that pretends to be a replica
// connecting for replication. Each such connection must identify itself to
// mysqld with a server ID that is unique both among other BinlogConnections and
// among actual replicas in the topology.
type BinlogConnection struct {
	*mysql.Conn
	cp       dbconfigs.Connector
	serverID uint32
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// serverIDPool is the IDPool for server IDs used to connect as a replica.
// We randomize the initial value, so it is unlikely that we reuse serverID
// values when connecting to the same server (e.g. if it is external MySQL
// like RDS);  but we DO want to reuse serverID values from the same
// client, or we will start orphaning binlog streaming connections
var serverIDPool = pools.NewIDPool(getRandomInitialServerID())

func getRandomInitialServerID() uint32 {
	// Leave some breathing room below MaxInt32 to generate IDs into
	max := big.NewInt(math.MaxInt32 - 10000)
	id, _ := crand.Int(crand.Reader, max)
	return uint32(id.Int64())
}

// NewBinlogConnection creates a new binlog connection to the mysqld instance.
func NewBinlogConnection(cp dbconfigs.Connector) (*BinlogConnection, error) {
	conn, err := connectForReplication(cp)
	if err != nil {
		return nil, err
	}

	bc := &BinlogConnection{
		Conn:     conn,
		cp:       cp,
		serverID: serverIDPool.Get(),
	}
	log.Infof("new binlog connection: serverID=%d", bc.serverID)
	return bc, nil
}

// connectForReplication create a MySQL connection ready to use for replication.
func connectForReplication(cp dbconfigs.Connector) (*mysql.Conn, error) {
	ctx := context.Background()
	conn, err := cp.Connect(ctx)
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

// StartBinlogDumpFromCurrent requests a replication binlog dump from
// the current position.
func (bc *BinlogConnection) StartBinlogDumpFromCurrent(ctx context.Context) (mysql.Position, <-chan mysql.BinlogEvent, error) {
	ctx, bc.cancel = context.WithCancel(ctx)

	masterPosition, err := bc.Conn.MasterPosition()
	if err != nil {
		return mysql.Position{}, nil, fmt.Errorf("failed to get master position: %v", err)
	}

	c, err := bc.StartBinlogDumpFromPosition(ctx, masterPosition)
	return masterPosition, c, err
}

// StartBinlogDumpFromPosition requests a replication binlog dump from
// the master mysqld at the given Position and then sends binlog
// events to the provided channel.
// The stream will continue in the background, waiting for new events if
// necessary, until the connection is closed, either by the master or
// by canceling the context.
//
// Note the context is valid and used until eventChan is closed.
func (bc *BinlogConnection) StartBinlogDumpFromPosition(ctx context.Context, startPos mysql.Position) (<-chan mysql.BinlogEvent, error) {
	ctx, bc.cancel = context.WithCancel(ctx)

	log.Infof("sending binlog dump command: startPos=%v, serverID=%v", startPos, bc.serverID)
	if err := bc.SendBinlogDumpCommand(bc.serverID, startPos); err != nil {
		log.Errorf("couldn't send binlog dump command: %v", err)
		return nil, err
	}

	return bc.streamEvents(ctx), nil
}

// streamEvents returns a channel on which events are streamed.
func (bc *BinlogConnection) streamEvents(ctx context.Context) chan mysql.BinlogEvent {
	// FIXME(alainjobart) I think we can use a buffered channel for better performance.
	eventChan := make(chan mysql.BinlogEvent)

	// Start reading events.
	bc.wg.Add(1)
	go func() {
		defer func() {
			close(eventChan)
			bc.wg.Done()
		}()
		for {
			event, err := bc.Conn.ReadBinlogEvent()
			if err != nil {
				if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.CRServerLost {
					// CRServerLost = Lost connection to MySQL server during query
					// This is not necessarily an error. It could just be that we closed
					// the connection from outside.
					log.Infof("connection closed during binlog stream (possibly intentional): %v", err)
					return
				}
				log.Errorf("read error while streaming binlog events: %v", err)
				return
			}

			select {
			case eventChan <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return eventChan
}

// StartBinlogDumpFromBinlogBeforeTimestamp requests a replication
// binlog dump from the master mysqld starting with a file that has
// timestamps smaller than the provided timestamp, and then sends
// binlog events to the provided channel.
//
// The startup phase will list all the binary logs, and find the one
// that has events starting strictly before the provided timestamp. It
// will then start from there, and stream all events. It is the
// responsibility of the calling site to filter the events more.
//
// MySQL 5.6+ note: we need to do it that way because of the way the
// GTIDSet works. In the previous two streaming functions, we pass in
// the full GTIDSet (that has the list of all transactions seen in
// the replication stream). In this case, we don't know it, all we
// have is the binlog file names. We depend on parsing the first
// PREVIOUS_GTIDS_EVENT event in the logs to get it. So we need the
// caller to parse that event, and it can't be skipped because its
// timestamp is lower. Then, for each subsequent event, the caller
// also needs to add the event GTID to its GTIDSet. Otherwise it won't
// be correct ever. So the caller really needs to build up its GTIDSet
// along the entire file, not just for events whose timestamp is in a
// given range.
//
// The stream will continue in the background, waiting for new events if
// necessary, until the connection is closed, either by the master or
// by canceling the context.
//
// Note the context is valid and used until eventChan is closed.
func (bc *BinlogConnection) StartBinlogDumpFromBinlogBeforeTimestamp(ctx context.Context, timestamp int64) (<-chan mysql.BinlogEvent, error) {
	ctx, bc.cancel = context.WithCancel(ctx)

	filename, err := bc.findFileBeforeTimestamp(ctx, timestamp)
	if err != nil {
		return nil, err
	}

	// Start dumping the logs. The position is '4' to skip the
	// Binlog File Header. See this page for more info:
	// https://dev.mysql.com/doc/internals/en/binlog-file.html
	if err := bc.Conn.WriteComBinlogDump(bc.serverID, filename, 4, 0); err != nil {
		return nil, fmt.Errorf("failed to send the ComBinlogDump command: %v", err)
	}
	return bc.streamEvents(ctx), nil
}

func (bc *BinlogConnection) findFileBeforeTimestamp(ctx context.Context, timestamp int64) (filename string, err error) {
	// List the binlogs.
	binlogs, err := bc.Conn.ExecuteFetch("SHOW BINARY LOGS", 1000, false)
	if err != nil {
		return "", fmt.Errorf("failed to SHOW BINARY LOGS: %v", err)
	}

	// Start with the most recent binlog file until we find the right event.
	for binlogIndex := len(binlogs.Rows) - 1; binlogIndex >= 0; binlogIndex-- {
		// Exit the loop early if context is canceled.
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		filename := binlogs.Rows[binlogIndex][0].ToString()
		blTimestamp, err := bc.getBinlogTimeStamp(filename)
		if err != nil {
			return "", err
		}
		if blTimestamp < timestamp {
			// The binlog timestamp is older: we've found a good starting point.
			return filename, nil
		}
	}

	log.Errorf("couldn't find an old enough binlog to match timestamp >= %v (looked at %v files)", timestamp, len(binlogs.Rows))
	return "", ErrBinlogUnavailable
}

func (bc *BinlogConnection) getBinlogTimeStamp(filename string) (blTimestamp int64, err error) {
	conn, err := connectForReplication(bc.cp)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if err := conn.WriteComBinlogDump(bc.serverID, filename, 4, 0); err != nil {
		return 0, fmt.Errorf("failed to send the ComBinlogDump command: %v", err)
	}

	// Get the first event to get its timestamp. We skip
	// events that don't have timestamps (although it seems
	// most do anyway).
	for {
		event, err := conn.ReadBinlogEvent()
		if err != nil {
			return 0, fmt.Errorf("error reading binlog event %v: %v", filename, err)
		}
		if !event.IsValid() {
			return 0, fmt.Errorf("first event from binlog %v is not valid", filename)
		}
		if ts := event.Timestamp(); ts > 0 {
			return int64(ts), nil
		}
	}
}

// Close closes the binlog connection, which also signals an ongoing dump
// started with StartBinlogDump() to stop and close its BinlogEvent channel.
// The ID for the binlog connection is recycled back into the pool.
func (bc *BinlogConnection) Close() {
	if bc.Conn != nil {
		log.Infof("closing binlog socket to unblock reads")
		bc.Conn.Close()

		// bc.cancel is set at the beginning of the StartBinlogDump*
		// methods. If we error out before then, it's nil.
		// Note we also may error out before adding 1 to bc.wg,
		// but then the Wait() still works.
		if bc.cancel != nil {
			log.Infof("waiting for binlog dump thread to end")
			bc.cancel()
			bc.wg.Wait()
			bc.cancel = nil
		}

		log.Infof("closing binlog MySQL client with serverID %v. Will recycle ID.", bc.serverID)
		bc.Conn = nil
		serverIDPool.Put(bc.serverID)
	}
}
