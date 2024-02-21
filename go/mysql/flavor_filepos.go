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

package mysql

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type filePosFlavor struct {
	format     BinlogFormat
	file       string
	savedEvent BinlogEvent
}

// newFilePosFlavor creates a new filePos flavor.
func newFilePosFlavor() flavor {
	return &filePosFlavor{}
}

// primaryGTIDSet is part of the Flavor interface.
func (flv *filePosFlavor) primaryGTIDSet(c *Conn) (replication.GTIDSet, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, ErrNoPrimaryStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return nil, err
	}
	return replication.ParseFilePosGTIDSet(fmt.Sprintf("%s:%s", resultMap["File"], resultMap["Position"]))
}

// purgedGTIDSet is part of the Flavor interface.
func (flv *filePosFlavor) purgedGTIDSet(c *Conn) (replication.GTIDSet, error) {
	return nil, nil
}

// gtidMode is part of the Flavor interface.
func (flv *filePosFlavor) gtidMode(c *Conn) (string, error) {
	qr, err := c.ExecuteFetch("select @@global.gtid_mode", 1, false)
	if err != nil {
		return "", err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for gtid_mode: %#v", qr)
	}
	return qr.Rows[0][0].ToString(), nil
}

// serverUUID is part of the Flavor interface.
func (flv *filePosFlavor) serverUUID(c *Conn) (string, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.server_uuid", 1, false)
	if err != nil {
		return "", err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for server_uuid: %#v", qr)
	}
	return qr.Rows[0][0].ToString(), nil
}

func (flv *filePosFlavor) startReplicationCommand() string {
	return "unsupported"
}

func (flv *filePosFlavor) restartReplicationCommands() []string {
	return []string{"unsupported"}
}

func (flv *filePosFlavor) stopReplicationCommand() string {
	return "unsupported"
}

func (flv *filePosFlavor) stopIOThreadCommand() string {
	return "unsupported"
}

func (flv *filePosFlavor) stopSQLThreadCommand() string {
	return "unsupported"
}

func (flv *filePosFlavor) startSQLThreadCommand() string {
	return "unsupported"
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (flv *filePosFlavor) sendBinlogDumpCommand(c *Conn, serverID uint32, binlogFilename string, startPos replication.Position) error {
	rpos, ok := startPos.GTIDSet.(replication.FilePosGTID)
	if !ok {
		return fmt.Errorf("startPos.GTIDSet is wrong type - expected filePosGTID, got: %#v", startPos.GTIDSet)
	}

	flv.file = rpos.File
	return c.WriteComBinlogDump(serverID, rpos.File, rpos.Pos, 0)
}

// readBinlogEvent is part of the Flavor interface.
func (flv *filePosFlavor) readBinlogEvent(c *Conn) (BinlogEvent, error) {
	if ret := flv.savedEvent; ret != nil {
		flv.savedEvent = nil
		return ret, nil
	}

	for {
		result, err := c.ReadPacket()
		if err != nil {
			return nil, err
		}
		switch result[0] {
		case EOFPacket:
			return nil, sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "%v", io.EOF)
		case ErrPacket:
			return nil, ParseErrorPacket(result)
		}

		buf, semiSyncAckRequested, err := c.AnalyzeSemiSyncAckRequest(result[1:])
		if err != nil {
			return nil, err
		}
		event := newFilePosBinlogEventWithSemiSyncInfo(buf, semiSyncAckRequested)
		switch event.Type() {
		case eGTIDEvent, eAnonymousGTIDEvent, ePreviousGTIDsEvent, eMariaGTIDListEvent:
			// Don't transmit fake or irrelevant events because we should not
			// resume replication at these positions.
			continue
		case eMariaGTIDEvent:
			// Copied from mariadb flavor.
			const FLStandalone = 1
			flags2 := result[8+4]
			// This means that it's also a BEGIN event.
			if flags2&FLStandalone == 0 {
				return newFilePosQueryEvent("begin", event.Timestamp()), nil
			}
			// Otherwise, don't send this event.
			continue
		case eFormatDescriptionEvent:
			format, err := event.Format()
			if err != nil {
				return nil, err
			}
			flv.format = format
		case eRotateEvent:
			if !flv.format.IsZero() {
				stripped, _, _ := event.StripChecksum(flv.format)
				_, flv.file = stripped.(*filePosBinlogEvent).rotate(flv.format)
				// No need to transmit. Just update the internal position for the next event.
				continue
			}
		case eXIDEvent, eTableMapEvent,
			eWriteRowsEventV0, eWriteRowsEventV1, eWriteRowsEventV2,
			eDeleteRowsEventV0, eDeleteRowsEventV1, eDeleteRowsEventV2,
			eUpdateRowsEventV0, eUpdateRowsEventV1, eUpdateRowsEventV2:
			flv.savedEvent = event
			return newFilePosGTIDEvent(flv.file, event.nextPosition(flv.format), event.Timestamp()), nil
		case eQueryEvent:
			q, err := event.Query(flv.format)
			if err == nil && strings.HasPrefix(q.SQL, "#") {
				continue
			}
			flv.savedEvent = event
			return newFilePosGTIDEvent(flv.file, event.nextPosition(flv.format), event.Timestamp()), nil
		default:
			// For unrecognized events, send a fake "repair" event so that
			// the position gets transmitted.
			if !flv.format.IsZero() {
				if v := event.nextPosition(flv.format); v != 0 {
					flv.savedEvent = newFilePosQueryEvent("repair", event.Timestamp())
					return newFilePosGTIDEvent(flv.file, v, event.Timestamp()), nil
				}
			}
		}
		return event, nil
	}
}

// resetReplicationCommands is part of the Flavor interface.
func (flv *filePosFlavor) resetReplicationCommands(c *Conn) []string {
	return []string{
		"unsupported",
	}
}

// resetReplicationParametersCommands is part of the Flavor interface.
func (flv *filePosFlavor) resetReplicationParametersCommands(c *Conn) []string {
	return []string{
		"unsupported",
	}
}

// setReplicationPositionCommands is part of the Flavor interface.
func (flv *filePosFlavor) setReplicationPositionCommands(pos replication.Position) []string {
	return []string{
		"unsupported",
	}
}

// setReplicationPositionCommands is part of the Flavor interface.
func (flv *filePosFlavor) changeReplicationSourceArg() string {
	return "unsupported"
}

// status is part of the Flavor interface.
func (flv *filePosFlavor) status(c *Conn) (replication.ReplicationStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a replica.
		return replication.ReplicationStatus{}, ErrNotReplica
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}

	return replication.ParseFilePosReplicationStatus(resultMap)
}

// primaryStatus is part of the Flavor interface.
func (flv *filePosFlavor) primaryStatus(c *Conn) (replication.PrimaryStatus, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. We don't know how this could happen.
		return replication.PrimaryStatus{}, ErrNoPrimaryStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}

	return replication.ParseFilePosPrimaryStatus(resultMap)
}

// waitUntilPosition is part of the Flavor interface.
func (flv *filePosFlavor) waitUntilPosition(ctx context.Context, c *Conn, pos replication.Position) error {
	filePosPos, ok := pos.GTIDSet.(replication.FilePosGTID)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "position is not filePos compatible: %#v", pos.GTIDSet)
	}

	query := fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %d)", filePosPos.File, filePosPos.Pos)
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for position %v", pos)
		}
		query = fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %d, %.6f)", filePosPos.File, filePosPos.Pos, timeout.Seconds())
	}

	result, err := c.ExecuteFetch(query, 1, false)
	if err != nil {
		return err
	}

	// For MASTER_POS_WAIT(), the return value is the number of log events
	// the replica had to wait for to advance to the specified position.
	// The function returns NULL if the replica SQL thread is not started,
	// the replica's source information is not initialized, the arguments
	// are incorrect, or an error occurs. It returns -1 if the timeout has
	// been exceeded. If the replica SQL thread stops while MASTER_POS_WAIT()
	// is waiting, the function returns NULL. If the replica is past the
	// specified position, the function returns immediately.
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid results: %#v", result)
	}
	val := result.Rows[0][0]
	if val.IsNull() {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "replication is not running")
	}
	state, err := val.ToInt64()
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid result of %#v", val)
	}
	switch {
	case state == -1:
		return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for position %v", pos)
	case state >= 0:
		return nil
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid result of %d", state)
	}
}

func (*filePosFlavor) startReplicationUntilAfter(pos replication.Position) string {
	return "unsupported"
}

func (*filePosFlavor) startSQLThreadUntilAfter(pos replication.Position) string {
	return "unsupported"
}

// baseShowTables is part of the Flavor interface.
func (*filePosFlavor) baseShowTables() string {
	return mysqlFlavor{}.baseShowTables()
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (*filePosFlavor) baseShowTablesWithSizes() string {
	return TablesWithSize56
}

// supportsCapability is part of the Flavor interface.
func (*filePosFlavor) supportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	switch capability {
	default:
		return false, nil
	}
}
