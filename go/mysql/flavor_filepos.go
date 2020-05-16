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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
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

// masterGTIDSet is part of the Flavor interface.
func (flv *filePosFlavor) masterGTIDSet(c *Conn) (GTIDSet, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, errors.New("no master status")
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return nil, err
	}
	pos, err := strconv.Atoi(resultMap["Position"])
	if err != nil {
		return nil, fmt.Errorf("invalid FilePos GTID (%v): expecting pos to be an integer", resultMap["Position"])
	}

	return filePosGTID{
		file: resultMap["File"],
		pos:  pos,
	}, nil
}

func (flv *filePosFlavor) startSlaveCommand() string {
	return "unsupported"
}

func (flv *filePosFlavor) restartSlaveCommands() []string {
	return []string{"unsupported"}
}

func (flv *filePosFlavor) stopSlaveCommand() string {
	return "unsupported"
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (flv *filePosFlavor) sendBinlogDumpCommand(c *Conn, slaveID uint32, startPos Position) error {
	rpos, ok := startPos.GTIDSet.(filePosGTID)
	if !ok {
		return fmt.Errorf("startPos.GTIDSet is wrong type - expected filePosGTID, got: %#v", startPos.GTIDSet)
	}

	flv.file = rpos.file
	return c.WriteComBinlogDump(slaveID, rpos.file, uint32(rpos.pos), 0)
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
			return nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", io.EOF)
		case ErrPacket:
			return nil, ParseErrorPacket(result)
		}

		event := &filePosBinlogEvent{binlogEvent: binlogEvent(result[1:])}
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

// setSlavePositionCommands is part of the Flavor interface.
func (flv *filePosFlavor) setSlavePositionCommands(pos Position) []string {
	return []string{
		"unsupported",
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (flv *filePosFlavor) changeMasterArg() string {
	return "unsupported"
}

// status is part of the Flavor interface.
func (flv *filePosFlavor) status(c *Conn) (SlaveStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return SlaveStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a slave.
		return SlaveStatus{}, ErrNotSlave
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return SlaveStatus{}, err
	}

	status := parseSlaveStatus(resultMap)
	pos, err := strconv.Atoi(resultMap["Exec_Master_Log_Pos"])
	if err != nil {
		return SlaveStatus{}, fmt.Errorf("invalid FilePos GTID (%v): expecting pos to be an integer", resultMap["Exec_Master_Log_Pos"])
	}

	status.Position.GTIDSet = filePosGTID{
		file: resultMap["Relay_Master_Log_File"],
		pos:  pos,
	}

	return status, nil
}

// waitUntilPositionCommand is part of the Flavor interface.
func (flv *filePosFlavor) waitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	filePosPos, ok := pos.GTIDSet.(filePosGTID)
	if !ok {
		return "", fmt.Errorf("Position is not filePos compatible: %#v", pos.GTIDSet)
	}

	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return "", fmt.Errorf("timed out waiting for position %v", pos)
		}
		return fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %d, %.6f)", filePosPos.file, filePosPos.pos, timeout.Seconds()), nil
	}

	return fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %d)", filePosPos.file, filePosPos.pos), nil
}

func (*filePosFlavor) startSlaveUntilAfter(pos Position) string {
	return "unsupported"
}

// enableBinlogPlaybackCommand is part of the Flavor interface.
func (*filePosFlavor) enableBinlogPlaybackCommand() string {
	return ""
}

// disableBinlogPlaybackCommand is part of the Flavor interface.
func (*filePosFlavor) disableBinlogPlaybackCommand() string {
	return ""
}
