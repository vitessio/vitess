package replication

import (
	"encoding/binary"
	"fmt"
)

// mariadbBinlogEvent wraps a raw packet buffer and provides methods to examine
// it by implementing BinlogEvent. Some methods are pulled in from
// binlogEvent.
type mariadbBinlogEvent struct {
	binlogEvent
}

// NewMariadbBinlogEvent creates a BinlogEvent instance from given byte array
func NewMariadbBinlogEvent(buf []byte) BinlogEvent {
	return mariadbBinlogEvent{binlogEvent: binlogEvent(buf)}
}

// IsGTID implements BinlogEvent.IsGTID().
func (ev mariadbBinlogEvent) IsGTID() bool {
	return ev.Type() == eMariaGTIDEvent
}

// GTID implements BinlogEvent.GTID().
//
// Expected format:
//   # bytes   field
//   8         sequence number
//   4         domain ID
//   1         flags2
func (ev mariadbBinlogEvent) GTID(f BinlogFormat) (GTID, bool, error) {
	const FLStandalone = 1

	data := ev.Bytes()[f.HeaderLength:]
	flags2 := data[8+4]

	return MariadbGTID{
		Sequence: binary.LittleEndian.Uint64(data[:8]),
		Domain:   binary.LittleEndian.Uint32(data[8 : 8+4]),
		Server:   ev.ServerID(),
	}, flags2&FLStandalone == 0, nil
}

// PreviousGTIDs implements BinlogEvent.PreviousGTIDs().
func (ev mariadbBinlogEvent) PreviousGTIDs(f BinlogFormat) (Position, error) {
	return Position{}, fmt.Errorf("MariaDB should not provide PREVIOUS_GTIDS_EVENT events")
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev mariadbBinlogEvent) StripChecksum(f BinlogFormat) (BinlogEvent, []byte, error) {
	switch f.ChecksumAlgorithm {
	case BinlogChecksumAlgOff, BinlogChecksumAlgUndef:
		// There is no checksum.
		return ev, nil, nil
	default:
		// Checksum is the last 4 bytes of the event buffer.
		data := ev.Bytes()
		length := len(data)
		checksum := data[length-4:]
		data = data[:length-4]
		return mariadbBinlogEvent{binlogEvent: binlogEvent(data)}, checksum, nil
	}
}
