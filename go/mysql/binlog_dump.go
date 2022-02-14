package mysql

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// BinglogMagicNumber is 4-byte number at the beginning of every binary log
	BinglogMagicNumber = []byte{0xfe, 0x62, 0x69, 0x6e}
	readPacketErr      = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error reading BinlogDumpGTID packet")
)

func (c *Conn) parseComBinlogDumpGTID(data []byte) (logFile string, logPos uint64, position Position, err error) {
	pos := 1
	pos += 2 // flags
	pos += 4 // server-id

	fileNameLen, pos, ok := readUint32(data, pos)
	if !ok {
		return logFile, logPos, position, readPacketErr
	}
	logFile = string(data[pos : pos+int(fileNameLen)])
	pos += int(fileNameLen)

	logPos, pos, ok = readUint64(data, pos)
	if !ok {
		return logFile, logPos, position, readPacketErr
	}

	dataSize, pos, ok := readUint32(data, pos)
	if !ok {
		return logFile, logPos, position, readPacketErr
	}
	if gtid := string(data[pos : pos+int(dataSize)]); gtid != "" {
		position, err = DecodePosition(gtid)
		if err != nil {
			return logFile, logPos, position, err
		}
	}

	return logFile, logPos, position, nil
}
