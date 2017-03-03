package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// binlogEvent wraps a raw packet buffer and provides methods to examine it
// by partially implementing BinlogEvent. These methods can be composed
// into flavor-specific event types to pull in common parsing code.
//
// The default v4 header format is:
//                  offset : size
//   +============================+
//   | timestamp         0 : 4    |
//   +----------------------------+
//   | type_code         4 : 1    |
//   +----------------------------+
//   | server_id         5 : 4    |
//   +----------------------------+
//   | event_length      9 : 4    |
//   +----------------------------+
//   | next_position    13 : 4    |
//   +----------------------------+
//   | flags            17 : 2    |
//   +----------------------------+
//   | extra_headers    19 : x-19 |
//   +============================+
//   http://dev.mysql.com/doc/internals/en/event-header-fields.html
type binlogEvent []byte

// IsValid implements BinlogEvent.IsValid().
func (ev binlogEvent) IsValid() bool {
	bufLen := len(ev.Bytes())

	// The buffer must be at least 19 bytes to contain a valid header.
	if bufLen < 19 {
		return false
	}

	// It's now safe to use methods that examine header fields.
	// Let's see if the event is right about its own size.
	evLen := ev.Length()
	if evLen < 19 || evLen != uint32(bufLen) {
		return false
	}

	// Everything's there, so we shouldn't have any out-of-bounds issues while
	// reading header fields or constant-offset data fields. We should still check
	// bounds any time we compute an offset based on values in the buffer itself.
	return true
}

// Bytes returns the underlying byte buffer.
func (ev binlogEvent) Bytes() []byte {
	return []byte(ev)
}

// Type returns the type_code field from the header.
func (ev binlogEvent) Type() byte {
	return ev.Bytes()[4]
}

// Flags returns the flags field from the header.
func (ev binlogEvent) Flags() uint16 {
	return binary.LittleEndian.Uint16(ev.Bytes()[17 : 17+2])
}

// Timestamp returns the timestamp field from the header.
func (ev binlogEvent) Timestamp() uint32 {
	return binary.LittleEndian.Uint32(ev.Bytes()[:4])
}

// ServerID returns the server_id field from the header.
func (ev binlogEvent) ServerID() uint32 {
	return binary.LittleEndian.Uint32(ev.Bytes()[5 : 5+4])
}

// Length returns the event_length field from the header.
func (ev binlogEvent) Length() uint32 {
	return binary.LittleEndian.Uint32(ev.Bytes()[9 : 9+4])
}

// IsFormatDescription implements BinlogEvent.IsFormatDescription().
func (ev binlogEvent) IsFormatDescription() bool {
	return ev.Type() == eFormatDescriptionEvent
}

// IsQuery implements BinlogEvent.IsQuery().
func (ev binlogEvent) IsQuery() bool {
	return ev.Type() == eQueryEvent
}

// IsRotate implements BinlogEvent.IsRotate().
func (ev binlogEvent) IsRotate() bool {
	return ev.Type() == eRotateEvent
}

// IsXID implements BinlogEvent.IsXID().
func (ev binlogEvent) IsXID() bool {
	return ev.Type() == eXIDEvent
}

// IsIntVar implements BinlogEvent.IsIntVar().
func (ev binlogEvent) IsIntVar() bool {
	return ev.Type() == eIntVarEvent
}

// IsRand implements BinlogEvent.IsRand().
func (ev binlogEvent) IsRand() bool {
	return ev.Type() == eRandEvent
}

// IsPreviousGTIDs implements BinlogEvent.IsPreviousGTIDs().
func (ev binlogEvent) IsPreviousGTIDs() bool {
	return ev.Type() == ePreviousGTIDsEvent
}

// IsTableMap implements BinlogEvent.IsTableMap().
func (ev binlogEvent) IsTableMap() bool {
	return ev.Type() == eTableMapEvent
}

// IsWriteRows implements BinlogEvent.IsWriteRows().
// We do not support v0.
func (ev binlogEvent) IsWriteRows() bool {
	return ev.Type() == eWriteRowsEventV1 ||
		ev.Type() == eWriteRowsEventV2
}

// IsUpdateRows implements BinlogEvent.IsUpdateRows().
// We do not support v0.
func (ev binlogEvent) IsUpdateRows() bool {
	return ev.Type() == eUpdateRowsEventV1 ||
		ev.Type() == eUpdateRowsEventV2
}

// IsDeleteRows implements BinlogEvent.IsDeleteRows().
// We do not support v0.
func (ev binlogEvent) IsDeleteRows() bool {
	return ev.Type() == eDeleteRowsEventV1 ||
		ev.Type() == eDeleteRowsEventV2
}

// Format implements BinlogEvent.Format().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   2         format version
//   50        server version string, 0-padded but not necessarily 0-terminated
//   4         timestamp (same as timestamp header field)
//   1         header length
//   p         (one byte per packet type) event type header lengths
//             Rest was infered from reading source code:
//   1         checksum algorithm
//   4         checksum
func (ev binlogEvent) Format() (f BinlogFormat, err error) {
	// FORMAT_DESCRIPTION_EVENT has a fixed header size of 19
	// because we have to read it before we know the header_length.
	data := ev.Bytes()[19:]

	f.FormatVersion = binary.LittleEndian.Uint16(data[:2])
	if f.FormatVersion != 4 {
		return f, fmt.Errorf("format version = %d, we only support version 4", f.FormatVersion)
	}
	f.ServerVersion = string(bytes.TrimRight(data[2:2+50], "\x00"))
	f.HeaderLength = data[2+50+4]
	if f.HeaderLength < 19 {
		return f, fmt.Errorf("header length = %d, should be >= 19", f.HeaderLength)
	}

	// MySQL/MariaDB 5.6.1+ always adds a 4-byte checksum to the end of a
	// FORMAT_DESCRIPTION_EVENT, regardless of the server setting. The byte
	// immediately before that checksum tells us which checksum algorithm
	// (if any) is used for the rest of the events.
	f.ChecksumAlgorithm = data[len(data)-5]

	f.HeaderSizes = data[2+50+4+1 : len(data)-5]
	return f, nil
}

// Query implements BinlogEvent.Query().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   4         thread_id
//   4         execution time
//   1         length of db_name, not including NULL terminator (X)
//   2         error code
//   2         length of status vars block (Y)
//   Y         status vars block
//   X+1       db_name + NULL terminator
//   L-X-1-Y   SQL statement (no NULL terminator)
func (ev binlogEvent) Query(f BinlogFormat) (query Query, err error) {
	const varsPos = 4 + 4 + 1 + 2 + 2

	data := ev.Bytes()[f.HeaderLength:]

	// length of database name
	dbLen := int(data[4+4])
	// length of status variables block
	varsLen := int(binary.LittleEndian.Uint16(data[4+4+1+2 : 4+4+1+2+2]))

	// position of database name
	dbPos := varsPos + varsLen
	// position of SQL query
	sqlPos := dbPos + dbLen + 1 // +1 for NULL terminator
	if sqlPos > len(data) {
		return query, fmt.Errorf("SQL query position overflows buffer (%v > %v)", sqlPos, len(data))
	}

	// We've checked that the buffer is big enough for sql, so everything before
	// it (db and vars) is in-bounds too.
	query.Database = string(data[dbPos : dbPos+dbLen])
	query.SQL = string(data[sqlPos:])

	// Scan the status vars for ones we care about. This requires us to know the
	// size of every var that comes before the ones we're interested in.
	vars := data[varsPos : varsPos+varsLen]

varsLoop:
	for pos := 0; pos < len(vars); {
		code := vars[pos]
		pos++

		// All codes are optional, but if present they must occur in numerically
		// increasing order (except for 6 which occurs in the place of 2) to allow
		// for backward compatibility.
		switch code {
		case QFlags2Code, QAutoIncrement:
			pos += 4
		case QSQLModeCode:
			pos += 8
		case QCatalog: // Used in MySQL 5.0.0 - 5.0.3
			if pos+1 > len(vars) {
				return query, fmt.Errorf("Q_CATALOG status var overflows buffer (%v + 1 > %v)", pos, len(vars))
			}
			pos += 1 + int(vars[pos]) + 1
		case QCatalogNZCode: // Used in MySQL > 5.0.3 to replace QCatalog
			if pos+1 > len(vars) {
				return query, fmt.Errorf("Q_CATALOG_NZ_CODE status var overflows buffer (%v + 1 > %v)", pos, len(vars))
			}
			pos += 1 + int(vars[pos])
		case QCharsetCode:
			if pos+6 > len(vars) {
				return query, fmt.Errorf("Q_CHARSET_CODE status var overflows buffer (%v + 6 > %v)", pos, len(vars))
			}
			query.Charset = &binlogdatapb.Charset{
				Client: int32(binary.LittleEndian.Uint16(vars[pos : pos+2])),
				Conn:   int32(binary.LittleEndian.Uint16(vars[pos+2 : pos+4])),
				Server: int32(binary.LittleEndian.Uint16(vars[pos+4 : pos+6])),
			}
			pos += 6
		default:
			// If we see something higher than what we're interested in, we can stop.
			break varsLoop
		}
	}

	return query, nil
}

// IntVar implements BinlogEvent.IntVar().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   1         variable ID
//   8         variable value
func (ev binlogEvent) IntVar(f BinlogFormat) (byte, uint64, error) {
	data := ev.Bytes()[f.HeaderLength:]

	typ := data[0]
	if typ != IntVarLastInsertID && typ != IntVarInsertID {
		return 0, 0, fmt.Errorf("invalid IntVar ID: %v", data[0])
	}

	value := binary.LittleEndian.Uint64(data[1 : 1+8])
	return typ, value, nil
}

// Rand implements BinlogEvent.Rand().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   8         seed 1
//   8         seed 2
func (ev binlogEvent) Rand(f BinlogFormat) (seed1 uint64, seed2 uint64, err error) {
	data := ev.Bytes()[f.HeaderLength:]
	seed1 = binary.LittleEndian.Uint64(data[0:8])
	seed2 = binary.LittleEndian.Uint64(data[8 : 8+8])
	return seed1, seed2, nil
}

func (ev binlogEvent) TableID(f BinlogFormat) uint64 {
	typ := ev.Type()
	pos := f.HeaderLength
	if f.HeaderSize(typ) == 6 {
		// Encoded in 4 bytes.
		return uint64(binary.LittleEndian.Uint32(ev[pos : pos+4]))
	}

	// Encoded in 6 bytes.
	return uint64(ev[pos]) |
		uint64(ev[pos+1])<<8 |
		uint64(ev[pos+2])<<16 |
		uint64(ev[pos+3])<<24 |
		uint64(ev[pos+4])<<32 |
		uint64(ev[pos+5])<<40
}

// TableMap implements BinlogEvent.TableMap().
//
// Expected format (L = total length of event data):
//  # bytes   field
//  4/6       table id
//  2         flags
//  1         schema name length sl
//  sl        schema name
//  1         [00]
//  1         table name length tl
//  tl        table name
//  1         [00]
//  <var>     column count cc (var-len encoded)
//  cc        column-def, one byte per column
//  <var>     column-meta-def (var-len encoded string)
//  n         NULL-bitmask, length: (cc + 7) / 8
func (ev binlogEvent) TableMap(f BinlogFormat) (*TableMap, error) {
	data := ev.Bytes()[f.HeaderLength:]

	result := &TableMap{}
	pos := 6
	if f.HeaderSize(eTableMapEvent) == 6 {
		pos = 4
	}
	result.Flags = binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2

	l := int(data[pos])
	result.Database = string(data[pos+1 : pos+1+l])
	pos += 1 + l + 1

	l = int(data[pos])
	result.Name = string(data[pos+1 : pos+1+l])
	pos += 1 + l + 1

	// FIXME(alainjobart) this is varlength encoded.
	columnCount := int(data[pos])

	result.Columns = make([]TableMapColumn, columnCount)
	for i := 0; i < columnCount; i++ {
		result.Columns[i].Type = data[pos+1+i]
	}
	pos += 1 + columnCount

	// FIXME(alainjobart) this is a var-len-string.
	// These are type-specific meta-data per field. Not sure what's in
	// there.
	l = int(data[pos])
	pos += 1 + l

	// A bit array that says if each colum can be NULL.
	nullBitmap, _ := newBitmap(data, pos, columnCount)
	for i := 0; i < columnCount; i++ {
		result.Columns[i].CanBeNull = nullBitmap.Bit(i)
	}

	return result, nil
}

// cellLength returns the new position after the field with the given type is read.
func cellLength(data []byte, pos int, tmc *TableMapColumn) (int, error) {
	switch tmc.Type {
	case TypeTiny:
		return 1, nil
	case TypeShort, TypeYear:
		return 2, nil
	case TypeLong, TypeInt24:
		return 4, nil
	case TypeLongLong:
		return 8, nil
	case TypeTimestamp, TypeDate, TypeTime, TypeDateTime:
		// first byte has the length.
		l := int(data[pos])
		return 1 + l, nil
	case TypeVarchar:
		// Length is encoded in 2 bytes.
		l := int(uint64(data[pos]) |
			uint64(data[pos+1])<<8)
		return 2 + l, nil
	default:
		return 0, fmt.Errorf("Unsupported type %v (data: %v pos: %v)", tmc.Type, data, pos)
	}
}

// FIXME(alainjobart) are the ints signed? It seems Tiny is unsigned,
// but the others are.
func cellData(data []byte, pos int, tmc *TableMapColumn) (string, int, error) {
	switch tmc.Type {
	case TypeTiny:
		return fmt.Sprintf("%v", data[pos]), 1, nil
	case TypeShort, TypeYear:
		val := binary.LittleEndian.Uint16(data[pos : pos+2])
		return fmt.Sprintf("%v", val), 2, nil
	case TypeLong, TypeInt24:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		return fmt.Sprintf("%v", val), 4, nil
	case TypeLongLong:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		return fmt.Sprintf("%v", val), 8, nil
	case TypeTimestamp, TypeDate, TypeTime, TypeDateTime:
		panic(fmt.Errorf("Not yet implemented type %v", tmc.Type))
	case TypeVarchar:
		// Varchar length is two bytes here.
		l := int(uint64(data[pos]) |
			uint64(data[pos+1])<<8)
		return string(data[pos+2 : pos+2+l]), 2 + l, nil
	default:
		return "", 0, fmt.Errorf("Unsupported type %v", tmc.Type)
	}
}

// Rows implements BinlogEvent.TableMap().
//
// Expected format (L = total length of event data):
//  # bytes   field
//  4/6       table id
//  2         flags
//  -- if version == 2
//  2         extra data length edl
//  edl       extra data
//  -- endif
// <var>      number of columns (var-len encoded)
// <var>      identify bitmap
// <var>      data bitmap
// -- for each row
// <var>      null bitmap for identify for present rows
// <var>      values for each identify field
// <var>      null bitmap for data for present rows
// <var>      values for each data field
// --
func (ev binlogEvent) Rows(f BinlogFormat, tm *TableMap) (Rows, error) {
	typ := ev.Type()
	data := ev.Bytes()[f.HeaderLength:]
	hasIdentify := typ == eUpdateRowsEventV1 || typ == eUpdateRowsEventV2 ||
		typ == eDeleteRowsEventV1 || typ == eDeleteRowsEventV2
	hasData := typ == eWriteRowsEventV1 || typ == eWriteRowsEventV2 ||
		typ == eUpdateRowsEventV1 || typ == eUpdateRowsEventV2

	result := Rows{}
	pos := 6
	if f.HeaderSize(typ) == 6 {
		pos = 4
	}
	result.Flags = binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2

	// version=2 have extra data here.
	if typ == eWriteRowsEventV2 || typ == eUpdateRowsEventV2 || typ == eDeleteRowsEventV2 {
		// This extraDataLength contains the 2 bytes length.
		extraDataLength := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += int(extraDataLength)
	}

	// FIXME(alainjobart) this is var len encoded.
	columnCount := int(data[pos])
	pos++

	numIdentifyColumns := 0
	numDataColumns := 0

	if hasIdentify {
		// Bitmap of the columns used for identify.
		result.IdentifyColumns, pos = newBitmap(data, pos, columnCount)
		numIdentifyColumns = result.IdentifyColumns.BitCount()
	}

	if hasData {
		// Bitmap of columns that are present.
		result.DataColumns, pos = newBitmap(data, pos, columnCount)
		numDataColumns = result.DataColumns.BitCount()
	}

	// One row at a time.
	for pos < len(data) {
		row := Row{}

		if hasIdentify {
			// Bitmap of identify columns that are null (amongst the ones that are present).
			row.NullIdentifyColumns, pos = newBitmap(data, pos, numIdentifyColumns)

			// Get the identify values.
			startPos := pos
			valueIndex := 0
			for c := 0; c < columnCount; c++ {
				if !result.IdentifyColumns.Bit(c) {
					// This column is not represented.
					continue
				}

				if row.NullIdentifyColumns.Bit(valueIndex) {
					// This column is represented, but its value is NULL.
					valueIndex++
					continue
				}

				// This column is represented now. We need to skip its length.
				l, err := cellLength(data, pos, &tm.Columns[c])
				if err != nil {
					return result, err
				}
				pos += l
				valueIndex++
			}
			row.Identify = data[startPos:pos]
		}

		if hasData {
			// Bitmap of columns that are null (amongst the ones that are present).
			row.NullColumns, pos = newBitmap(data, pos, numDataColumns)

			// Get the values.
			startPos := pos
			valueIndex := 0
			for c := 0; c < columnCount; c++ {
				if !result.DataColumns.Bit(c) {
					// This column is not represented.
					continue
				}

				if row.NullColumns.Bit(valueIndex) {
					// This column is represented, but its value is NULL.
					valueIndex++
					continue
				}

				// This column is represented now. We need to skip its length.
				l, err := cellLength(data, pos, &tm.Columns[c])
				if err != nil {
					return result, err
				}
				pos += l
				valueIndex++
			}
			row.Data = data[startPos:pos]
		}

		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// StringValues is a helper method to return the string value of all columns in a row in a Row.
func (rs *Rows) StringValues(tm *TableMap, rowIndex int) ([]string, error) {
	var result []string

	valueIndex := 0
	data := rs.Rows[rowIndex].Data
	pos := 0
	for c := 0; c < rs.DataColumns.Count(); c++ {
		if !rs.DataColumns.Bit(c) {
			continue
		}

		if rs.Rows[rowIndex].NullColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			result = append(result, "NULL")
			valueIndex++
			continue
		}

		// We have real data
		value, l, err := cellData(data, pos, &tm.Columns[c])
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		pos += l
		valueIndex++
	}

	return result, nil
}

// StringIdentifies is a helper method to return the string identify of all columns in a row in a Row.
func (rs *Rows) StringIdentifies(tm *TableMap, rowIndex int) ([]string, error) {
	var result []string

	valueIndex := 0
	data := rs.Rows[rowIndex].Identify
	pos := 0
	for c := 0; c < rs.IdentifyColumns.Count(); c++ {
		if !rs.IdentifyColumns.Bit(c) {
			continue
		}

		if rs.Rows[rowIndex].NullIdentifyColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			result = append(result, "NULL")
			valueIndex++
			continue
		}

		// We have real data
		value, l, err := cellData(data, pos, &tm.Columns[c])
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		pos += l
		valueIndex++
	}

	return result, nil
}
