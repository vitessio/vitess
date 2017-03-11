package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
	pos++

	result.Types = data[pos : pos+columnCount]
	pos += columnCount

	// FIXME(alainjobart) this is a var-len-string.
	l = int(data[pos])
	pos++

	// Allocate and parse / copy Metadata.
	result.Metadata = make([]uint16, columnCount)
	expectedEnd := pos + l
	for c := 0; c < columnCount; c++ {
		var err error
		result.Metadata[c], pos, err = metadataRead(data, pos, result.Types[c])
		if err != nil {
			return nil, err
		}
	}
	if pos != expectedEnd {
		return nil, fmt.Errorf("unexpected metadata end: got %v was expecting %v (data=%v)", pos, expectedEnd, data)
	}

	// A bit array that says if each colum can be NULL.
	result.CanBeNull, _ = newBitmap(data, pos, columnCount)

	return result, nil
}

// metadataLength returns how many bytes are used for metadata, based on a type.
func metadataLength(typ byte) int {
	switch typ {
	case TypeDecimal, TypeTiny, TypeShort, TypeLong, TypeNull, TypeTimestamp, TypeLongLong, TypeInt24, TypeDate, TypeTime, TypeDateTime, TypeYear, TypeNewDate, TypeVarString:
		// No data here.
		return 0

	case TypeFloat, TypeDouble, TypeTimestamp2, TypeDateTime2, TypeTime2, TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeGeometry:
		// One byte.
		return 1

	case TypeNewDecimal, TypeEnum, TypeSet, TypeString:
		// Two bytes, Big Endian because of crazy encoding.
		return 2

	case TypeVarchar, TypeBit:
		// Two bytes, Little Endian
		return 2

	default:
		// Unknown type. This is used in tests only, so panic.
		panic(fmt.Errorf("metadataLength: unhandled data type: %v", typ))
	}
}

// metadataTotalLength returns the total size of the metadata for an
// array of types.
func metadataTotalLength(types []byte) int {
	sum := 0
	for _, t := range types {
		sum += metadataLength(t)
	}
	return sum
}

// metadataRead reads a single value from the metadata string.
func metadataRead(data []byte, pos int, typ byte) (uint16, int, error) {
	switch typ {

	case TypeDecimal, TypeTiny, TypeShort, TypeLong, TypeNull, TypeTimestamp, TypeLongLong, TypeInt24, TypeDate, TypeTime, TypeDateTime, TypeYear, TypeNewDate, TypeVarString:
		// No data here.
		return 0, pos, nil

	case TypeFloat, TypeDouble, TypeTimestamp2, TypeDateTime2, TypeTime2, TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeGeometry:
		// One byte.
		return uint16(data[pos]), pos + 1, nil

	case TypeNewDecimal, TypeEnum, TypeSet, TypeString:
		// Two bytes, Big Endian because of crazy encoding.
		return uint16(data[pos])<<8 + uint16(data[pos+1]), pos + 2, nil

	case TypeVarchar, TypeBit:
		// Two bytes, Little Endian
		return uint16(data[pos]) + uint16(data[pos+1])<<8, pos + 2, nil

	default:
		// Unknown types, we can't go on.
		return 0, 0, fmt.Errorf("metadataRead: unhandled data type: %v", typ)
	}
}

// metadataWrite writes a single value into the metadata string.
func metadataWrite(data []byte, pos int, typ byte, value uint16) int {
	switch typ {

	case TypeDecimal, TypeTiny, TypeShort, TypeLong, TypeNull, TypeTimestamp, TypeLongLong, TypeInt24, TypeDate, TypeTime, TypeDateTime, TypeYear, TypeNewDate, TypeVarString:
		// No data here.
		return pos

	case TypeFloat, TypeDouble, TypeTimestamp2, TypeDateTime2, TypeTime2, TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeGeometry:
		// One byte.
		data[pos] = byte(value)
		return pos + 1

	case TypeNewDecimal, TypeEnum, TypeSet, TypeString:
		// Two bytes, Big Endian because of crazy encoding.
		data[pos] = byte(value >> 8)
		data[pos+1] = byte(value)
		return pos + 2

	case TypeVarchar, TypeBit:
		// Two bytes, Little Endian
		data[pos] = byte(value)
		data[pos+1] = byte(value >> 8)
		return pos + 2

	default:
		// Unknown type. This is used in tests only, so panic.
		panic(fmt.Errorf("metadataRead: unhandled data type: %v", typ))
	}
}

// cellLength returns the new position after the field with the given
// type is read.
func cellLength(data []byte, pos int, typ byte, metadata uint16) (int, error) {
	switch typ {
	case TypeNull:
		return 0, nil
	case TypeTiny, TypeYear:
		return 1, nil
	case TypeShort:
		return 2, nil
	case TypeInt24:
		return 3, nil
	case TypeLong, TypeTimestamp:
		return 4, nil
	case TypeLongLong:
		return 8, nil
	case TypeDate, TypeNewDate:
		return 3, nil
	case TypeTime:
		return 4, nil
	case TypeDateTime:
		return 8, nil
	case TypeVarchar:
		// Length is encoded in 1 or 2 bytes.
		if metadata > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return l + 2, nil
		}
		l := int(data[pos])
		return l + 1, nil
	case TypeBit:
		// bitmap length is in metadata, as:
		// upper 8 bits: bytes length
		// lower 8 bits: bit length
		nbits := ((metadata >> 8) * 8) + (metadata & 0xFF)
		return (int(nbits) + 7) / 8, nil
	case TypeTimestamp2:
		// metadata has number of decimals. One byte encodes
		// two decimals.
		return 4 + (int(metadata)+1)/2, nil
	case TypeDateTime2:
		// metadata has number of decimals. One byte encodes
		// two decimals.
		return 5 + (int(metadata)+1)/2, nil
	case TypeTime2:
		// metadata has number of decimals. One byte encodes
		// two decimals.
		return 3 + (int(metadata)+1)/2, nil

	default:
		return 0, fmt.Errorf("Unsupported type %v (data: %v pos: %v)", typ, data, pos)
	}
}

// CellValue returns the data for a cell as a sqltypes.Value, and how
// many bytes it takes. It only uses the querypb.Type value for the
// signed flag.
func CellValue(data []byte, pos int, typ byte, metadata uint16, styp querypb.Type) (sqltypes.Value, int, error) {
	switch typ {
	case TypeTiny:
		if sqltypes.IsSigned(styp) {
			return sqltypes.MakeTrusted(querypb.Type_INT8,
				strconv.AppendInt(nil, int64(int8(data[pos])), 10)), 1, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT8,
			strconv.AppendUint(nil, uint64(data[pos]), 10)), 1, nil
	case TypeYear:
		return sqltypes.MakeTrusted(querypb.Type_YEAR,
			strconv.AppendUint(nil, uint64(data[pos])+1900, 10)), 1, nil
	case TypeShort:
		val := binary.LittleEndian.Uint16(data[pos : pos+2])
		if sqltypes.IsSigned(styp) {
			return sqltypes.MakeTrusted(querypb.Type_INT16,
				strconv.AppendInt(nil, int64(int16(val)), 10)), 2, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT16,
			strconv.AppendUint(nil, uint64(val), 10)), 2, nil
	case TypeInt24:
		if sqltypes.IsSigned(styp) && data[pos+2]&128 > 0 {
			// Negative number, have to extend the sign.
			val := int32(uint32(data[pos]) +
				uint32(data[pos+1])<<8 +
				uint32(data[pos+2])<<16 +
				uint32(255)<<24)
			return sqltypes.MakeTrusted(querypb.Type_INT24,
				strconv.AppendInt(nil, int64(val), 10)), 3, nil
		}
		// Positive number.
		val := uint64(data[pos]) +
			uint64(data[pos+1])<<8 +
			uint64(data[pos+2])<<16
		return sqltypes.MakeTrusted(querypb.Type_UINT24,
			strconv.AppendUint(nil, val, 10)), 3, nil
	case TypeLong:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		if sqltypes.IsSigned(styp) {
			return sqltypes.MakeTrusted(querypb.Type_INT32,
				strconv.AppendInt(nil, int64(int32(val)), 10)), 4, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT32,
			strconv.AppendUint(nil, uint64(val), 10)), 4, nil
	case TypeTimestamp:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
			strconv.AppendUint(nil, uint64(val), 10)), 4, nil
	case TypeLongLong:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		if sqltypes.IsSigned(styp) {
			return sqltypes.MakeTrusted(querypb.Type_INT64,
				strconv.AppendInt(nil, int64(val), 10)), 8, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT64,
			strconv.AppendUint(nil, val, 10)), 8, nil
	case TypeDate, TypeNewDate:
		val := uint32(data[pos]) +
			uint32(data[pos+1])<<8 +
			uint32(data[pos+2])<<16
		day := val & 31
		month := val >> 5 & 15
		year := val >> 9
		return sqltypes.MakeTrusted(querypb.Type_DATE,
			[]byte(fmt.Sprintf("%04d-%02d-%02d", year, month, day))), 3, nil
	case TypeTime:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		hour := val / 10000
		minute := (val % 10000) / 100
		second := val % 100
		return sqltypes.MakeTrusted(querypb.Type_TIME,
			[]byte(fmt.Sprintf("%02d:%02d:%02d", hour, minute, second))), 4, nil
	case TypeDateTime:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		d := val / 1000000
		t := val % 1000000
		year := d / 10000
		month := (d % 10000) / 100
		day := d % 100
		hour := t / 10000
		minute := (t % 10000) / 100
		second := t % 100
		return sqltypes.MakeTrusted(querypb.Type_DATETIME,
			[]byte(fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second))), 8, nil
	case TypeVarchar:
		// Length is encoded in 1 or 2 bytes.
		if metadata > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return sqltypes.MakeTrusted(querypb.Type_VARCHAR,
				data[pos+2:pos+2+l]), l + 2, nil
		}
		l := int(data[pos])
		return sqltypes.MakeTrusted(querypb.Type_VARCHAR,
			data[pos+1:pos+1+l]), l + 1, nil
	case TypeBit:
		// The contents is just the bytes, quoted.
		nbits := ((metadata >> 8) * 8) + (metadata & 0xFF)
		l := (int(nbits) + 7) / 8
		return sqltypes.MakeTrusted(querypb.Type_BIT,
			data[pos:pos+l]), l, nil
	case TypeTimestamp2:
		second := binary.LittleEndian.Uint32(data[pos : pos+4])
		switch metadata {
		case 1:
			decimals := int(data[pos+4])
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				[]byte(fmt.Sprintf("%v.%01d", second, decimals))), 5, nil
		case 2:
			decimals := int(data[pos+4])
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				[]byte(fmt.Sprintf("%v.%02d", second, decimals))), 5, nil
		case 3:
			decimals := int(data[pos+4]) +
				int(data[pos+5])<<8
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				[]byte(fmt.Sprintf("%v.%03d", second, decimals))), 6, nil
		case 4:
			decimals := int(data[pos+4]) +
				int(data[pos+5])<<8
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				[]byte(fmt.Sprintf("%v.%04d", second, decimals))), 6, nil
		case 5:
			decimals := int(data[pos+4]) +
				int(data[pos+5])<<8 +
				int(data[pos+6])<<16
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				[]byte(fmt.Sprintf("%v.%05d", second, decimals))), 7, nil
		case 6:
			decimals := int(data[pos+4]) +
				int(data[pos+5])<<8 +
				int(data[pos+6])<<16
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				[]byte(fmt.Sprintf("%v.%.6d", second, decimals))), 7, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
			strconv.AppendUint(nil, uint64(second), 10)), 4, nil
	case TypeDateTime2:
		ymdhms := (uint64(data[pos]) |
			uint64(data[pos+1])<<8 |
			uint64(data[pos+2])<<16 |
			uint64(data[pos+3])<<24 |
			uint64(data[pos+4])<<32) - uint64(0x8000000000)
		ymd := ymdhms >> 17
		ym := ymd >> 5
		hms := ymdhms % (1 << 17)

		day := ymd % (1 << 5)
		month := ym % 13
		year := ym / 13

		second := hms % (1 << 6)
		minute := (hms >> 6) % (1 << 6)
		hour := hms >> 12

		datetime := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)

		switch metadata {
		case 1:
			decimals := int(data[pos+5])
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				[]byte(fmt.Sprintf("%v.%01d", datetime, decimals))), 6, nil
		case 2:
			decimals := int(data[pos+5])
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				[]byte(fmt.Sprintf("%v.%02d", datetime, decimals))), 6, nil
		case 3:
			decimals := int(data[pos+5]) +
				int(data[pos+6])<<8
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				[]byte(fmt.Sprintf("%v.%03d", datetime, decimals))), 7, nil
		case 4:
			decimals := int(data[pos+5]) +
				int(data[pos+6])<<8
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				[]byte(fmt.Sprintf("%v.%04d", datetime, decimals))), 7, nil
		case 5:
			decimals := int(data[pos+5]) +
				int(data[pos+6])<<8 +
				int(data[pos+7])<<16
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				[]byte(fmt.Sprintf("%v.%05d", datetime, decimals))), 8, nil
		case 6:
			decimals := int(data[pos+5]) +
				int(data[pos+6])<<8 +
				int(data[pos+7])<<16
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				[]byte(fmt.Sprintf("%v.%.6d", datetime, decimals))), 8, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_DATETIME,
			[]byte(datetime)), 5, nil
	case TypeTime2:
		hms := (int64(data[pos]) |
			int64(data[pos+1])<<8 |
			int64(data[pos+2])<<16) - 0x800000
		sign := ""
		if hms < 0 {
			hms = -hms
			sign = "-"
		}

		fracStr := ""
		switch metadata {
		case 1:
			frac := int(data[pos+3])
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x100 - frac
			}
			fracStr = fmt.Sprintf(".%.1d", frac/10)
		case 2:
			frac := int(data[pos+3])
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x100 - frac
			}
			fracStr = fmt.Sprintf(".%.2d", frac)
		case 3:
			frac := int(data[pos+3]) |
				int(data[pos+4])<<8
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x10000 - frac
			}
			fracStr = fmt.Sprintf(".%.3d", frac/10)
		case 4:
			frac := int(data[pos+3]) |
				int(data[pos+4])<<8
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x10000 - frac
			}
			fracStr = fmt.Sprintf(".%.4d", frac)
		case 5:
			frac := int(data[pos+3]) |
				int(data[pos+4])<<8 |
				int(data[pos+5])<<16
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x1000000 - frac
			}
			fracStr = fmt.Sprintf(".%.5d", frac/10)
		case 6:
			frac := int(data[pos+3]) |
				int(data[pos+4])<<8 |
				int(data[pos+5])<<16
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x1000000 - frac
			}
			fracStr = fmt.Sprintf(".%.6d", frac)
		}

		hour := (hms >> 12) % (1 << 10)
		minute := (hms >> 6) % (1 << 6)
		second := hms % (1 << 6)
		return sqltypes.MakeTrusted(querypb.Type_TIME,
			[]byte(fmt.Sprintf("%v%02d:%02d:%02d%v", sign, hour, minute, second, fracStr))), 3 + (int(metadata)+1)/2, nil

	default:
		return sqltypes.NULL, 0, fmt.Errorf("Unsupported type %v", typ)
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
				l, err := cellLength(data, pos, tm.Types[c], tm.Metadata[c])
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
				l, err := cellLength(data, pos, tm.Types[c], tm.Metadata[c])
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

// StringValuesForTests is a helper method to return the string value
// of all columns in a row in a Row. Only use it in tests, as the
// returned values cannot be interpreted correctly without the schema.
// We assume everything is unsigned in this method.
func (rs *Rows) StringValuesForTests(tm *TableMap, rowIndex int) ([]string, error) {
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
		value, l, err := CellValue(data, pos, tm.Types[c], tm.Metadata[c], querypb.Type_UINT64)
		if err != nil {
			return nil, err
		}
		result = append(result, value.String())
		pos += l
		valueIndex++
	}

	return result, nil
}

// StringIdentifiesForTests is a helper method to return the string
// identify of all columns in a row in a Row. Only use it in tests, as the
// returned values cannot be interpreted correctly without the schema.
// We assume everything is unsigned in this method.
func (rs *Rows) StringIdentifiesForTests(tm *TableMap, rowIndex int) ([]string, error) {
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
		value, l, err := CellValue(data, pos, tm.Types[c], tm.Metadata[c], querypb.Type_UINT64)
		if err != nil {
			return nil, err
		}
		result = append(result, value.String())
		pos += l
		valueIndex++
	}

	return result, nil
}
