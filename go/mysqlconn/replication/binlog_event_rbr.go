package replication

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

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
