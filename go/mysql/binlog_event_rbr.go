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
	"encoding/binary"

	"vitess.io/vitess/go/mysql/binlog"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TableMap implements BinlogEvent.TableMap().
//
// Expected format (L = total length of event data):
//
//	# bytes   field
//	4/6       table id
//	2         flags
//	1         schema name length sl
//	sl        schema name
//	1         [00]
//	1         table name length tl
//	tl        table name
//	1         [00]
//	<var>     column count cc (var-len encoded)
//	cc        column-def, one byte per column
//	<var>     column-meta-def (var-len encoded string)
//	n         NULL-bitmask, length: (cc + 7) / 8
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

	columnCount, read, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "expected column count at position %v (data=%v)", pos, data)
	}
	pos = read

	result.Types = data[pos : pos+int(columnCount)]
	pos += int(columnCount)

	metaLen, read, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "expected metadata length at position %v (data=%v)", pos, data)
	}
	pos = read

	// Allocate and parse / copy Metadata.
	result.Metadata = make([]uint16, columnCount)
	expectedEnd := pos + int(metaLen)
	for c := uint64(0); c < columnCount; c++ {
		var err error
		result.Metadata[c], pos, err = metadataRead(data, pos, result.Types[c])
		if err != nil {
			return nil, err
		}
	}
	if pos != expectedEnd {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected metadata end: got %v was expecting %v (data=%v)", pos, expectedEnd, data)
	}

	// A bit array that says if each column can be NULL.
	result.CanBeNull, _ = newBitmap(data, pos, int(columnCount))

	return result, nil
}

// metadataLength returns how many bytes are used for metadata, based on a type.
func metadataLength(typ byte) int {
	switch typ {
	case binlog.TypeDecimal, binlog.TypeTiny, binlog.TypeShort, binlog.TypeLong, binlog.TypeNull, binlog.TypeTimestamp, binlog.TypeLongLong, binlog.TypeInt24, binlog.TypeDate, binlog.TypeTime, binlog.TypeDateTime, binlog.TypeYear, binlog.TypeNewDate:
		// No data here.
		return 0

	case binlog.TypeFloat, binlog.TypeDouble, binlog.TypeTimestamp2, binlog.TypeDateTime2, binlog.TypeTime2, binlog.TypeJSON, binlog.TypeTinyBlob, binlog.TypeMediumBlob, binlog.TypeLongBlob, binlog.TypeBlob, binlog.TypeGeometry:
		// One byte.
		return 1

	case binlog.TypeNewDecimal, binlog.TypeEnum, binlog.TypeSet, binlog.TypeString:
		// Two bytes, Big Endian because of crazy encoding.
		return 2

	case binlog.TypeVarchar, binlog.TypeBit, binlog.TypeVarString:
		// Two bytes, Little Endian
		return 2

	default:
		// Unknown type. This is used in tests only, so panic.
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "metadataLength: unhandled data type: %v", typ))
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

	case binlog.TypeDecimal, binlog.TypeTiny, binlog.TypeShort, binlog.TypeLong, binlog.TypeNull, binlog.TypeTimestamp, binlog.TypeLongLong, binlog.TypeInt24, binlog.TypeDate, binlog.TypeTime, binlog.TypeDateTime, binlog.TypeYear, binlog.TypeNewDate:
		// No data here.
		return 0, pos, nil

	case binlog.TypeFloat, binlog.TypeDouble, binlog.TypeTimestamp2, binlog.TypeDateTime2, binlog.TypeTime2, binlog.TypeJSON, binlog.TypeTinyBlob, binlog.TypeMediumBlob, binlog.TypeLongBlob, binlog.TypeBlob, binlog.TypeGeometry:
		// One byte.
		return uint16(data[pos]), pos + 1, nil

	case binlog.TypeNewDecimal, binlog.TypeEnum, binlog.TypeSet, binlog.TypeString:
		// Two bytes, Big Endian because of crazy encoding.
		return uint16(data[pos])<<8 + uint16(data[pos+1]), pos + 2, nil

	case binlog.TypeVarchar, binlog.TypeBit, binlog.TypeVarString:
		// Two bytes, Little Endian
		return uint16(data[pos]) + uint16(data[pos+1])<<8, pos + 2, nil

	default:
		// Unknown types, we can't go on.
		return 0, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "metadataRead: unhandled data type: %v", typ)
	}
}

// metadataWrite writes a single value into the metadata string.
func metadataWrite(data []byte, pos int, typ byte, value uint16) int {
	switch typ {

	case binlog.TypeDecimal, binlog.TypeTiny, binlog.TypeShort, binlog.TypeLong, binlog.TypeNull, binlog.TypeTimestamp, binlog.TypeLongLong, binlog.TypeInt24, binlog.TypeDate, binlog.TypeTime, binlog.TypeDateTime, binlog.TypeYear, binlog.TypeNewDate:
		// No data here.
		return pos

	case binlog.TypeFloat, binlog.TypeDouble, binlog.TypeTimestamp2, binlog.TypeDateTime2, binlog.TypeTime2, binlog.TypeJSON, binlog.TypeTinyBlob, binlog.TypeMediumBlob, binlog.TypeLongBlob, binlog.TypeBlob, binlog.TypeGeometry:
		// One byte.
		data[pos] = byte(value)
		return pos + 1

	case binlog.TypeNewDecimal, binlog.TypeEnum, binlog.TypeSet, binlog.TypeString:
		// Two bytes, Big Endian because of crazy encoding.
		data[pos] = byte(value >> 8)
		data[pos+1] = byte(value)
		return pos + 2

	case binlog.TypeVarchar, binlog.TypeBit, binlog.TypeVarString:
		// Two bytes, Little Endian
		data[pos] = byte(value)
		data[pos+1] = byte(value >> 8)
		return pos + 2

	default:
		// Unknown type. This is used in tests only, so panic.
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "metadataRead: unhandled data type: %v", typ))
	}
}

// Rows implements BinlogEvent.TableMap().
//
// Expected format (L = total length of event data):
//
//	# bytes   field
//	4/6       table id
//	2         flags
//	-- if version == 2
//	2         extra data length edl
//	edl       extra data
//	-- endif
//
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

	columnCount, read, ok := readLenEncInt(data, pos)
	if !ok {
		return result, vterrors.Errorf(vtrpc.Code_INTERNAL, "expected column count at position %v (data=%v)", pos, data)
	}
	pos = read

	numIdentifyColumns := 0
	numDataColumns := 0

	if hasIdentify {
		// Bitmap of the columns used for identify.
		result.IdentifyColumns, pos = newBitmap(data, pos, int(columnCount))
		numIdentifyColumns = result.IdentifyColumns.BitCount()
	}

	if hasData {
		// Bitmap of columns that are present.
		result.DataColumns, pos = newBitmap(data, pos, int(columnCount))
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
			for c := 0; c < int(columnCount); c++ {
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
				l, err := binlog.CellLength(data, pos, tm.Types[c], tm.Metadata[c])
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
			for c := 0; c < int(columnCount); c++ {
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
				l, err := binlog.CellLength(data, pos, tm.Types[c], tm.Metadata[c])
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
		value, l, err := binlog.CellValue(data, pos, tm.Types[c], tm.Metadata[c], &querypb.Field{Type: querypb.Type_UINT64})
		if err != nil {
			return nil, err
		}
		result = append(result, value.ToString())
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
		value, l, err := binlog.CellValue(data, pos, tm.Types[c], tm.Metadata[c], &querypb.Field{Type: querypb.Type_UINT64})
		if err != nil {
			return nil, err
		}
		result = append(result, value.ToString())
		pos += l
		valueIndex++
	}

	return result, nil
}
