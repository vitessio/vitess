/*
Copyright 2023 The Vitess Authors.

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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// ZeroTimestamp is the special value 0 for a timestamp.
var ZeroTimestamp = []byte("0000-00-00 00:00:00")

var dig2bytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// CellLength returns the new position after the field with the given
// type is read.
func CellLength(data []byte, pos int, typ byte, metadata uint16) (int, error) {
	switch typ {
	case TypeNull:
		return 0, nil
	case TypeTiny, TypeYear:
		return 1, nil
	case TypeShort:
		return 2, nil
	case TypeInt24:
		return 3, nil
	case TypeLong, TypeFloat, TypeTimestamp:
		return 4, nil
	case TypeLongLong, TypeDouble:
		return 8, nil
	case TypeDate, TypeTime, TypeNewDate:
		return 3, nil
	case TypeDateTime:
		return 8, nil
	case TypeVarchar, TypeVarString:
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
	case TypeNewDecimal:
		precision := int(metadata >> 8)
		scale := int(metadata & 0xff)
		// Example:
		//   NNNNNNNNNNNN.MMMMMM
		//     12 bytes     6 bytes
		// precision is 18
		// scale is 6
		// storage is done by groups of 9 digits:
		// - 32 bits are used to store groups of 9 digits.
		// - any leftover digit is stored in:
		//   - 1 byte for 1 and 2 digits
		//   - 2 bytes for 3 and 4 digits
		//   - 3 bytes for 5 and 6 digits
		//   - 4 bytes for 7 and 8 digits (would also work for 9)
		// both sides of the dot are stored separately.
		// In this example, we'd have:
		// - 2 bytes to store the first 3 full digits.
		// - 4 bytes to store the next 9 full digits.
		// - 3 bytes to store the 6 fractional digits.
		intg := precision - scale
		intg0 := intg / 9
		frac0 := scale / 9
		intg0x := intg - intg0*9
		frac0x := scale - frac0*9
		return intg0*4 + dig2bytes[intg0x] + frac0*4 + dig2bytes[frac0x], nil
	case TypeEnum, TypeSet:
		return int(metadata & 0xff), nil
	case TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob, TypeGeometry:
		// Of the Blobs, only TypeBlob is used in binary logs,
		// but supports others just in case.
		switch metadata {
		case 1:
			return 1 + int(uint32(data[pos])), nil
		case 2:
			return 2 + int(uint32(data[pos])|
				uint32(data[pos+1])<<8), nil
		case 3:
			return 3 + int(uint32(data[pos])|
				uint32(data[pos+1])<<8|
				uint32(data[pos+2])<<16), nil
		case 4:
			return 4 + int(uint32(data[pos])|
				uint32(data[pos+1])<<8|
				uint32(data[pos+2])<<16|
				uint32(data[pos+3])<<24), nil
		default:
			return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported blob/geometry metadata value %v (data: %v pos: %v)", metadata, data, pos)
		}
	case TypeString:
		// This may do String, Enum, and Set. The type is in
		// metadata. If it's a string, then there will be more bits.
		// This will give us the maximum length of the field.
		t := metadata >> 8
		if t == TypeEnum || t == TypeSet {
			return int(metadata & 0xff), nil
		}
		max := int((((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff))
		// Length is encoded in 1 or 2 bytes.
		if max > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return l + 2, nil
		}
		l := int(data[pos])
		return l + 1, nil

	default:
		return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported type %v (data: %v pos: %v)", typ, data, pos)
	}
}

// printTimestamp is a helper method to append a timestamp into a bytes.Buffer,
// and return the Buffer.
func printTimestamp(v uint32) *bytes.Buffer {
	if v == 0 {
		return bytes.NewBuffer(ZeroTimestamp)
	}

	t := time.Unix(int64(v), 0).UTC()
	year, month, day := t.Date()
	hour, minute, second := t.Clock()

	result := &bytes.Buffer{}
	fmt.Fprintf(result, "%04d-%02d-%02d %02d:%02d:%02d", year, int(month), day, hour, minute, second)
	return result
}

// CellValue returns the data for a cell as a sqltypes.Value, and how
// many bytes it takes. It uses source type in querypb.Type and vitess type
// byte to determine general shared aspects of types and the querypb.Field to
// determine other info specifically about its underlying column (SQL column
// type, column length, charset, etc)
func CellValue(data []byte, pos int, typ byte, metadata uint16, field *querypb.Field) (sqltypes.Value, int, error) {
	switch typ {
	case TypeTiny:
		if sqltypes.IsSigned(field.Type) {
			return sqltypes.MakeTrusted(querypb.Type_INT8,
				strconv.AppendInt(nil, int64(int8(data[pos])), 10)), 1, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT8,
			strconv.AppendUint(nil, uint64(data[pos]), 10)), 1, nil
	case TypeYear:
		val := data[pos]
		if val == 0 {
			return sqltypes.MakeTrusted(querypb.Type_YEAR,
				[]byte{'0', '0', '0', '0'}), 1, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_YEAR,
			strconv.AppendUint(nil, uint64(data[pos])+1900, 10)), 1, nil
	case TypeShort:
		val := binary.LittleEndian.Uint16(data[pos : pos+2])
		if sqltypes.IsSigned(field.Type) {
			return sqltypes.MakeTrusted(querypb.Type_INT16,
				strconv.AppendInt(nil, int64(int16(val)), 10)), 2, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT16,
			strconv.AppendUint(nil, uint64(val), 10)), 2, nil
	case TypeInt24:
		if sqltypes.IsSigned(field.Type) && data[pos+2]&128 > 0 {
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
		if sqltypes.IsSigned(field.Type) {
			return sqltypes.MakeTrusted(querypb.Type_INT32,
				strconv.AppendInt(nil, int64(int32(val)), 10)), 4, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_UINT32,
			strconv.AppendUint(nil, uint64(val), 10)), 4, nil
	case TypeFloat:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		fval := math.Float32frombits(val)
		return sqltypes.MakeTrusted(querypb.Type_FLOAT32,
			strconv.AppendFloat(nil, float64(fval), 'E', -1, 32)), 4, nil
	case TypeDouble:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		fval := math.Float64frombits(val)
		return sqltypes.MakeTrusted(querypb.Type_FLOAT64,
			strconv.AppendFloat(nil, fval, 'E', -1, 64)), 8, nil
	case TypeTimestamp:
		val := binary.LittleEndian.Uint32(data[pos : pos+4])
		txt := printTimestamp(val)
		return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
			txt.Bytes()), 4, nil
	case TypeLongLong:
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		if sqltypes.IsSigned(field.Type) {
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
		var hour, minute, second int32
		if data[pos+2]&128 > 0 {
			// Negative number, have to extend the sign.
			val := int32(uint32(data[pos]) +
				uint32(data[pos+1])<<8 +
				uint32(data[pos+2])<<16 +
				uint32(255)<<24)
			hour = val / 10000
			minute = -((val % 10000) / 100)
			second = -(val % 100)
		} else {
			val := int32(data[pos]) +
				int32(data[pos+1])<<8 +
				int32(data[pos+2])<<16
			hour = val / 10000
			minute = (val % 10000) / 100
			second = val % 100
		}
		return sqltypes.MakeTrusted(querypb.Type_TIME,
			[]byte(fmt.Sprintf("%02d:%02d:%02d", hour, minute, second))), 3, nil
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
	case TypeVarchar, TypeVarString:
		// We trust that typ is compatible with the field.Type
		// Length is encoded in 1 or 2 bytes.
		typeToUse := querypb.Type_VARCHAR
		if field.Type == querypb.Type_VARBINARY || field.Type == querypb.Type_BINARY || field.Type == querypb.Type_BLOB {
			typeToUse = field.Type
		}
		if metadata > 255 {
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return sqltypes.MakeTrusted(typeToUse,
				data[pos+2:pos+2+l]), l + 2, nil
		}
		l := int(data[pos])
		return sqltypes.MakeTrusted(typeToUse,
			data[pos+1:pos+1+l]), l + 1, nil
	case TypeBit:
		// The contents is just the bytes, quoted.
		nbits := ((metadata >> 8) * 8) + (metadata & 0xFF)
		l := (int(nbits) + 7) / 8
		return sqltypes.MakeTrusted(querypb.Type_BIT,
			data[pos:pos+l]), l, nil
	case TypeTimestamp2:
		second := binary.BigEndian.Uint32(data[pos : pos+4])
		txt := printTimestamp(second)
		switch metadata {
		case 1:
			decimals := int(data[pos+4])
			fmt.Fprintf(txt, ".%01d", decimals/10)
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				txt.Bytes()), 5, nil
		case 2:
			decimals := int(data[pos+4])
			fmt.Fprintf(txt, ".%02d", decimals)
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				txt.Bytes()), 5, nil
		case 3:
			decimals := int(data[pos+4])<<8 +
				int(data[pos+5])
			fmt.Fprintf(txt, ".%03d", decimals/10)
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				txt.Bytes()), 6, nil
		case 4:
			decimals := int(data[pos+4])<<8 +
				int(data[pos+5])
			fmt.Fprintf(txt, ".%04d", decimals)
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				txt.Bytes()), 6, nil
		case 5:
			decimals := int(data[pos+4])<<16 +
				int(data[pos+5])<<8 +
				int(data[pos+6])
			fmt.Fprintf(txt, ".%05d", decimals/10)
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				txt.Bytes()), 7, nil
		case 6:
			decimals := int(data[pos+4])<<16 +
				int(data[pos+5])<<8 +
				int(data[pos+6])
			fmt.Fprintf(txt, ".%06d", decimals)
			return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
				txt.Bytes()), 7, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_TIMESTAMP,
			txt.Bytes()), 4, nil
	case TypeDateTime2:
		ymdhms := (uint64(data[pos])<<32 |
			uint64(data[pos+1])<<24 |
			uint64(data[pos+2])<<16 |
			uint64(data[pos+3])<<8 |
			uint64(data[pos+4])) - uint64(0x8000000000)
		ymd := ymdhms >> 17
		ym := ymd >> 5
		hms := ymdhms % (1 << 17)

		day := ymd % (1 << 5)
		month := ym % 13
		year := ym / 13

		second := hms % (1 << 6)
		minute := (hms >> 6) % (1 << 6)
		hour := hms >> 12

		txt := &bytes.Buffer{}
		fmt.Fprintf(txt, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)

		switch metadata {
		case 1:
			decimals := int(data[pos+5])
			fmt.Fprintf(txt, ".%01d", decimals/10)
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				txt.Bytes()), 6, nil
		case 2:
			decimals := int(data[pos+5])
			fmt.Fprintf(txt, ".%02d", decimals)
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				txt.Bytes()), 6, nil
		case 3:
			decimals := int(data[pos+5])<<8 +
				int(data[pos+6])
			fmt.Fprintf(txt, ".%03d", decimals/10)
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				txt.Bytes()), 7, nil
		case 4:
			decimals := int(data[pos+5])<<8 +
				int(data[pos+6])
			fmt.Fprintf(txt, ".%04d", decimals)
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				txt.Bytes()), 7, nil
		case 5:
			decimals := int(data[pos+5])<<16 +
				int(data[pos+6])<<8 +
				int(data[pos+7])
			fmt.Fprintf(txt, ".%05d", decimals/10)
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				txt.Bytes()), 8, nil
		case 6:
			decimals := int(data[pos+5])<<16 +
				int(data[pos+6])<<8 +
				int(data[pos+7])
			fmt.Fprintf(txt, ".%06d", decimals)
			return sqltypes.MakeTrusted(querypb.Type_DATETIME,
				txt.Bytes()), 8, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_DATETIME,
			txt.Bytes()), 5, nil
	case TypeTime2:
		hms := (int64(data[pos])<<16 |
			int64(data[pos+1])<<8 |
			int64(data[pos+2])) - 0x800000
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
			frac := int(data[pos+3])<<8 |
				int(data[pos+4])
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x10000 - frac
			}
			fracStr = fmt.Sprintf(".%.3d", frac/10)
		case 4:
			frac := int(data[pos+3])<<8 |
				int(data[pos+4])
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x10000 - frac
			}
			fracStr = fmt.Sprintf(".%.4d", frac)
		case 5:
			frac := int(data[pos+3])<<16 |
				int(data[pos+4])<<8 |
				int(data[pos+5])
			if sign == "-" && frac != 0 {
				hms--
				frac = 0x1000000 - frac
			}
			fracStr = fmt.Sprintf(".%.5d", frac/10)
		case 6:
			frac := int(data[pos+3])<<16 |
				int(data[pos+4])<<8 |
				int(data[pos+5])
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

	case TypeNewDecimal:
		precision := int(metadata >> 8) // total digits number
		scale := int(metadata & 0xff)   // number of fractional digits
		intg := precision - scale       // number of full digits
		intg0 := intg / 9               // number of 32-bits digits
		intg0x := intg - intg0*9        // leftover full digits
		frac0 := scale / 9              // number of 32 bits fractionals
		frac0x := scale - frac0*9       // leftover fractionals

		l := intg0*4 + dig2bytes[intg0x] + frac0*4 + dig2bytes[frac0x]

		// Copy the data so we can change it. Otherwise
		// decoding is just too hard.
		d := make([]byte, l)
		copy(d, data[pos:pos+l])

		txt := &bytes.Buffer{}

		isNegative := (d[0] & 0x80) == 0
		d[0] ^= 0x80 // First bit is inverted.
		if isNegative {
			// Negative numbers are just inverted bytes.
			txt.WriteByte('-')
			for i := range d {
				d[i] ^= 0xff
			}
		}

		// first we have the leftover full digits
		var val uint32
		switch dig2bytes[intg0x] {
		case 0:
			// nothing to do
		case 1:
			// one byte, up to two digits
			val = uint32(d[0])
		case 2:
			// two bytes, up to 4 digits
			val = uint32(d[0])<<8 +
				uint32(d[1])
		case 3:
			// 3 bytes, up to 6 digits
			val = uint32(d[0])<<16 +
				uint32(d[1])<<8 +
				uint32(d[2])
		case 4:
			// 4 bytes, up to 8 digits (9 digits would be a full)
			val = uint32(d[0])<<24 +
				uint32(d[1])<<16 +
				uint32(d[2])<<8 +
				uint32(d[3])
		}
		pos = dig2bytes[intg0x]
		if val > 0 {
			txt.Write(strconv.AppendUint(nil, uint64(val), 10))
		}

		// now the full digits, 32 bits each, 9 digits
		for i := 0; i < intg0; i++ {
			val = binary.BigEndian.Uint32(d[pos : pos+4])
			fmt.Fprintf(txt, "%09d", val)
			pos += 4
		}

		// now see if we have a fraction
		if scale == 0 {
			// When the field is a DECIMAL using a scale of 0, e.g.
			// DECIMAL(5,0), a binlogged value of 0 is almost treated
			// like the NULL byte and we get a 0 byte length value.
			// In this case let's return the correct value of 0.
			if txt.Len() == 0 {
				txt.WriteRune('0')
			}

			return sqltypes.MakeTrusted(querypb.Type_DECIMAL,
				txt.Bytes()), l, nil
		}
		txt.WriteByte('.')

		// now the full fractional digits
		for i := 0; i < frac0; i++ {
			val = binary.BigEndian.Uint32(d[pos : pos+4])
			fmt.Fprintf(txt, "%09d", val)
			pos += 4
		}

		// then the partial fractional digits
		switch dig2bytes[frac0x] {
		case 0:
			// Nothing to do
			return sqltypes.MakeTrusted(querypb.Type_DECIMAL,
				txt.Bytes()), l, nil
		case 1:
			// one byte, 1 or 2 digits
			val = uint32(d[pos])
			if frac0x == 1 {
				fmt.Fprintf(txt, "%1d", val)
			} else {
				fmt.Fprintf(txt, "%02d", val)
			}
		case 2:
			// two bytes, 3 or 4 digits
			val = uint32(d[pos])<<8 +
				uint32(d[pos+1])
			if frac0x == 3 {
				fmt.Fprintf(txt, "%03d", val)
			} else {
				fmt.Fprintf(txt, "%04d", val)
			}
		case 3:
			// 3 bytes, 5 or 6 digits
			val = uint32(d[pos])<<16 +
				uint32(d[pos+1])<<8 +
				uint32(d[pos+2])
			if frac0x == 5 {
				fmt.Fprintf(txt, "%05d", val)
			} else {
				fmt.Fprintf(txt, "%06d", val)
			}
		case 4:
			// 4 bytes, 7 or 8 digits (9 digits would be a full)
			val = uint32(d[pos])<<24 +
				uint32(d[pos+1])<<16 +
				uint32(d[pos+2])<<8 +
				uint32(d[pos+3])
			if frac0x == 7 {
				fmt.Fprintf(txt, "%07d", val)
			} else {
				fmt.Fprintf(txt, "%08d", val)
			}
		}

		// remove preceding 0s from the integral part, otherwise we get "000000000001.23" instead of "1.23"
		trimPrecedingZeroes := func(b []byte) []byte {
			s := string(b)
			isNegative := false
			if s[0] == '-' {
				isNegative = true
				s = s[1:]
			}
			s = strings.TrimLeft(s, "0")
			if isNegative {
				s = fmt.Sprintf("-%s", s)
			}
			return []byte(s)
		}
		return sqltypes.MakeTrusted(querypb.Type_DECIMAL, trimPrecedingZeroes(txt.Bytes())), l, nil

	case TypeEnum:
		switch metadata & 0xff {
		case 1:
			// One byte storage.
			return sqltypes.MakeTrusted(querypb.Type_ENUM,
				strconv.AppendUint(nil, uint64(data[pos]), 10)), 1, nil
		case 2:
			// Two bytes storage.
			val := binary.LittleEndian.Uint16(data[pos : pos+2])
			return sqltypes.MakeTrusted(querypb.Type_ENUM,
				strconv.AppendUint(nil, uint64(val), 10)), 2, nil
		default:
			return sqltypes.NULL, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected enum size: %v", metadata&0xff)
		}

	case TypeSet:
		l := int(metadata & 0xff)
		return sqltypes.MakeTrusted(querypb.Type_SET,
			data[pos:pos+l]), l, nil

	case TypeJSON, TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob:
		// Only TypeBlob is used in binary logs,
		// but supports others just in case.
		l := 0
		switch metadata {
		case 1:
			l = int(uint32(data[pos]))
		case 2:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8)
		case 3:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16)
		case 4:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16 |
				uint32(data[pos+3])<<24)
		default:
			return sqltypes.NULL, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported blob metadata value %v (data: %v pos: %v)", metadata, data, pos)
		}
		pos += int(metadata)

		// For JSON, we parse the data, and emit SQL.
		if typ == TypeJSON {
			var err error
			jsonData := data[pos : pos+l]
			jsonVal, err := ParseBinaryJSON(jsonData)
			if err != nil {
				panic(err)
			}
			d := jsonVal.MarshalTo(nil)
			return sqltypes.MakeTrusted(sqltypes.Expression,
				d), l + int(metadata), nil
		}

		return sqltypes.MakeTrusted(querypb.Type_VARBINARY,
			data[pos:pos+l]), l + int(metadata), nil

	case TypeString:
		// This may do String, Enum, and Set. The type is in
		// metadata. If it's a string, then there will be more bits.
		t := metadata >> 8
		if t == TypeEnum {
			// We don't know the string values. So just use the
			// numbers.
			switch metadata & 0xff {
			case 1:
				// One byte storage.
				return sqltypes.MakeTrusted(querypb.Type_UINT8,
					strconv.AppendUint(nil, uint64(data[pos]), 10)), 1, nil
			case 2:
				// Two bytes storage.
				val := binary.LittleEndian.Uint16(data[pos : pos+2])
				return sqltypes.MakeTrusted(querypb.Type_UINT16,
					strconv.AppendUint(nil, uint64(val), 10)), 2, nil
			default:
				return sqltypes.NULL, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected enum size: %v", metadata&0xff)
			}
		}
		if t == TypeSet {
			// We don't know the set values. So just use the
			// numbers.
			l := int(metadata & 0xff)
			var val uint64
			for i := 0; i < l; i++ {
				val += uint64(data[pos+i]) << (uint(i) * 8)
			}
			return sqltypes.MakeTrusted(querypb.Type_UINT64,
				strconv.AppendUint(nil, uint64(val), 10)), l, nil
		}
		// This is a real string. The length is weird.
		max := int((((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff))
		// Length is encoded in 1 or 2 bytes.
		if max > 255 {
			// This code path exists due to https://bugs.mysql.com/bug.php?id=37426.
			// CHAR types need to allocate 3 bytes per char. So, the length for CHAR(255)
			// cannot be represented in 1 byte. This also means that this rule does not
			// apply to BINARY data.
			l := int(uint64(data[pos]) |
				uint64(data[pos+1])<<8)
			return sqltypes.MakeTrusted(querypb.Type_VARCHAR,
				data[pos+2:pos+2+l]), l + 2, nil
		}
		l := int(data[pos])
		mdata := data[pos+1 : pos+1+l]
		if sqltypes.IsBinary(field.Type) {
			// For binary(n) column types, mysql pads the data on the right with nulls. However the binlog event contains
			// the data without this padding. This causes several issues:
			//    * if a binary(n) column is part of the sharding key, the keyspace_id() returned during the copy phase
			//      (where the value is the result of a mysql query) is different from the one during replication
			//      (where the value is the one from the binlogs)
			//    * mysql where clause comparisons do not do the right thing without padding
			// So for fixed length BINARY columns we right-pad it with nulls if necessary to match what MySQL returns.
			// Because CHAR columns with a binary collation (e.g. utf8mb4_bin) have the same metadata as a BINARY column
			// in binlog events, we also need to check for this case based on the underlying column type.
			if l < max && strings.HasPrefix(strings.ToLower(field.ColumnType), "binary") {
				paddedData := make([]byte, max)
				copy(paddedData[:l], mdata)
				mdata = paddedData
			}
			return sqltypes.MakeTrusted(querypb.Type_BINARY, mdata), l + 1, nil
		}
		return sqltypes.MakeTrusted(querypb.Type_VARCHAR, mdata), l + 1, nil

	case TypeGeometry:
		l := 0
		switch metadata {
		case 1:
			l = int(uint32(data[pos]))
		case 2:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8)
		case 3:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16)
		case 4:
			l = int(uint32(data[pos]) |
				uint32(data[pos+1])<<8 |
				uint32(data[pos+2])<<16 |
				uint32(data[pos+3])<<24)
		default:
			return sqltypes.NULL, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported geometry metadata value %v (data: %v pos: %v)", metadata, data, pos)
		}
		pos += int(metadata)
		return sqltypes.MakeTrusted(querypb.Type_GEOMETRY,
			data[pos:pos+l]), l + int(metadata), nil

	default:
		return sqltypes.NULL, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unsupported type %v", typ)
	}
}
