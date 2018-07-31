/*
Copyright 2017 Google Inc.

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

// Package sqltypes implements interfaces and types that represent SQL values.
package sqltypes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/hack"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	// NULL represents the NULL value.
	NULL = Value{}

	// DontEscape tells you if a character should not be escaped.
	DontEscape = byte(255)

	nullstr = []byte("null")
)

// BinWriter interface is used for encoding values.
// Types like bytes.Buffer conform to this interface.
// We expect the writer objects to be in-memory buffers.
// So, we don't expect the write operations to fail.
type BinWriter interface {
	Write([]byte) (int, error)
}

// Value can store any SQL value. If the value represents
// an integral type, the bytes are always stored as a canonical
// representation that matches how MySQL returns such values.
type Value struct {
	typ querypb.Type
	val []byte
}

// NewValue builds a Value using typ and val. If the value and typ
// don't match, it returns an error.
func NewValue(typ querypb.Type, val []byte) (v Value, err error) {
	switch {
	case IsSigned(typ):
		if _, err := strconv.ParseInt(string(val), 0, 64); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsUnsigned(typ):
		if _, err := strconv.ParseUint(string(val), 0, 64); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsFloat(typ) || typ == Decimal:
		if _, err := strconv.ParseFloat(string(val), 64); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsQuoted(typ) || typ == Null:
		return MakeTrusted(typ, val), nil
	}
	// All other types are unsafe or invalid.
	return NULL, fmt.Errorf("invalid type specified for MakeValue: %v", typ)
}

// MakeTrusted makes a new Value based on the type.
// This function should only be used if you know the value
// and type conform to the rules. Every place this function is
// called, a comment is needed that explains why it's justified.
// Exceptions: The current package and mysql package do not need
// comments. Other packages can also use the function to create
// VarBinary or VarChar values.
func MakeTrusted(typ querypb.Type, val []byte) Value {
	if typ == Null {
		return NULL
	}
	return Value{typ: typ, val: val}
}

// NewInt64 builds an Int64 Value.
func NewInt64(v int64) Value {
	return MakeTrusted(Int64, strconv.AppendInt(nil, v, 10))
}

// NewInt32 builds an Int64 Value.
func NewInt32(v int32) Value {
	return MakeTrusted(Int32, strconv.AppendInt(nil, int64(v), 10))
}

// NewUint64 builds an Uint64 Value.
func NewUint64(v uint64) Value {
	return MakeTrusted(Uint64, strconv.AppendUint(nil, v, 10))
}

// NewFloat32 builds an Float64 Value.
func NewFloat32(v float32) Value {
	return MakeTrusted(Float32, strconv.AppendFloat(nil, float64(v), 'f', -1, 64))
}

// NewFloat64 builds an Float64 Value.
func NewFloat64(v float64) Value {
	return MakeTrusted(Float64, strconv.AppendFloat(nil, v, 'g', -1, 64))
}

// NewVarChar builds a VarChar Value.
func NewVarChar(v string) Value {
	return MakeTrusted(VarChar, []byte(v))
}

// NewVarBinary builds a VarBinary Value.
// The input is a string because it's the most common use case.
func NewVarBinary(v string) Value {
	return MakeTrusted(VarBinary, []byte(v))
}

// NewIntegral builds an integral type from a string representation.
// The type will be Int64 or Uint64. Int64 will be preferred where possible.
func NewIntegral(val string) (n Value, err error) {
	signed, err := strconv.ParseInt(val, 0, 64)
	if err == nil {
		return MakeTrusted(Int64, strconv.AppendInt(nil, signed, 10)), nil
	}
	unsigned, err := strconv.ParseUint(val, 0, 64)
	if err != nil {
		return Value{}, err
	}
	return MakeTrusted(Uint64, strconv.AppendUint(nil, unsigned, 10)), nil
}

// InterfaceToValue builds a value from a go type.
// Supported types are nil, int64, uint64, float64,
// string and []byte.
// This function is deprecated. Use the type-specific
// functions instead.
func InterfaceToValue(goval interface{}) (Value, error) {
	switch goval := goval.(type) {
	case nil:
		return NULL, nil
	case []byte:
		return MakeTrusted(VarBinary, goval), nil
	case int64:
		return NewInt64(goval), nil
	case uint64:
		return NewUint64(goval), nil
	case float64:
		return NewFloat64(goval), nil
	case string:
		return NewVarChar(goval), nil
	default:
		return NULL, fmt.Errorf("unexpected type %T: %v", goval, goval)
	}
}

// Type returns the type of Value.
func (v Value) Type() querypb.Type {
	return v.typ
}

// Raw returns the internal representation of the value. For newer types,
// this may not match MySQL's representation.
func (v Value) Raw() []byte {
	return v.val
}

// ToBytes returns the value as MySQL would return it as []byte.
// In contrast, Raw returns the internal representation of the Value, which may not
// match MySQL's representation for newer types.
// If the value is not convertible like in the case of Expression, it returns nil.
func (v Value) ToBytes() []byte {
	if v.typ == Expression {
		return nil
	}
	return v.val
}

// Len returns the length.
func (v Value) Len() int {
	return len(v.val)
}

// ToString returns the value as MySQL would return it as string.
// If the value is not convertible like in the case of Expression, it returns nil.
func (v Value) ToString() string {
	if v.typ == Expression {
		return ""
	}
	return hack.String(v.val)
}

// String returns a printable version of the value.
func (v Value) String() string {
	if v.typ == Null {
		return "NULL"
	}
	if v.IsQuoted() {
		return fmt.Sprintf("%v(%q)", v.typ, v.val)
	}
	return fmt.Sprintf("%v(%s)", v.typ, v.val)
}

func writeByte(data []byte, pos int, value byte) int {
	data[pos] = value
	return pos + 1
}

func writeUint16(data []byte, pos int, value uint16) int {
	data[pos] = byte(value)
	data[pos+1] = byte(value >> 8)
	return pos + 2
}

func writeUint32(data []byte, pos int, value uint32) int {
	data[pos] = byte(value)
	data[pos+1] = byte(value >> 8)
	data[pos+2] = byte(value >> 16)
	data[pos+3] = byte(value >> 24)
	return pos + 4
}

func writeUint64(data []byte, pos int, value uint64) int {
	data[pos] = byte(value)
	data[pos+1] = byte(value >> 8)
	data[pos+2] = byte(value >> 16)
	data[pos+3] = byte(value >> 24)
	data[pos+4] = byte(value >> 32)
	data[pos+5] = byte(value >> 40)
	data[pos+6] = byte(value >> 48)
	data[pos+7] = byte(value >> 56)
	return pos + 8
}

// lenEncIntSize returns the number of bytes required to encode a
// variable-length integer.
func lenEncIntSize(i uint64) int {
	switch {
	case i < 251:
		return 1
	case i < 1<<16:
		return 3
	case i < 1<<24:
		return 4
	default:
		return 9
	}
}

func writeLenEncInt(data []byte, pos int, i uint64) int {
	switch {
	case i < 251:
		data[pos] = byte(i)
		return pos + 1
	case i < 1<<16:
		data[pos] = 0xfc
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		return pos + 3
	case i < 1<<24:
		data[pos] = 0xfd
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		data[pos+3] = byte(i >> 16)
		return pos + 4
	default:
		data[pos] = 0xfe
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		data[pos+3] = byte(i >> 16)
		data[pos+4] = byte(i >> 24)
		data[pos+5] = byte(i >> 32)
		data[pos+6] = byte(i >> 40)
		data[pos+7] = byte(i >> 48)
		data[pos+8] = byte(i >> 56)
		return pos + 9
	}
}

// ToMySQL converts Value to a mysql type value.
func (v Value) ToMySQL() ([]byte, error) {
	var out []byte
	pos := 0
	switch v.typ {
	case Null:
		// no-op
	case Int8:
		val, err := strconv.ParseInt(v.ToString(), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case Uint8:
		val, err := strconv.ParseUint(v.ToString(), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case Uint16:
		val, err := strconv.ParseUint(v.ToString(), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case Int16, Year:
		val, err := strconv.ParseInt(v.ToString(), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case Uint24, Uint32:
		val, err := strconv.ParseUint(v.ToString(), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case Int24, Int32:
		val, err := strconv.ParseInt(v.ToString(), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case Float32:
		val, err := strconv.ParseFloat(v.ToString(), 32)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float32bits(float32(val))
		out = make([]byte, 4)
		writeUint32(out, pos, bits)
	case Uint64:
		val, err := strconv.ParseUint(v.ToString(), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case Int64:
		val, err := strconv.ParseInt(v.ToString(), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case Float64:
		val, err := strconv.ParseFloat(v.ToString(), 64)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float64bits(val)
		out = make([]byte, 8)
		writeUint64(out, pos, bits)
	case Timestamp, Date, Datetime:
		if len(v.val) > 19 {
			out = make([]byte, 1+11)
			out[pos] = 0x0b
			pos++
			year, err := strconv.ParseUint(string(v.val[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.val[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.val[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.val[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.val[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.val[17:19]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			val := make([]byte, 6)
			count := copy(val, v.val[20:])
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSecond, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			pos = writeByte(out, pos, byte(second))
			pos = writeUint32(out, pos, uint32(microSecond))
		} else if len(v.val) > 10 {
			out = make([]byte, 1+7)
			out[pos] = 0x07
			pos++
			year, err := strconv.ParseUint(string(v.val[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.val[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.val[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.val[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.val[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.val[17:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			pos = writeByte(out, pos, byte(second))
		} else if len(v.val) > 0 {
			out = make([]byte, 1+4)
			out[pos] = 0x04
			pos++
			year, err := strconv.ParseUint(string(v.val[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.val[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.val[8:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
		} else {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
	case Time:
		if string(v.val) == "00:00:00" {
			out = make([]byte, 1)
			out[pos] = 0x00
		} else if strings.Contains(string(v.val), ".") {
			out = make([]byte, 1+12)
			out[pos] = 0x0c
			pos++

			sub1 := strings.Split(string(v.val), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}
			sub2 := strings.Split(sub1[2], ".")
			if len(sub2) != 2 {
				err := fmt.Errorf("incorrect time value, '.' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub2[0]
			microSecond := sub2[1]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			pos = writeByte(out, pos, byte(seconds))

			val := make([]byte, 6)
			count := copy(val, microSecond)
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSeconds, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(microSeconds))
		} else if len(v.val) > 0 {
			out = make([]byte, 1+8)
			out[pos] = 0x08
			pos++

			sub1 := strings.Split(string(v.val), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub1[2]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			pos = writeByte(out, pos, byte(seconds))
		} else {
			err := fmt.Errorf("incorrect time value")
			return []byte{}, err
		}
	case Decimal, Text, VarChar, VarBinary, Char, Bit, Enum, Set, Geometry, Binary, TypeJSON:
		l := len(v.val)
		length := lenEncIntSize(uint64(l)) + l
		out = make([]byte, length)
		pos = writeLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.val)
	case Blob:
		l := len(v.val)
		length := lenEncIntSize(uint64(l)) + l + 1
		out = make([]byte, length)
		pos = writeLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.val)
	default:
		out = make([]byte, len(v.val))
		copy(out, v.val)
	}
	return out, nil
}

// ToMySQLLen converts Value to a mysql type value length.
func (v Value) ToMySQLLen() (int, error) {
	var length int
	var err error
	switch v.typ {
	case Null:
		length = 0
	case Int8, Uint8:
		length = 1
	case Uint16, Int16, Year:
		length = 2
	case Uint24, Uint32, Int24, Int32, Float32:
		length = 4
	case Uint64, Int64, Float64:
		length = 8
	case Timestamp, Date, Datetime:
		if len(v.val) > 19 {
			length = 12
		} else if len(v.val) > 10 {
			length = 8
		} else if len(v.val) > 0 {
			length = 5
		} else {
			length = 1
		}
	case Time:
		if string(v.val) == "00:00:00" {
			length = 1
		} else if strings.Contains(string(v.val), ".") {
			length = 13
		} else if len(v.val) > 0 {
			length = 9
		} else {
			err = fmt.Errorf("incorrect time value")
		}
	case Decimal, Text, VarChar, VarBinary, Char, Bit, Enum, Set, Geometry, TypeJSON:
		l := len(v.val)
		length = lenEncIntSize(uint64(l)) + l
	case Blob:
		l := len(v.val)
		length = lenEncIntSize(uint64(l)) + l + 1
	case Binary:
		length = len(v.val)
	default:
		length = len(v.val)
	}
	if err != nil {
		return 0, err
	}
	return length, nil
}

// EncodeSQL encodes the value into an SQL statement. Can be binary.
func (v Value) EncodeSQL(b BinWriter) {
	switch {
	case v.typ == Null:
		b.Write(nullstr)
	case v.IsQuoted():
		encodeBytesSQL(v.val, b)
	default:
		b.Write(v.val)
	}
}

// EncodeASCII encodes the value using 7-bit clean ascii bytes.
func (v Value) EncodeASCII(b BinWriter) {
	switch {
	case v.typ == Null:
		b.Write(nullstr)
	case v.IsQuoted():
		encodeBytesASCII(v.val, b)
	default:
		b.Write(v.val)
	}
}

// IsNull returns true if Value is null.
func (v Value) IsNull() bool {
	return v.typ == Null
}

// IsIntegral returns true if Value is an integral.
func (v Value) IsIntegral() bool {
	return IsIntegral(v.typ)
}

// IsSigned returns true if Value is a signed integral.
func (v Value) IsSigned() bool {
	return IsSigned(v.typ)
}

// IsUnsigned returns true if Value is an unsigned integral.
func (v Value) IsUnsigned() bool {
	return IsUnsigned(v.typ)
}

// IsFloat returns true if Value is a float.
func (v Value) IsFloat() bool {
	return IsFloat(v.typ)
}

// IsQuoted returns true if Value must be SQL-quoted.
func (v Value) IsQuoted() bool {
	return IsQuoted(v.typ)
}

// IsText returns true if Value is a collatable text.
func (v Value) IsText() bool {
	return IsText(v.typ)
}

// IsBinary returns true if Value is binary.
func (v Value) IsBinary() bool {
	return IsBinary(v.typ)
}

// MarshalJSON should only be used for testing.
// It's not a complete implementation.
func (v Value) MarshalJSON() ([]byte, error) {
	switch {
	case v.IsQuoted():
		return json.Marshal(v.ToString())
	case v.typ == Null:
		return nullstr, nil
	}
	return v.val, nil
}

// UnmarshalJSON should only be used for testing.
// It's not a complete implementation.
func (v *Value) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("error unmarshaling empty bytes")
	}
	var val interface{}
	var err error
	switch b[0] {
	case '-':
		var ival int64
		err = json.Unmarshal(b, &ival)
		val = ival
	case '"':
		var bval []byte
		err = json.Unmarshal(b, &bval)
		val = bval
	case 'n': // null
		err = json.Unmarshal(b, &val)
	default:
		var uval uint64
		err = json.Unmarshal(b, &uval)
		val = uval
	}
	if err != nil {
		return err
	}
	*v, err = InterfaceToValue(val)
	return err
}

func encodeBytesSQL(val []byte, b BinWriter) {
	buf := &bytes2.Buffer{}
	buf.WriteByte('\'')
	for _, ch := range val {
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
	b.Write(buf.Bytes())
}

func encodeBytesASCII(val []byte, b BinWriter) {
	buf := &bytes2.Buffer{}
	buf.WriteByte('\'')
	encoder := base64.NewEncoder(base64.StdEncoding, buf)
	encoder.Write(val)
	encoder.Close()
	buf.WriteByte('\'')
	b.Write(buf.Bytes())
}

// SQLEncodeMap specifies how to escape binary data with '\'.
// Complies to http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
var SQLEncodeMap [256]byte

// SQLDecodeMap is the reverse of SQLEncodeMap
var SQLDecodeMap [256]byte

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range SQLEncodeMap {
		SQLEncodeMap[i] = DontEscape
		SQLDecodeMap[i] = DontEscape
	}
	for i := range SQLEncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			SQLEncodeMap[byte(i)] = to
			SQLDecodeMap[to] = byte(i)
		}
	}
}
