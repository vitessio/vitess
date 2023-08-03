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

// Package sqltypes implements interfaces and types that represent SQL values.
package sqltypes

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// NULL represents the NULL value.
	NULL = Value{}

	// DontEscape tells you if a character should not be escaped.
	DontEscape = byte(255)
	NullStr    = "null"
	NullBytes  = []byte(NullStr)

	// ErrIncompatibleTypeCast indicates a casting problem
	ErrIncompatibleTypeCast = errors.New("Cannot convert value to desired type")
)

type (
	// BinWriter interface is used for encoding values.
	// Types like bytes.Buffer conform to this interface.
	// We expect the writer objects to be in-memory buffers.
	// So, we don't expect the write operations to fail.
	BinWriter interface {
		Write([]byte) (int, error)
	}

	// Value can store any SQL value. If the value represents
	// an integral type, the bytes are always stored as a canonical
	// representation that matches how MySQL returns such values.
	Value struct {
		typ querypb.Type
		val []byte
	}

	Row = []Value
)

// NewValue builds a Value using typ and val. If the value and typ
// don't match, it returns an error.
func NewValue(typ querypb.Type, val []byte) (v Value, err error) {
	switch {
	case IsSigned(typ):
		if _, err := fastparse.ParseInt64(hack.String(val), 10); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsUnsigned(typ):
		if _, err := fastparse.ParseUint64(hack.String(val), 10); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsFloat(typ):
		if _, err := fastparse.ParseFloat64(hack.String(val)); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsDecimal(typ):
		if _, err := decimal.NewFromMySQL(val); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsQuoted(typ) || typ == Bit || typ == HexNum || typ == HexVal || typ == Null || typ == BitNum:
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

// NewHexNum builds an Hex Value.
func NewHexNum(v []byte) Value {
	return MakeTrusted(HexNum, v)
}

// NewHexVal builds a HexVal Value.
func NewHexVal(v []byte) Value {
	return MakeTrusted(HexVal, v)
}

// NewBitNum builds a BitNum Value.
func NewBitNum(v []byte) Value {
	return MakeTrusted(BitNum, v)
}

// NewInt64 builds an Int64 Value.
func NewInt64(v int64) Value {
	return MakeTrusted(Int64, strconv.AppendInt(nil, v, 10))
}

// NewInt8 builds an Int8 Value.
func NewInt8(v int8) Value {
	return MakeTrusted(Int8, strconv.AppendInt(nil, int64(v), 10))
}

// NewInt32 builds an Int64 Value.
func NewInt32(v int32) Value {
	return MakeTrusted(Int32, strconv.AppendInt(nil, int64(v), 10))
}

// NewUint64 builds an Uint64 Value.
func NewUint64(v uint64) Value {
	return MakeTrusted(Uint64, strconv.AppendUint(nil, v, 10))
}

// NewUint32 builds an Uint32 Value.
func NewUint32(v uint32) Value {
	return MakeTrusted(Uint32, strconv.AppendUint(nil, uint64(v), 10))
}

// NewFloat64 builds an Float64 Value.
func NewFloat64(v float64) Value {
	return MakeTrusted(Float64, strconv.AppendFloat(nil, v, 'g', -1, 64))
}

// NewVarChar builds a VarChar Value.
func NewVarChar(v string) Value {
	return MakeTrusted(VarChar, []byte(v))
}

func NewJSON(v string) (Value, error) {
	j := []byte(v)
	if !json.Valid(j) {
		return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid JSON value: %q", v)
	}
	return MakeTrusted(TypeJSON, j), nil
}

// NewVarBinary builds a VarBinary Value.
// The input is a string because it's the most common use case.
func NewVarBinary(v string) Value {
	return MakeTrusted(VarBinary, []byte(v))
}

// NewDate builds a Date value.
func NewDate(v string) Value {
	return MakeTrusted(Date, []byte(v))
}

// NewTime builds a Time value.
func NewTime(v string) Value {
	return MakeTrusted(Time, []byte(v))
}

// NewTimestamp builds a Timestamp value.
func NewTimestamp(v string) Value {
	return MakeTrusted(Timestamp, []byte(v))
}

// NewDatetime builds a Datetime value.
func NewDatetime(v string) Value {
	return MakeTrusted(Datetime, []byte(v))
}

// NewDecimal builds a Decimal value.
func NewDecimal(v string) Value {
	return MakeTrusted(Decimal, []byte(v))
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
func InterfaceToValue(goval any) (Value, error) {
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

// RawStr returns the internal representation of the value as a string instead
// of a byte slice. This is equivalent to calling `string(v.Raw())` but does
// not allocate.
func (v Value) RawStr() string {
	return hack.String(v.val)
}

// ToBytes returns the value as MySQL would return it as []byte.
// In contrast, Raw returns the internal representation of the Value, which may not
// match MySQL's representation for hex encoded binary data or newer types.
// If the value is not convertible like in the case of Expression, it returns an error.
func (v Value) ToBytes() ([]byte, error) {
	switch v.typ {
	case Expression:
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "expression cannot be converted to bytes")
	case HexVal: // TODO: all the decode below have problem when decoding odd number of bytes. This needs to be fixed.
		return v.decodeHexVal()
	case HexNum:
		return v.decodeHexNum()
	case BitNum:
		return v.decodeBitNum()
	default:
		return v.val, nil
	}
}

// Len returns the length.
func (v Value) Len() int {
	return len(v.val)
}

// ToInt64 returns the value as MySQL would return it as a int64.
func (v Value) ToInt64() (int64, error) {
	if !v.IsIntegral() {
		return 0, ErrIncompatibleTypeCast
	}

	return fastparse.ParseInt64(v.RawStr(), 10)
}

func (v Value) ToInt32() (int32, error) {
	if !v.IsIntegral() {
		return 0, ErrIncompatibleTypeCast
	}

	i, err := strconv.ParseInt(v.RawStr(), 10, 32)
	return int32(i), err
}

// ToInt returns the value as MySQL would return it as a int.
func (v Value) ToInt() (int, error) {
	if !v.IsIntegral() {
		return 0, ErrIncompatibleTypeCast
	}

	return strconv.Atoi(v.RawStr())
}

// ToFloat64 returns the value as MySQL would return it as a float64.
func (v Value) ToFloat64() (float64, error) {
	if !IsNumber(v.typ) {
		return 0, ErrIncompatibleTypeCast
	}

	return fastparse.ParseFloat64(v.RawStr())
}

// ToUint16 returns the value as MySQL would return it as a uint16.
func (v Value) ToUint16() (uint16, error) {
	if !v.IsIntegral() {
		return 0, ErrIncompatibleTypeCast
	}

	i, err := strconv.ParseUint(v.RawStr(), 10, 16)
	return uint16(i), err
}

// ToUint64 returns the value as MySQL would return it as a uint64.
func (v Value) ToUint64() (uint64, error) {
	if !v.IsIntegral() {
		return 0, ErrIncompatibleTypeCast
	}

	return fastparse.ParseUint64(v.RawStr(), 10)
}

func (v Value) ToUint32() (uint32, error) {
	if !v.IsIntegral() {
		return 0, ErrIncompatibleTypeCast
	}

	u, err := strconv.ParseUint(v.RawStr(), 10, 32)
	return uint32(u), err
}

// ToBool returns the value as a bool value
func (v Value) ToBool() (bool, error) {
	i, err := v.ToInt64()
	if err != nil {
		return false, err
	}
	switch i {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, ErrIncompatibleTypeCast
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
	if v.IsQuoted() || v.typ == Bit {
		return fmt.Sprintf("%v(%q)", v.typ, v.val)
	}
	return fmt.Sprintf("%v(%s)", v.typ, v.val)
}

// EncodeSQL encodes the value into an SQL statement. Can be binary.
func (v Value) EncodeSQL(b BinWriter) {
	switch {
	case v.typ == Null:
		b.Write(NullBytes)
	case v.IsQuoted():
		encodeBytesSQL(v.val, b)
	case v.typ == Bit:
		encodeBytesSQLBits(v.val, b)
	default:
		b.Write(v.val)
	}
}

// EncodeSQLStringBuilder is identical to EncodeSQL but it takes a strings.Builder
// as its writer, so it can be inlined for performance.
func (v Value) EncodeSQLStringBuilder(b *strings.Builder) {
	switch {
	case v.typ == Null:
		b.Write(NullBytes)
	case v.IsQuoted():
		encodeBytesSQLStringBuilder(v.val, b)
	case v.typ == Bit:
		encodeBytesSQLBits(v.val, b)
	default:
		b.Write(v.val)
	}
}

// EncodeSQLBytes2 is identical to EncodeSQL but it takes a bytes2.Buffer
// as its writer, so it can be inlined for performance.
func (v Value) EncodeSQLBytes2(b *bytes2.Buffer) {
	switch {
	case v.typ == Null:
		b.Write(NullBytes)
	case v.IsQuoted():
		encodeBytesSQLBytes2(v.val, b)
	case v.typ == Bit:
		encodeBytesSQLBits(v.val, b)
	default:
		b.Write(v.val)
	}
}

// EncodeASCII encodes the value using 7-bit clean ascii bytes.
func (v Value) EncodeASCII(b BinWriter) {
	switch {
	case v.typ == Null:
		b.Write(NullBytes)
	case v.IsQuoted() || v.typ == Bit:
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

// IsDateTime returns true if Value is datetime.
func (v Value) IsDateTime() bool {
	return v.typ == querypb.Type_DATETIME
}

// IsTimestamp returns true if Value is date.
func (v Value) IsTimestamp() bool {
	return v.typ == querypb.Type_TIMESTAMP
}

// IsDate returns true if Value is date.
func (v Value) IsDate() bool {
	return v.typ == querypb.Type_DATE
}

// IsTime returns true if Value is time.
func (v Value) IsTime() bool {
	return v.typ == querypb.Type_TIME
}

// IsDecimal returns true if Value is a decimal.
func (v Value) IsDecimal() bool {
	return IsDecimal(v.typ)
}

// IsComparable returns true if the Value is null safe comparable without collation information.
func (v *Value) IsComparable() bool {
	if v.typ == Null || IsNumber(v.typ) || IsBinary(v.typ) {
		return true
	}
	switch v.typ {
	case Timestamp, Date, Time, Datetime, Enum, Set, TypeJSON, Bit:
		return true
	}
	return false
}

// MarshalJSON should only be used for testing.
// It's not a complete implementation.
func (v Value) MarshalJSON() ([]byte, error) {
	switch {
	case v.IsQuoted() || v.typ == Bit:
		return json.Marshal(v.ToString())
	case v.typ == Null:
		return NullBytes, nil
	}
	return v.val, nil
}

// UnmarshalJSON should only be used for testing.
// It's not a complete implementation.
func (v *Value) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("error unmarshaling empty bytes")
	}
	var val any
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

// decodeHexVal decodes the SQL hex value of the form x'A1' into a byte
// array matching what MySQL would return when querying the column where
// an INSERT was performed with x'A1' having been specified as a value
func (v *Value) decodeHexVal() ([]byte, error) {
	if len(v.val) < 3 || (v.val[0] != 'x' && v.val[0] != 'X') || v.val[1] != '\'' || v.val[len(v.val)-1] != '\'' {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid hex value: %v", v.val)
	}
	hexBytes := v.val[2 : len(v.val)-1]
	decodedHexBytes, err := hex.DecodeString(string(hexBytes))
	if err != nil {
		return nil, err
	}
	return decodedHexBytes, nil
}

// decodeHexNum decodes the SQL hex value of the form 0xA1 into a byte
// array matching what MySQL would return when querying the column where
// an INSERT was performed with 0xA1 having been specified as a value
func (v *Value) decodeHexNum() ([]byte, error) {
	if len(v.val) < 3 || v.val[0] != '0' || v.val[1] != 'x' {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid hex number: %v", v.val)
	}
	hexBytes := v.val[2:]
	decodedHexBytes, err := hex.DecodeString(string(hexBytes))
	if err != nil {
		return nil, err
	}
	return decodedHexBytes, nil
}

// decodeBitNum decodes the SQL bit value of the form 0b101 into a byte
// array matching what MySQL would return when querying the column where
// an INSERT was performed with 0x5 having been specified as a value
func (v *Value) decodeBitNum() ([]byte, error) {
	if len(v.val) < 3 || v.val[0] != '0' || v.val[1] != 'b' {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid bit number: %v", v.val)
	}
	var i big.Int
	_, ok := i.SetString(string(v.val), 0)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid bit number: %v", v.val)
	}
	return i.Bytes(), nil
}

func encodeBytesSQL(val []byte, b BinWriter) {
	buf := &bytes2.Buffer{}
	encodeBytesSQLBytes2(val, buf)
	b.Write(buf.Bytes())
}

func encodeBytesSQLBytes2(val []byte, buf *bytes2.Buffer) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		// If \% or \_ is present, we want to keep them as is, and don't want to escape \ again
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

func encodeBytesSQLStringBuilder(val []byte, buf *strings.Builder) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		// If \% or \_ is present, we want to keep them as is, and don't want to escape \ again
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

// BufEncodeStringSQL encodes the string into a strings.Builder
func BufEncodeStringSQL(buf *strings.Builder, val string) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		if ch > 255 {
			buf.WriteRune(ch)
			continue
		}
		// If \% or \_ is present, we want to keep them as is, and don't want to escape \ again
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteRune(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteRune(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

// EncodeStringSQL encodes the string as a SQL string.
func EncodeStringSQL(val string) string {
	var buf strings.Builder
	BufEncodeStringSQL(&buf, val)
	return buf.String()
}

func encodeBytesSQLBits(val []byte, b BinWriter) {
	fmt.Fprint(b, "b'")
	for _, ch := range val {
		fmt.Fprintf(b, "%08b", ch)
	}
	fmt.Fprint(b, "'")
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
// Complies to https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
// Handling escaping of % and _ is different than other characters.
// When escaped in a like clause, they are supposed to be treated as literals
// Everywhere else, they evaluate to strings '\%' and '\_' respectively.
// In Vitess, the way we are choosing to handle this behaviour is to always
// preserve the escaping of % and _ as is in all the places and handle it like MySQL
// in our evaluation engine for Like.
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
