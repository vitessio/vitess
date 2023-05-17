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
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/format"
	"vitess.io/vitess/go/mysql/json"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

/*
References:

* Docs for MySQL JSON binary format:
https://dev.mysql.com/doc/dev/mysql-server/latest/json__binary_8h.html

* nice description of MySQL's json representation
https://lafengnan.gitbooks.io/blog/content/mysql/chapter2.html

* java/python connector links: useful for test cases and reverse engineering
https://github.com/shyiko/mysql-binlog-connector-java/pull/119/files
https://github.com/noplay/python-mysql-replication/blob/175df28cc8b536a68522ff9b09dc5440adad6094/pymysqlreplication/packet.py
*/

// ParseBinaryJSON provides the parsing function from the mysql binary json
// representation to a JSON value instance.
func ParseBinaryJSON(data []byte) (*json.Value, error) {
	var err error
	var node *json.Value
	if len(data) == 0 {
		node = json.ValueNull
	} else {
		node, err = binparserNode(jsonDataType(data[0]), data, 1)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

// jsonDataType has the values used in the mysql json binary representation to denote types.
// We have string, literal(true/false/null), number, object or array types.
// large object => doc size > 64K: you get pointers instead of inline values.
type jsonDataType byte

// type mapping as defined by the mysql json representation
const (
	jsonSmallObject = 0
	jsonLargeObject = 1
	jsonSmallArray  = 2
	jsonLargeArray  = 3
	jsonLiteral     = 4
	jsonInt16       = 5
	jsonUint16      = 6
	jsonInt32       = 7
	jsonUint32      = 8
	jsonInt64       = 9
	jsonUint64      = 10 //0x0a
	jsonDouble      = 11 //0x0b
	jsonString      = 12 //0x0c a utf8mb4 string
	jsonOpaque      = 15 //0x0f "custom" data
)

// literals in the binary json format can be one of three types: null, true, false
type jsonDataLiteral byte

// this is how mysql maps the three literals in the binlog
const (
	jsonNullLiteral  = '\x00'
	jsonTrueLiteral  = '\x01'
	jsonFalseLiteral = '\x02'
)

// in objects and arrays some values are inlined, other types have offsets into the raw data.
// literals (true/false/null) and 16bit integers are always inlined.
// for large documents 32bit integers are also inlined.
// principle is that two byte values are inlined in "small", and four byte in "large" docs
func isInline(typ jsonDataType, large bool) bool {
	switch typ {
	case jsonLiteral, jsonInt16, jsonUint16:
		return true
	case jsonInt32, jsonUint32:
		if large {
			return true
		}
	}
	return false
}

// readInt returns either a 32-bit or a 16-bit int from the passed buffer. Which one it is,
// depends on whether the document is "large" or not.
// JSON documents stored are considered "large" if the size of the stored json document is
// more than 64K bytes. Values of non-inlined types are stored as offsets into the document.
// The int returned is either an (i) offset into the raw data, (ii) count of elements, or (iii) size of the represented data structure.
// (This design decision allows a fixed number of bytes to be used for representing object keys and array indices.)
// readInt also returns the new position (by advancing the position by the number of bytes read).
func readInt(data []byte, pos int, large bool) (int, int) {
	if large {
		return int(data[pos]) +
				int(data[pos+1])<<8 +
				int(data[pos+2])<<16 +
				int(data[pos+3])<<24,
			pos + 4
	}
	return int(data[pos]) +
		int(data[pos+1])<<8, pos + 2
}

// readVariableLength implements the logic to decode the length
// of an arbitrarily long string.
// readVariableLength also returns the new position (by advancing the position by the number of bytes read).
func readVariableLength(data []byte, pos int) (int, int) {
	var bb byte
	var length int
	var idx byte
	for {
		bb = data[pos]
		pos++
		length |= int(bb&0x7f) << (7 * idx)
		// if the high bit is 1, the integer value of the byte will be negative.
		// high bit of 1 signifies that the next byte is part of the length encoding.
		if int8(bb) >= 0 {
			break
		}
		idx++
	}
	return length, pos
}

var binparserFn [16]func(dataType jsonDataType, data []byte, pos int) (*json.Value, error)

func init() {
	binparserFn[jsonSmallObject] = binparserObject
	binparserFn[jsonLargeObject] = binparserObject
	binparserFn[jsonSmallArray] = binparserArray
	binparserFn[jsonLargeArray] = binparserArray
	binparserFn[jsonLiteral] = binparserLiteral
	binparserFn[jsonInt16] = binparserInt
	binparserFn[jsonUint16] = binparserInt
	binparserFn[jsonInt32] = binparserInt
	binparserFn[jsonUint32] = binparserInt
	binparserFn[jsonInt64] = binparserInt
	binparserFn[jsonUint64] = binparserInt
	binparserFn[jsonDouble] = binparserInt
	binparserFn[jsonString] = binparserString
	binparserFn[jsonOpaque] = binparserOpaque
}

func binparserNode(typ jsonDataType, data []byte, pos int) (node *json.Value, err error) {
	if int(typ) < len(binparserFn) {
		if p := binparserFn[typ]; p != nil {
			return p(typ, data, pos)
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid json type: %d", typ)
}

// getElem returns the json value found inside json objects and arrays at the provided position
func binparserElement(data []byte, pos int, large bool) (*json.Value, int, error) {
	var elem *json.Value
	var err error
	var offset int
	typ := jsonDataType(data[pos])
	pos++
	if isInline(typ, large) {
		elem, err = binparserNode(typ, data, pos)
		if err != nil {
			return nil, 0, err
		}
		if large {
			pos += 4
		} else {
			pos += 2
		}
	} else {
		offset, pos = readInt(data, pos, large)
		if offset >= len(data) { // consistency check, should only come here is there is a bug in the code
			return nil, 0, fmt.Errorf("unable to decode element: %+v", data)
		}
		newData := data[offset:]
		//newPos ignored because this is an offset into the "extra" section of the buffer
		elem, err = binparserNode(typ, newData, 1)
		if err != nil {
			return nil, 0, err
		}
	}
	return elem, pos, nil
}

//endregion

var binaryIntSizes = map[jsonDataType]int{
	jsonUint64: 8,
	jsonInt64:  8,
	jsonUint32: 4,
	jsonInt32:  4,
	jsonUint16: 2,
	jsonInt16:  2,
	jsonDouble: 8,
}

func binparserInt(typ jsonDataType, data []byte, pos int) (*json.Value, error) {
	var val uint64
	size := binaryIntSizes[typ]
	for i := 0; i < size; i++ {
		val = val + uint64(data[pos+i])<<(8*i)
	}
	var s string
	var n json.NumberType
	switch typ {
	case jsonInt16:
		s = strconv.FormatInt(int64(int16(val)), 10)
		n = json.NumberTypeSigned
	case jsonUint16:
		s = strconv.FormatUint(uint64(uint16(val)), 10)
		n = json.NumberTypeUnsigned
	case jsonInt32:
		s = strconv.FormatInt(int64(int32(val)), 10)
		n = json.NumberTypeSigned
	case jsonUint32:
		s = strconv.FormatUint(uint64(uint32(val)), 10)
		n = json.NumberTypeUnsigned
	case jsonInt64:
		s = strconv.FormatInt(int64(val), 10)
		n = json.NumberTypeSigned
	case jsonUint64:
		s = strconv.FormatUint(val, 10)
		n = json.NumberTypeUnsigned
	case jsonDouble:
		s = hack.String(format.FormatFloat(math.Float64frombits(val)))
		n = json.NumberTypeFloat
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid int type: %d", typ)
	}
	return json.NewNumber(s, n), nil
}

func binparserLiteral(_ jsonDataType, data []byte, pos int) (node *json.Value, err error) {
	val := jsonDataLiteral(data[pos])
	switch val {
	case jsonNullLiteral:
		node = json.ValueNull
	case jsonTrueLiteral:
		node = json.ValueTrue
	case jsonFalseLiteral:
		node = json.ValueFalse
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown literal value %v", val)
	}
	return node, nil
}

// other types are stored as catch-all opaque types: documentation on these is scarce.
// we currently know about (and support) date/time/datetime/decimal.
func binparserOpaque(_ jsonDataType, data []byte, pos int) (node *json.Value, err error) {
	dataType := data[pos]
	start := 3       // account for length of stored value
	end := start + 8 // all currently supported opaque data types are 8 bytes in size
	switch dataType {
	case TypeDate:
		raw := binary.LittleEndian.Uint64(data[start:end])
		value := raw >> 24
		yearMonth := (value >> 22) & 0x01ffff // 17 bits starting at 22nd
		year := yearMonth / 13
		month := yearMonth % 13
		day := (value >> 17) & 0x1f // 5 bits starting at 17th
		dateString := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
		node = json.NewDate(dateString)
	case TypeTime2, TypeTime:
		raw := binary.LittleEndian.Uint64(data[start:end])
		value := raw >> 24
		hour := (value >> 12) & 0x03ff // 10 bits starting at 12th
		minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
		second := value & 0x3f         // 6 bits starting at 0th
		microSeconds := raw & 0xffffff // 24 lower bits
		timeString := fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, microSeconds)
		node = json.NewTime(timeString)
	case TypeDateTime2, TypeDateTime, TypeTimestamp2, TypeTimestamp:
		raw := binary.LittleEndian.Uint64(data[start:end])
		value := raw >> 24
		yearMonth := (value >> 22) & 0x01ffff // 17 bits starting at 22nd
		year := yearMonth / 13
		month := yearMonth % 13
		day := (value >> 17) & 0x1f    // 5 bits starting at 17th
		hour := (value >> 12) & 0x1f   // 5 bits starting at 12th
		minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
		second := value & 0x3f         // 6 bits starting at 0th
		microSeconds := raw & 0xffffff // 24 lower bits
		timeString := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, microSeconds)
		node = json.NewDateTime(timeString)
	case TypeDecimal, TypeNewDecimal:
		decimalData := data[start:end]
		precision := decimalData[0]
		scale := decimalData[1]
		metadata := (uint16(precision) << 8) + uint16(scale)
		val, _, err := CellValue(decimalData, 2, TypeNewDecimal, metadata, &querypb.Field{Type: querypb.Type_DECIMAL})
		if err != nil {
			return nil, err
		}
		node = json.NewNumber(val.ToString(), json.NumberTypeDecimal)
	case TypeVarchar, TypeVarString, TypeString, TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob:
		node = json.NewBlob(string(data[pos+1:]))
	case TypeBit:
		node = json.NewBit(string(data[pos+1:]))
	default:
		node = json.NewOpaqueValue(string(data[pos+1:]))
	}
	return node, nil
}

func binparserString(_ jsonDataType, data []byte, pos int) (node *json.Value, err error) {
	size, pos := readVariableLength(data, pos)
	return json.NewString(string(data[pos : pos+size])), nil
}

// arrays are stored thus:
// | type_identifier(one of [2,3]) | elem count | obj size | list of offsets+lengths of values | actual values |
func binparserArray(typ jsonDataType, data []byte, pos int) (node *json.Value, err error) {
	var nodes []*json.Value
	var elem *json.Value
	var elementCount int
	large := typ == jsonLargeArray
	elementCount, pos = readInt(data, pos, large)
	_, pos = readInt(data, pos, large)
	for i := 0; i < elementCount; i++ {
		elem, pos, err = binparserElement(data, pos, large)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, elem)
	}
	node = json.NewArray(nodes)
	return node, nil
}

// objects are stored thus:
// | type_identifier(0/1) | elem count | obj size | list of offsets+lengths of keys | list of offsets+lengths of values | actual keys | actual values |
func binparserObject(typ jsonDataType, data []byte, pos int) (node *json.Value, err error) {
	// "large" decides number of bytes used to specify element count and total object size: 4 bytes for large, 2 for small
	var large = typ == jsonLargeObject
	var elementCount int // total number of elements (== keys) in this object map. (element can be another object: recursively handled)

	elementCount, pos = readInt(data, pos, large)
	_, pos = readInt(data, pos, large)

	keys := make([]string, elementCount) // stores all the keys in this object
	for i := 0; i < elementCount; i++ {
		var keyOffset int
		var keyLength int
		keyOffset, pos = readInt(data, pos, large)
		keyLength, pos = readInt(data, pos, false) // keyLength is always a 16-bit int

		keyOffsetStart := keyOffset + 1
		// check that offsets are not out of bounds (can happen only if there is a bug in the parsing code)
		if keyOffsetStart >= len(data) || keyOffsetStart+keyLength > len(data) {
			return nil, fmt.Errorf("unable to decode object elements: %v", data)
		}
		keys[i] = string(data[keyOffsetStart : keyOffsetStart+keyLength])
	}

	var object json.Object
	var elem *json.Value

	// get the value for each key
	for i := 0; i < elementCount; i++ {
		elem, pos, err = binparserElement(data, pos, large)
		if err != nil {
			return nil, err
		}
		object.Add(keys[i], elem)
	}

	return json.NewObject(object), nil
}
