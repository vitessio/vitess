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

package json

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// MarshalSQLTo appends marshaled v to dst and returns the result in
// the form like `JSON_OBJECT` or `JSON_ARRAY` to ensure we don't
// lose any type information.
func (v *Value) MarshalSQLTo(dst []byte) []byte {
	return v.marshalSQLInternal(true, dst)
}

func (v *Value) marshalSQLInternal(top bool, dst []byte) []byte {
	switch v.t {
	case TypeObject:
		dst = append(dst, "JSON_OBJECT("...)
		for i, vv := range v.o.kvs {
			if i != 0 {
				dst = append(dst, ", "...)
			}
			dst = append(dst, "_utf8mb4'"...)
			dst = append(dst, vv.k...)
			dst = append(dst, "', "...)
			dst = vv.v.marshalSQLInternal(false, dst)
		}
		dst = append(dst, ')')
		return dst
	case TypeArray:
		dst = append(dst, "JSON_ARRAY("...)
		for i, vv := range v.a {
			if i != 0 {
				dst = append(dst, ", "...)
			}
			dst = vv.marshalSQLInternal(false, dst)
		}
		dst = append(dst, ')')
		return dst
	case TypeString:
		if top {
			dst = append(dst, "CAST(JSON_QUOTE("...)
		}
		dst = append(dst, "_utf8mb4"...)
		dst = append(dst, sqltypes.EncodeStringSQL(v.s)...)
		if top {
			dst = append(dst, ") as JSON)"...)
		}
		return dst
	case TypeDate:
		t, _ := v.Date()

		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "date '"...)
		dst = append(dst, t.Format("2006-01-02")...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeDateTime:
		t, _ := v.DateTime()

		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "timestamp '"...)
		dst = append(dst, t.Format("2006-01-02 15:04:05.000000")...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeTime:
		now := time.Now()
		year, month, day := now.Date()

		t, _ := v.Time()
		diff := t.Sub(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
		var neg bool
		if diff < 0 {
			diff = -diff
			neg = true
		}

		b := strings.Builder{}
		if neg {
			b.WriteByte('-')
		}

		hours := (diff / time.Hour)
		diff -= hours * time.Hour
		// For some reason MySQL wraps this around and loses data
		// if it's more than 32 hours.
		fmt.Fprintf(&b, "%02d", hours%32)
		minutes := (diff / time.Minute)
		fmt.Fprintf(&b, ":%02d", minutes)
		diff -= minutes * time.Minute
		seconds := (diff / time.Second)
		fmt.Fprintf(&b, ":%02d", seconds)
		diff -= seconds * time.Second
		fmt.Fprintf(&b, ".%06d", diff/time.Microsecond)

		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "time '"...)
		dst = append(dst, b.String()...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBlob:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "x'"...)
		dst = append(dst, hex.EncodeToString([]byte(v.s))...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBit:
		if top {
			dst = append(dst, "CAST("...)
		}
		var i big.Int
		i.SetBytes([]byte(v.s))
		dst = append(dst, "b'"...)
		dst = append(dst, i.Text(2)...)
		dst = append(dst, "'"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeNumber:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, v.s...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeBoolean:
		if top {
			dst = append(dst, "CAST("...)
		}
		if v == ValueTrue {
			dst = append(dst, "true"...)
		} else {
			dst = append(dst, "false"...)
		}
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	case TypeNull:
		if top {
			dst = append(dst, "CAST("...)
		}
		dst = append(dst, "null"...)
		if top {
			dst = append(dst, " as JSON)"...)
		}
		return dst
	default:
		panic(fmt.Errorf("BUG: unexpected Value type: %d", v.t))
	}
}

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

// ParseMySQL provides the parsing function from the mysql binary json
// representation to a JSON value instance.
func ParseMySQL(data []byte) (*Value, error) {
	var err error
	var node *Value
	if len(data) == 0 {
		node = ValueNull
	} else {
		node, err = bp.parse(data)
		if err != nil {
			return nil, err
		}
	}
	return node, nil
}

var bp *binParser

func init() {
	bp = &binParser{
		plugins: make(map[jsonDataType]jsonPlugin),
	}
}

//region plugin manager

// binParser contains the plugins for all json types and methods for parsing the
// binary json representation of a specific type from the binlog
type binParser struct {
	plugins map[jsonDataType]jsonPlugin
}

// parse decodes a value from the binlog
func (jh *binParser) parse(data []byte) (node *Value, err error) {
	// pos keeps track of the offset of the current node being parsed
	pos := 0
	typ := data[pos]
	pos++
	return jh.getNode(jsonDataType(typ), data, pos)
}

// each plugin registers itself in init()s
func (jh *binParser) register(typ jsonDataType, Plugin jsonPlugin) {
	jh.plugins[typ] = Plugin
}

// gets the node at this position
func (jh *binParser) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
	Plugin := jh.plugins[typ]
	if Plugin == nil {
		return nil, fmt.Errorf("Plugin not found for type %d", typ)
	}
	return Plugin.getNode(typ, data, pos)
}

//endregion

//region enums

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

//endregion

//region util funcs

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
// of an arbitrarily long string as implemented by the mysql server.
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

// getElem returns the json value found inside json objects and arrays at the provided position
func getElem(data []byte, pos int, large bool) (*Value, int, error) {
	var elem *Value
	var err error
	var offset int
	typ := jsonDataType(data[pos])
	pos++
	if isInline(typ, large) {
		elem, err = bp.getNode(typ, data, pos)
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
		elem, err = bp.getNode(typ, newData, 1)
		if err != nil {
			return nil, 0, err
		}
	}
	return elem, pos, nil
}

//endregion

// json sub-type interface
// one plugin for each sub-type, plugins are stateless and initialized on load via individual init() functions
type jsonPlugin interface {
	getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error)
}

type jsonPluginInfo struct {
	name  string
	types []jsonDataType
}

//region int plugin

func init() {
	newIntPlugin()
}

type intPlugin struct {
	info  *jsonPluginInfo
	sizes map[jsonDataType]int
}

var _ jsonPlugin = (*intPlugin)(nil)

func (ih intPlugin) getVal(typ jsonDataType, data []byte, pos int) *Value {
	var val uint64
	size := ih.sizes[typ]
	for i := 0; i < size; i++ {
		val = val + uint64(data[pos+i])<<(8*i)
	}
	var s string
	var dbl bool
	switch typ {
	case jsonInt16:
		s = strconv.FormatInt(int64(int16(val)), 10)
	case jsonUint16:
		s = strconv.FormatUint(uint64(uint16(val)), 10)
	case jsonInt32:
		s = strconv.FormatInt(int64(int32(val)), 10)
	case jsonUint32:
		s = strconv.FormatUint(uint64(uint32(val)), 10)
	case jsonInt64:
		s = strconv.FormatInt(int64(val), 10)
	case jsonUint64:
		s = strconv.FormatUint(val, 10)
	case jsonDouble:
		dbl = true
		s = string(mysql.FormatFloat(sqltypes.Float64, math.Float64frombits(val)))
	}
	return &Value{
		t: TypeNumber,
		s: s,
		i: !dbl,
	}
}

func (ih intPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
	val := ih.getVal(typ, data, pos)
	return val, nil
}

func newIntPlugin() *intPlugin {
	ih := &intPlugin{
		info: &jsonPluginInfo{
			name:  "Int",
			types: []jsonDataType{jsonInt64, jsonInt32, jsonInt16, jsonUint16, jsonUint32, jsonUint64, jsonDouble},
		},
		sizes: make(map[jsonDataType]int),
	}
	ih.sizes = map[jsonDataType]int{
		jsonUint64: 8,
		jsonInt64:  8,
		jsonUint32: 4,
		jsonInt32:  4,
		jsonUint16: 2,
		jsonInt16:  2,
		jsonDouble: 8,
	}
	for _, typ := range ih.info.types {
		bp.register(typ, ih)
	}
	return ih
}

//endregion

//region literal plugin

func init() {
	newLiteralPlugin()
}

type literalPlugin struct {
	info *jsonPluginInfo
}

var _ jsonPlugin = (*literalPlugin)(nil)

func (lh literalPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
	val := jsonDataLiteral(data[pos])
	switch val {
	case jsonNullLiteral:
		node = ValueNull
	case jsonTrueLiteral:
		node = ValueTrue
	case jsonFalseLiteral:
		node = ValueFalse
	default:
		return nil, fmt.Errorf("unknown literal value %v", val)
	}
	return node, nil
}

func newLiteralPlugin() *literalPlugin {
	lh := &literalPlugin{
		info: &jsonPluginInfo{
			name:  "Literal",
			types: []jsonDataType{jsonLiteral},
		},
	}
	bp.register(jsonLiteral, lh)
	return lh
}

//endregion

//region opaque plugin

func init() {
	newOpaquePlugin()
}

type opaquePlugin struct {
	info *jsonPluginInfo
}

var _ jsonPlugin = (*opaquePlugin)(nil)

// other types are stored as catch-all opaque types: documentation on these is scarce.
// we currently know about (and support) date/time/datetime/decimal.
func (oh opaquePlugin) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
	dataType := data[pos]
	start := 3       // account for length of stored value
	end := start + 8 // all currently supported opaque data types are 8 bytes in size
	switch dataType {
	case mysql.TypeDate:
		raw := binary.LittleEndian.Uint64(data[start:end])
		value := raw >> 24
		yearMonth := (value >> 22) & 0x01ffff // 17 bits starting at 22nd
		year := yearMonth / 13
		month := yearMonth % 13
		day := (value >> 17) & 0x1f // 5 bits starting at 17th
		dateString := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
		node = &Value{t: TypeDate, s: dateString}
	case mysql.TypeTime2, mysql.TypeTime:
		raw := binary.LittleEndian.Uint64(data[start:end])
		value := raw >> 24
		hour := (value >> 12) & 0x03ff // 10 bits starting at 12th
		minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
		second := value & 0x3f         // 6 bits starting at 0th
		microSeconds := raw & 0xffffff // 24 lower bits
		timeString := fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, microSeconds)
		node = &Value{t: TypeTime, s: timeString}
	case mysql.TypeDateTime2, mysql.TypeDateTime, mysql.TypeTimestamp2, mysql.TypeTimestamp:
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
		node = &Value{t: TypeDateTime, s: timeString}
	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		decimalData := data[start:end]
		precision := decimalData[0]
		scale := decimalData[1]
		metadata := (uint16(precision) << 8) + uint16(scale)
		val, _, err := mysql.CellValue(decimalData, 2, mysql.TypeNewDecimal, metadata, &querypb.Field{Type: querypb.Type_DECIMAL})
		if err != nil {
			return nil, err
		}
		node = &Value{t: TypeNumber, s: val.ToString(), i: true}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		node = &Value{t: TypeBlob, s: string(data[pos+1:])}
	case mysql.TypeBit:
		node = &Value{t: TypeBit, s: string(data[pos+1:])}
	default:
		node = &Value{t: TypeOpaque, s: string(data[pos+1:])}
	}
	return node, nil
}

func newOpaquePlugin() *opaquePlugin {
	oh := &opaquePlugin{
		info: &jsonPluginInfo{
			name:  "Opaque",
			types: []jsonDataType{jsonOpaque},
		},
	}
	bp.register(jsonOpaque, oh)
	return oh
}

//endregion

//region string plugin

func init() {
	newStringPlugin()
}

type stringPlugin struct {
	info *jsonPluginInfo
}

var _ jsonPlugin = (*stringPlugin)(nil)

func (sh stringPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
	size, pos := readVariableLength(data, pos)
	node = &Value{t: TypeString, s: string(data[pos : pos+size])}
	return node, nil
}

func newStringPlugin() *stringPlugin {
	sh := &stringPlugin{
		info: &jsonPluginInfo{
			name:  "String",
			types: []jsonDataType{jsonString},
		},
	}
	bp.register(jsonString, sh)
	return sh
}

//endregion

//region array plugin

func init() {
	newArrayPlugin()
}

type arrayPlugin struct {
	info *jsonPluginInfo
}

var _ jsonPlugin = (*arrayPlugin)(nil)

// arrays are stored thus:
// | type_identifier(one of [2,3]) | elem count | obj size | list of offsets+lengths of values | actual values |
func (ah arrayPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
	var nodes []*Value
	var elem *Value
	var elementCount int
	large := typ == jsonLargeArray
	elementCount, pos = readInt(data, pos, large)
	_, pos = readInt(data, pos, large)
	for i := 0; i < elementCount; i++ {
		elem, pos, err = getElem(data, pos, large)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, elem)
	}
	node = &Value{t: TypeArray, a: nodes}
	return node, nil
}

func newArrayPlugin() *arrayPlugin {
	ah := &arrayPlugin{
		info: &jsonPluginInfo{
			name:  "Array",
			types: []jsonDataType{jsonSmallArray, jsonLargeArray},
		},
	}
	bp.register(jsonSmallArray, ah)
	bp.register(jsonLargeArray, ah)
	return ah
}

//endregion

//region object plugin

func init() {
	newObjectPlugin()
}

type objectPlugin struct {
	info *jsonPluginInfo
}

var _ jsonPlugin = (*objectPlugin)(nil)

// objects are stored thus:
// | type_identifier(0/1) | elem count | obj size | list of offsets+lengths of keys | list of offsets+lengths of values | actual keys | actual values |
func (oh objectPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *Value, err error) {
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

	var object []kv
	var elem *Value

	// get the value for each key
	for i := 0; i < elementCount; i++ {
		elem, pos, err = getElem(data, pos, large)
		if err != nil {
			return nil, err
		}
		object = append(object, kv{k: keys[i], v: elem})
	}

	node = &Value{t: TypeObject, o: Object{kvs: object}}
	return node, nil
}

func newObjectPlugin() *objectPlugin {
	oh := &objectPlugin{
		info: &jsonPluginInfo{
			name:  "Object",
			types: []jsonDataType{jsonSmallObject, jsonLargeObject},
		},
	}
	bp.register(jsonSmallObject, oh)
	bp.register(jsonLargeObject, oh)
	return oh
}

//endregion
