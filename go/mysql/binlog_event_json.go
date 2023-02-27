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
	"fmt"
	"math"

	"vitess.io/vitess/go/vt/log"

	"github.com/spyzhov/ajson"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

/*

References:

* C source of mysql json data type implementation
https://fossies.org/linux/mysql/sql/json_binary.cc

* nice description of MySQL's json representation
https://lafengnan.gitbooks.io/blog/content/mysql/chapter2.html

* java/python connector links: useful for test cases and reverse engineering
https://github.com/shyiko/mysql-binlog-connector-java/pull/119/files
https://github.com/noplay/python-mysql-replication/blob/175df28cc8b536a68522ff9b09dc5440adad6094/pymysqlreplication/packet.py

*/

// region debug-only
// TODO remove once the json refactor is tested live
var jsonDebug = false

func jlog(tpl string, vals ...any) {
	if !jsonDebug {
		return
	}
	log.Infof("JSON:"+tpl+"\n", vals...)
	_ = printASCIIBytes
}

func printASCIIBytes(data []byte) {
	if !jsonDebug {
		return
	}
	s := ""
	for _, c := range data {
		if c < 127 && c > 32 {
			s += fmt.Sprintf("%c ", c)
		} else {
			s += fmt.Sprintf("%02d ", c)
		}
	}
	log.Infof("[%s]", s)
}

// only used for logging/debugging
var jsonTypeToName = map[uint]string{
	jsonSmallObject: "sObject",
	jsonLargeObject: "lObject",
	jsonSmallArray:  "sArray",
	jsonLargeArray:  "lArray",
	jsonLiteral:     "literal",
	jsonInt16:       "int16",
	jsonUint16:      "uint16",
	jsonInt32:       "int32",
	jsonUint32:      "uint32",
	jsonInt64:       "int64",
	jsonUint64:      "uint64",
	jsonDouble:      "double", //0x0b
	jsonString:      "string", //0x0c a utf8mb4 string
	jsonOpaque:      "opaque", //0x0f "custom" data
}

func jsonDataTypeToString(typ uint) string {
	sType, ok := jsonTypeToName[typ]
	if !ok {
		return "undefined"
	}
	return sType
}

//endregion

// provides the single API function, used to convert json from binary format used in binlogs to a string representation
func getJSONValue(data []byte) (string, error) {
	var ast *ajson.Node
	var err error
	if len(data) == 0 {
		ast = ajson.NullNode("")
	} else {
		ast, err = binlogJSON.parse(data)
		if err != nil {
			return "", err
		}
	}
	bytes, err := ajson.Marshal(ast)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

var binlogJSON *BinlogJSON

func init() {
	binlogJSON = &BinlogJSON{
		plugins: make(map[jsonDataType]jsonPlugin),
	}
}

//region plugin manager

// BinlogJSON contains the plugins for all json types and methods for parsing the
// binary json representation of a specific type from the binlog
type BinlogJSON struct {
	plugins map[jsonDataType]jsonPlugin
}

// parse decodes a value from the binlog
func (jh *BinlogJSON) parse(data []byte) (node *ajson.Node, err error) {
	// pos keeps track of the offset of the current node being parsed
	pos := 0
	typ := data[pos]
	jlog("Top level object is type %s\n", jsonDataTypeToString(uint(typ)))
	pos++
	return jh.getNode(jsonDataType(typ), data, pos)
}

// each plugin registers itself in init()s
func (jh *BinlogJSON) register(typ jsonDataType, Plugin jsonPlugin) {
	jh.plugins[typ] = Plugin
}

// gets the node at this position
func (jh *BinlogJSON) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
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
// of an arbitrarily long string as implemented by the mysql server
// https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.cc#L234
// https://github.com/mysql/mysql-server/blob/8.0/sql/json_binary.cc#L283
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
func getElem(data []byte, pos int, large bool) (*ajson.Node, int, error) {
	var elem *ajson.Node
	var err error
	var offset int
	typ := jsonDataType(data[pos])
	pos++
	if isInline(typ, large) {
		elem, err = binlogJSON.getNode(typ, data, pos)
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
			log.Errorf("unable to decode element")
			return nil, 0, fmt.Errorf("unable to decode element: %+v", data)
		}
		newData := data[offset:]
		//newPos ignored because this is an offset into the "extra" section of the buffer
		elem, err = binlogJSON.getNode(typ, newData, 1)
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
	getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error)
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

func (ih intPlugin) getVal(typ jsonDataType, data []byte, pos int) (value float64) {
	var val uint64
	var val2 float64
	size := ih.sizes[typ]
	for i := 0; i < size; i++ {
		val = val + uint64(data[pos+i])<<(8*i)
	}
	switch typ {
	case jsonInt16:
		val2 = float64(int16(val))
	case jsonUint16:
		val2 = float64(uint16(val))
	case jsonInt32:
		val2 = float64(int32(val))
	case jsonUint32:
		val2 = float64(uint32(val))
	case jsonInt64:
		val2 = float64(int64(val))
	case jsonUint64:
		val2 = float64(val)
	case jsonDouble:
		val2 = math.Float64frombits(val)
	}
	return val2
}

func (ih intPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
	val := ih.getVal(typ, data, pos)
	node = ajson.NumericNode("", val)
	return node, nil
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
		binlogJSON.register(typ, ih)
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

func (lh literalPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
	val := jsonDataLiteral(data[pos])
	switch val {
	case jsonNullLiteral:
		node = ajson.NullNode("")
	case jsonTrueLiteral:
		node = ajson.BoolNode("", true)
	case jsonFalseLiteral:
		node = ajson.BoolNode("", false)
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
	binlogJSON.register(jsonLiteral, lh)
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
func (oh opaquePlugin) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
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
		node = ajson.StringNode("", dateString)
	case TypeTime:
		raw := binary.LittleEndian.Uint64(data[start:end])
		value := raw >> 24
		hour := (value >> 12) & 0x03ff // 10 bits starting at 12th
		minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
		second := value & 0x3f         // 6 bits starting at 0th
		microSeconds := raw & 0xffffff // 24 lower bits
		timeString := fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, microSeconds)
		node = ajson.StringNode("", timeString)
	case TypeDateTime:
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
		node = ajson.StringNode("", timeString)
	case TypeNewDecimal:
		decimalData := data[start:end]
		precision := decimalData[0]
		scale := decimalData[1]
		metadata := (uint16(precision) << 8) + uint16(scale)
		val, _, err := CellValue(decimalData, 2, TypeNewDecimal, metadata, &querypb.Field{Type: querypb.Type_DECIMAL})
		if err != nil {
			return nil, err
		}
		float, err := val.ToFloat64()
		if err != nil {
			return nil, err
		}
		node = ajson.NumericNode("", float)
	default:
		return nil, fmt.Errorf("opaque type %d is not supported yet, data %v", dataType, data[2:])
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
	binlogJSON.register(jsonOpaque, oh)
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

func (sh stringPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
	size, pos := readVariableLength(data, pos)
	node = ajson.StringNode("", string(data[pos:pos+size]))

	return node, nil
}

func newStringPlugin() *stringPlugin {
	sh := &stringPlugin{
		info: &jsonPluginInfo{
			name:  "String",
			types: []jsonDataType{jsonString},
		},
	}
	binlogJSON.register(jsonString, sh)
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
func (ah arrayPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
	jlog("JSON Array %s, len %d", jsonDataTypeToString(uint(typ)), len(data))
	var nodes []*ajson.Node
	var elem *ajson.Node
	var elementCount, size int
	large := typ == jsonLargeArray
	elementCount, pos = readInt(data, pos, large)
	jlog("Array(%t): elem count: %d\n", large, elementCount)
	size, pos = readInt(data, pos, large)
	jlog("Array(%t): elem count: %d, size:%d\n", large, elementCount, size)
	for i := 0; i < elementCount; i++ {
		elem, pos, err = getElem(data, pos, large)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, elem)
		jlog("Index is %d:%s", i, jsonDataTypeToString(uint(typ)))
	}
	node = ajson.ArrayNode("", nodes)
	return node, nil
}

func newArrayPlugin() *arrayPlugin {
	ah := &arrayPlugin{
		info: &jsonPluginInfo{
			name:  "Array",
			types: []jsonDataType{jsonSmallArray, jsonLargeArray},
		},
	}
	binlogJSON.register(jsonSmallArray, ah)
	binlogJSON.register(jsonLargeArray, ah)
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
func (oh objectPlugin) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, err error) {
	jlog("JSON Type is %s, len %d", jsonDataTypeToString(uint(typ)), len(data))

	// "large" decides number of bytes used to specify element count and total object size: 4 bytes for large, 2 for small
	var large = typ == jsonLargeObject

	var elementCount int // total number of elements (== keys) in this object map. (element can be another object: recursively handled)
	var size int         // total size of object

	elementCount, pos = readInt(data, pos, large)
	size, pos = readInt(data, pos, large)
	jlog("Object: elem count: %d, size %d\n", elementCount, size)

	keys := make([]string, elementCount) // stores all the keys in this object
	for i := 0; i < elementCount; i++ {
		var keyOffset int
		var keyLength int
		keyOffset, pos = readInt(data, pos, large)
		keyLength, pos = readInt(data, pos, false) // keyLength is always a 16-bit int

		keyOffsetStart := keyOffset + 1
		// check that offsets are not out of bounds (can happen only if there is a bug in the parsing code)
		if keyOffsetStart >= len(data) || keyOffsetStart+keyLength > len(data) {
			log.Errorf("unable to decode object elements")
			return nil, fmt.Errorf("unable to decode object elements: %v", data)
		}
		keys[i] = string(data[keyOffsetStart : keyOffsetStart+keyLength])
	}
	jlog("Object keys: %+v", keys)

	object := make(map[string]*ajson.Node)
	var elem *ajson.Node

	// get the value for each key
	for i := 0; i < elementCount; i++ {
		elem, pos, err = getElem(data, pos, large)
		if err != nil {
			return nil, err
		}
		object[keys[i]] = elem
		jlog("Key is %s:%s", keys[i], jsonDataTypeToString(uint(typ)))
	}

	node = ajson.ObjectNode("", object)
	return node, nil
}

func newObjectPlugin() *objectPlugin {
	oh := &objectPlugin{
		info: &jsonPluginInfo{
			name:  "Object",
			types: []jsonDataType{jsonSmallObject, jsonLargeObject},
		},
	}
	binlogJSON.register(jsonSmallObject, oh)
	binlogJSON.register(jsonLargeObject, oh)
	return oh
}

//endregion
