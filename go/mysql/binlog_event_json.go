/*
Copyright 2020 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/log"

	"github.com/spyzhov/ajson"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var jsonDebug = false //TODO remove this once JSON functionality has been proven in the field

func jlog(s string, p ...interface{}) {
	if jsonDebug {
		log.Infof(s, p)
		fmt.Printf(s+"\n", p)
	}
}

// provides the single API function to convert json from binary format used in binlogs to a string representation
func getJSONValue(data []byte) (string, error) {
	jlog("In getJSONValue for %v", data)
	var ast *ajson.Node
	var err error
	if len(data) == 0 {
		ast = ajson.NullNode("")
	} else {
		ast, _, err = binaryJSON.parse(data)
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

var binaryJSON *BinaryJSON

func init() {
	binaryJSON = &BinaryJSON{
		handlers: make(map[jsonDataType]jsonHandler),
	}
	newIntHandler()
	newLiteralHandler()
	newOpaqueHandler()
	newStringHandler()
	newArrayHandler()
	newObjectHandler()
}

// BinaryJSON contains the handlers for all json types and methods for parsing the binary json representation from the binlog
type BinaryJSON struct {
	handlers map[jsonDataType]jsonHandler
}

func (jh *BinaryJSON) parse(data []byte) (node *ajson.Node, newPos int, err error) {
	var pos int
	typ := data[0]
	pos++
	return jh.getNode(jsonDataType(typ), data, pos)
}

func (jh *BinaryJSON) register(typ jsonDataType, handler jsonHandler) {
	jh.handlers[typ] = handler
}

func (jh *BinaryJSON) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	handler := jh.handlers[typ]
	if handler == nil {
		return nil, 0, fmt.Errorf("handler not found for type %d", typ)
	}
	return handler.getNode(typ, data, pos)
}

// jsonDataType has the values used in the mysql json binary representation to denote types
// we have string, literal(true/false/null), number, object or array types
// large object => doc size > 64K, you get pointers instead of inline values
type jsonDataType byte

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

const (
	jsonNullLiteral  = '\x00'
	jsonTrueLiteral  = '\x01'
	jsonFalseLiteral = '\x02'
)

// in objects and arrays some values are inlined, others have offsets into the raw data
var smallValueTypes = map[jsonDataType]bool{
	jsonSmallObject: false,
	jsonLargeObject: false,
	jsonSmallArray:  false,
	jsonLargeArray:  false,
	jsonLiteral:     true,
	jsonInt16:       true,
	jsonUint16:      true,
	jsonInt32:       false,
	jsonUint32:      false,
	jsonInt64:       false,
	jsonUint64:      false,
	jsonDouble:      false,
	jsonString:      false,
	jsonOpaque:      false,
}

// readOffsetOrSize returns either the offset or size for a scalar data type, depending on the type of the
// containing object. JSON documents stored are considered "large" if the size of the stored json document is
// more than 64K bytes. For a large document all types which have their smallValueTypes entry as true
// are inlined. Others only store the offset in the document
// (This design decision allows a fixed number of bytes to be used for representing objects keys and arrays entries)
func readOffsetOrSize(data []byte, pos int, large bool) (int, int) {
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
func readVariableLength(data []byte, pos int) (int, int) {
	var bb byte
	var res int
	var idx byte
	for {
		bb = data[pos]
		pos++
		res |= int(bb&0x7f) << (7 * idx)
		// if the high bit is 1, the integer value of the byte will be negative
		// high bit of 1 signifies that the next byte is part of the length encoding
		if int8(bb) >= 0 {
			break
		}
		idx++
	}
	return res, pos
}

type jsonHandler interface {
	getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error)
}

type handlerInfo struct {
	name  string
	types []jsonDataType
}

type intHandler struct {
	info  *handlerInfo
	sizes map[jsonDataType]int
}

var _ jsonHandler = (*intHandler)(nil)

func (ih intHandler) getVal(typ jsonDataType, data []byte, pos int) (value float64, newPos int) {
	var val uint64
	var val2 float64
	size := ih.sizes[typ]
	for i := 0; i < size; i++ {
		val = val + uint64(data[pos+i])<<(8*i)
	}
	pos += size
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
	return val2, pos
}

func (ih intHandler) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	val, pos := ih.getVal(typ, data, pos)
	node = ajson.NumericNode("", val)
	return node, pos, nil
}

func newIntHandler() *intHandler {
	ih := &intHandler{
		info: &handlerInfo{
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
		binaryJSON.register(typ, ih)
	}
	return ih
}

type literalHandler struct {
	info *handlerInfo
}

var _ jsonHandler = (*literalHandler)(nil)

func (lh literalHandler) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	val := jsonDataLiteral(data[pos])
	pos++
	switch val {
	case jsonNullLiteral:
		node = ajson.NullNode("")
	case jsonTrueLiteral:
		node = ajson.BoolNode("", true)
	case jsonFalseLiteral:
		node = ajson.BoolNode("", false)
	default:
		return nil, 0, fmt.Errorf("unknown literal value %v", val)
	}
	return node, pos, nil
}

func newLiteralHandler() *literalHandler {
	lh := &literalHandler{
		info: &handlerInfo{
			name:  "Literal",
			types: []jsonDataType{jsonLiteral},
		},
	}
	binaryJSON.register(jsonLiteral, lh)
	return lh
}

type opaqueHandler struct {
	info *handlerInfo
}

var _ jsonHandler = (*opaqueHandler)(nil)

func (oh opaqueHandler) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	dataType := data[pos]
	pos++
	_, pos = readVariableLength(data, pos)
	switch dataType {
	case TypeDate:
		raw := binary.LittleEndian.Uint64(data[3:11])
		value := raw >> 24
		yearMonth := (value >> 22) & 0x01ffff // 17 bits starting at 22nd
		year := yearMonth / 13
		month := yearMonth % 13
		day := (value >> 17) & 0x1f // 5 bits starting at 17th
		dateString := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
		node = ajson.StringNode("", dateString)
	case TypeTime:
		raw := binary.LittleEndian.Uint64(data[3:11])
		value := raw >> 24
		hour := (value >> 12) & 0x03ff // 10 bits starting at 12th
		minute := (value >> 6) & 0x3f  // 6 bits starting at 6th
		second := value & 0x3f         // 6 bits starting at 0th
		microSeconds := raw & 0xffffff // 24 lower bits
		timeString := fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, microSeconds)
		node = ajson.StringNode("", timeString)
	case TypeDateTime:
		raw := binary.LittleEndian.Uint64(data[3:11])
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
		decimalData := data[3:11]
		precision := decimalData[0]
		scale := decimalData[1]
		metadata := (uint16(precision) << 8) + uint16(scale)
		val, _, err := CellValue(decimalData, 2, TypeNewDecimal, metadata, querypb.Type_DECIMAL)
		if err != nil {
			return nil, 0, err
		}
		float, err := evalengine.ToFloat64(val)
		if err != nil {
			return nil, 0, err
		}
		node = ajson.NumericNode("", float)
	default:
		return nil, 0, fmt.Errorf("opaque type %d is not supported yet, data %v", dataType, data[2:])
	}
	pos += 8
	return node, pos, nil
}

func newOpaqueHandler() *opaqueHandler {
	oh := &opaqueHandler{
		info: &handlerInfo{
			name:  "Opaque",
			types: []jsonDataType{jsonOpaque},
		},
	}
	binaryJSON.register(jsonOpaque, oh)
	return oh
}

type stringHandler struct {
	info *handlerInfo
}

var _ jsonHandler = (*stringHandler)(nil)

func (sh stringHandler) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	size, pos := readVariableLength(data, pos)
	node = ajson.StringNode("", string(data[pos:pos+size]))

	return node, pos, nil
}

func newStringHandler() *stringHandler {
	sh := &stringHandler{
		info: &handlerInfo{
			name:  "String",
			types: []jsonDataType{jsonString},
		},
	}
	binaryJSON.register(jsonString, sh)
	return sh
}

type arrayHandler struct {
	info *handlerInfo
}

var _ jsonHandler = (*arrayHandler)(nil)

func (ah arrayHandler) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	var nodes []*ajson.Node
	var elem *ajson.Node
	var elementCount, offset int
	elementCount, pos = readOffsetOrSize(data, pos, false)
	_, pos = readOffsetOrSize(data, pos, false)
	_ = typ
	for i := 0; i < elementCount; i++ {
		typ = jsonDataType(data[pos])
		pos++
		if smallValueTypes[typ] {
			elem, pos, err = binaryJSON.getNode(typ, data, pos)
			if err != nil {
				return nil, 0, err
			}
		} else {
			offset, pos = readOffsetOrSize(data, pos, false)
			newData := data[offset:]
			elem, _, err = binaryJSON.getNode(typ, newData, 1) //newPos ignored because this is an offset into the "extra" section of the buffer
			if err != nil {
				return nil, 0, err
			}
		}
		nodes = append(nodes, elem)
	}
	node = ajson.ArrayNode("", nodes)
	return node, pos, nil
}

func newArrayHandler() *arrayHandler {
	ah := &arrayHandler{
		info: &handlerInfo{
			name:  "Array",
			types: []jsonDataType{jsonSmallArray},
		},
	}
	binaryJSON.register(jsonSmallArray, ah)
	return ah
}

type objectHandler struct {
	info *handlerInfo
}

var _ jsonHandler = (*objectHandler)(nil)

func (oh objectHandler) getNode(typ jsonDataType, data []byte, pos int) (node *ajson.Node, newPos int, err error) {
	nodes := make(map[string]*ajson.Node)
	var elem *ajson.Node
	var elementCount, offset int
	var large = false
	_ = typ
	elementCount, pos = readOffsetOrSize(data, pos, large)
	_, pos = readOffsetOrSize(data, pos, large)
	keys := make([]string, elementCount)
	for i := 0; i < elementCount; i++ {
		var keyOffset, keyLength int
		keyOffset, pos = readOffsetOrSize(data, pos, large)
		keyLength, pos = readOffsetOrSize(data, pos, false) // always 16
		keys[i] = string(data[keyOffset+1 : keyOffset+keyLength+1])
	}

	for i := 0; i < elementCount; i++ {
		typ = jsonDataType(data[pos])
		pos++
		if smallValueTypes[typ] {
			elem, pos, err = binaryJSON.getNode(typ, data, pos)
			if err != nil {
				return nil, 0, err
			}
		} else {
			offset, pos = readOffsetOrSize(data, pos, false)
			newData := data[offset:]
			elem, _, err = binaryJSON.getNode(typ, newData, 1) //newPos ignored because this is an offset into the "extra" section of the buffer
			if err != nil {
				return nil, 0, err
			}
		}
		nodes[keys[i]] = elem
	}

	node = ajson.ObjectNode("", nodes)
	return node, pos, nil
}

func newObjectHandler() *objectHandler {
	oh := &objectHandler{
		info: &handlerInfo{
			name:  "Object",
			types: []jsonDataType{jsonSmallObject},
		},
	}
	binaryJSON.register(jsonSmallObject, oh)
	return oh
}

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
